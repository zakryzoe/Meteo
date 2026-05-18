"""DuckDB Quack server for the Meteo analytical lakehouse.

Starts a DuckDB instance configured with:
- Memory and thread limits
- S3-compatible secret for MinIO storage access
- Quack remote protocol server listening on port 9494

The server runs in a background thread; the main thread blocks until
SIGTERM or SIGINT, then gracefully stops the Quack server and closes
the connection.
"""

import logging
import os
import signal
import threading

import duckdb

logger = logging.getLogger(__name__)

_uri: str = ""


def _load_config() -> dict[str, str]:
    """Load server configuration from environment variables.

    Returns:
        Dictionary of configuration values.
    """
    return {
        "minio_endpoint": os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        "minio_region": os.environ.get("MINIO_REGION", "us-east-1"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_use_ssl": os.environ.get("MINIO_USE_SSL", "false"),
        "minio_url_style": os.environ.get("MINIO_URL_STYLE", "path"),
        "quack_host": os.environ.get("QUACK_HOST", "0.0.0.0"),
        "quack_port": os.environ.get("QUACK_PORT", "9494"),
        "quack_token": os.environ.get("QUACK_TOKEN", ""),
        "memory_limit": os.environ.get("DUCKDB_MEMORY_LIMIT", "6GB"),
        "threads": os.environ.get("DUCKDB_THREADS", "4"),
        "bootstrap_data": os.environ.get("BOOTSTRAP_DATA", "true"),
        "tpch_scale_factor": os.environ.get("TPCH_SCALE_FACTOR", "0.1"),
    }


def _configure_duckdb(conn: duckdb.DuckDBPyConnection, config: dict[str, str]) -> None:
    """Apply DuckDB soft limits and create the S3/MinIO secret.

    Args:
        conn: Active DuckDB connection.
        config: Configuration dictionary from environment.
    """
    conn.execute(f"SET memory_limit = '{config['memory_limit']}';")
    conn.execute(f"SET threads = {config['threads']};")
    conn.execute("SET enable_progress_bar = false;")

    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")

    conn.execute(f"""
        CREATE OR REPLACE SECRET minio_secret (
            TYPE s3,
            KEY_ID '{config['minio_access_key']}',
            SECRET '{config['minio_secret_key']}',
            ENDPOINT '{config['minio_endpoint']}',
            REGION '{config['minio_region']}',
            USE_SSL {config['minio_use_ssl']},
            URL_STYLE '{config['minio_url_style']}'
        );
    """)
    logger.info(
        "DuckDB configured: memory_limit=%s, threads=%s, endpoint=%s",
        config["memory_limit"],
        config["threads"],
        config["minio_endpoint"],
    )


def _start_quack_server(conn: duckdb.DuckDBPyConnection, config: dict[str, str]) -> None:
    """Install Quack and start the remote protocol server (non-blocking).

    The server HTTP listener runs in the background.  The return
    value (listen URI, HTTP URL, auth token) is logged for reference.

    Args:
        conn: Active DuckDB connection.
        config: Configuration dictionary from environment.
    """
    global _uri
    _uri = f"quack:{config['quack_host']}:{config['quack_port']}"

    conn.execute("FORCE INSTALL quack FROM core_nightly;")
    conn.execute("LOAD quack;")

    token = config["quack_token"]
    if token:
        result = conn.execute(
            f"CALL quack_serve('{_uri}', allow_other_hostname => true, token => '{token}');"
        )
    else:
        result = conn.execute(
            f"CALL quack_serve('{_uri}', allow_other_hostname => true);"
        )

    listen_uri, http_url, auth_token = result.fetchone()
    logger.info(
        "Quack server started: listen=%s http=%s token=%s",
        listen_uri, http_url, auth_token,
    )


_TPCH_TABLES: list[str] = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def _bootstrap_tpch(conn: duckdb.DuckDBPyConnection, config: dict[str, str]) -> None:
    """Conditionally seed TPC-H data into MinIO and create DuckDB views.

    If ``BOOTSTRAP_DATA`` is truthy, the function:
    1. Checks whether a scale marker file exists in MinIO matching the
       configured ``TPCH_SCALE_FACTOR``.  If missing or mismatched,
       TPC-H data is regenerated at the requested scale.
    2. Generates TPC-H at the configured scale factor and exports each
       table as a zstd Parquet file.
    3. Creates ``CREATE OR REPLACE VIEW`` for each TPC-H table within
       the server's DuckDB session so they are visible to Quack clients.

    Args:
        conn: Active DuckDB connection.
        config: Configuration dictionary from environment.
    """
    raw = config.get("bootstrap_data", "true")
    if raw.lower() not in ("true", "yes", "1"):
        logger.info("Bootstrap disabled by user (BOOTSTRAP_DATA=%s)", raw)
        return

    scale_raw = config.get("tpch_scale_factor", "0.1")
    scale = float(scale_raw)
    logger.info("Bootstrap enabled — checking TPC-H data (scale=%s)", scale_raw)

    needs_generation = True
    try:
        result = conn.execute(
            "SELECT * FROM read_csv('s3://tpch/_scale.csv', header=false)"
        ).fetchone()
        stored_scale = str(result[0]).strip()
        if stored_scale == scale_raw:
            logger.info("TPC-H data present at scale %s, skipping", scale_raw)
            needs_generation = False
        else:
            logger.info(
                "Scale changed %s -> %s, regenerating", stored_scale, scale_raw
            )
    except duckdb.Error:
        logger.info("No scale marker found, generating TPC-H (scale=%s)", scale_raw)

    if needs_generation:
        conn.execute("INSTALL tpch;")
        conn.execute("LOAD tpch;")
        conn.execute(f"CALL dbgen(sf = {scale});")
        for table in _TPCH_TABLES:
            path = f"s3://tpch/{table}.parquet"
            conn.execute(
                f"COPY (SELECT * FROM {table}) TO '{path}' "
                f"(FORMAT 'parquet', COMPRESSION 'zstd', OVERWRITE_OR_IGNORE);"
            )
            logger.info("Generated and exported %s -> %s", table, path)
        conn.execute(
            f"COPY (SELECT '{scale_raw}') TO 's3://tpch/_scale.csv' "
            f"(HEADER false, OVERWRITE_OR_IGNORE);"
        )
        logger.info("Scale marker written: %s", scale_raw)

    for table in _TPCH_TABLES:
        conn.execute(f"DROP TABLE IF EXISTS {table};")
        conn.execute(
            f"CREATE OR REPLACE VIEW {table} AS "
            f"SELECT * FROM read_parquet('s3://tpch/{table}.parquet');"
        )
    logger.info("TPC-H views created: %s", ", ".join(_TPCH_TABLES))


def _handle_signal(signum: int, frame: object | None) -> None:
    """Log the received signal and let the shutdown flag be set by main()."""
    logger.info("Received signal %d, shutting down gracefully", signum)


def main() -> None:
    """Entrypoint: initialise DuckDB, configure MinIO access, start Quack server.

    Blocks indefinitely after starting the Quack server until a shutdown signal
    (SIGTERM / SIGINT) is received.  The server is stopped cleanly before exit.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    config = _load_config()
    conn = duckdb.connect()

    stop_event = threading.Event()

    def _on_signal(signum: int, frame: object | None) -> None:
        _handle_signal(signum, frame)
        stop_event.set()

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    try:
        _configure_duckdb(conn, config)
        _bootstrap_tpch(conn, config)
        _start_quack_server(conn, config)
        logger.info("Server ready — waiting for connections (Ctrl+C to stop)")
        stop_event.wait()
    except Exception:
        logger.exception("Fatal error in DuckDB Quack server")
        raise
    finally:
        logger.info("Shutting down Quack server")
        if _uri:
            try:
                conn.execute(f"CALL quack_stop('{_uri}');")
                logger.info("Quack server stopped")
            except duckdb.Error:
                logger.warning("quack_stop failed, server may already be down")
        try:
            conn.close()
        except Exception:
            logger.warning("Failed to close DuckDB connection")


if __name__ == "__main__":
    main()
