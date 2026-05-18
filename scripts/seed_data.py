"""Generate TPC-H dataset and export to MinIO as Parquet files.

Usage:
    python -m scripts.seed_data --scale-factor 0.1
"""

import argparse
import logging
import os

import duckdb

logger = logging.getLogger(__name__)

TPCH_TABLES: list[str] = [
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
]


def _load_config() -> dict[str, str]:
    """Load MinIO connection configuration from environment variables.

    Returns:
        Dictionary of MinIO configuration values.
    """
    return {
        "minio_endpoint": os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        "minio_region": os.environ.get("MINIO_REGION", "us-east-1"),
        "minio_access_key": os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        "minio_secret_key": os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        "minio_use_ssl": os.environ.get("MINIO_USE_SSL", "false"),
        "minio_url_style": os.environ.get("MINIO_URL_STYLE", "path"),
    }


def _create_s3_secret(conn: duckdb.DuckDBPyConnection, config: dict[str, str]) -> None:
    """Create an S3-compatible secret for accessing MinIO via ``httpfs``.

    Args:
        conn: Active DuckDB connection.
        config: MinIO configuration dictionary.
    """
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
    logger.info("S3 secret created for endpoint %s", config["minio_endpoint"])


def _generate_tpch(conn: duckdb.DuckDBPyConnection, scale_factor: float) -> None:
    """Generate TPC-H tables at the requested scale factor.

    Args:
        conn: Active DuckDB connection.
        scale_factor: TPC-H scale factor (0.1 = ~100 MB raw data).
    """
    conn.execute("INSTALL tpch;")
    conn.execute("LOAD tpch;")
    conn.execute(f"CALL dbgen(sf = {scale_factor});")
    logger.info("TPC-H data generated at scale factor %.2f", scale_factor)


def _export_to_minio(conn: duckdb.DuckDBPyConnection, bucket: str) -> None:
    """Export each TPC-H table to MinIO as a zstd-compressed Parquet file.

    Args:
        conn: Active DuckDB connection.
        bucket: MinIO bucket name to write into.
    """
    for table in TPCH_TABLES:
        path = f"s3://{bucket}/{table}.parquet"
        conn.execute(f"""
            COPY (SELECT * FROM {table})
            TO '{path}'
            (FORMAT 'parquet', COMPRESSION 'zstd');
        """)
        row_count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        logger.info("Exported %s: %s rows -> %s", table, row_count, path)


def main() -> None:
    """Entrypoint: generate TPC-H data and write each table as Parquet to MinIO."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Seed MinIO with TPC-H Parquet data",
    )
    parser.add_argument(
        "--scale-factor",
        type=float,
        default=0.1,
        help="TPC-H scale factor (default: 0.1, ~100 MB)",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default="tpch",
        help="MinIO bucket name (default: tpch)",
    )
    args = parser.parse_args()

    config = _load_config()
    conn = duckdb.connect()
    try:
        _create_s3_secret(conn, config)
        _generate_tpch(conn, args.scale_factor)
        _export_to_minio(conn, args.bucket)
        logger.info("TPC-H seeding complete")
    except Exception:
        logger.exception("Failed to seed TPC-H data")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            logger.warning("Failed to close DuckDB connection")


if __name__ == "__main__":
    main()
