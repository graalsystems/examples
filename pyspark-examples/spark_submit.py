"""Entry point of the PySpark example.

The `tools` package is shipped next to this script as `tools.zip`. It is registered with
`addPyFile` so that the driver and the executors can import it.
"""

import argparse
import logging
import os
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pyspark-examples")


def main() -> None:
    parser = argparse.ArgumentParser(description="PySpark example")
    parser.add_argument(
        "--table",
        default=os.getenv("SOURCE_TABLE"),
        help="catalog table to read, for example db.table (default: $SOURCE_TABLE)",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("pyspark-examples").getOrCreate()

    archive = Path(__file__).resolve().parent / "tools.zip"
    if archive.is_file():
        spark.sparkContext.addPyFile(str(archive))

    from tools.processing import read_table, run  # noqa: E402 (needs the archive on the path)

    log.info("Running the Spark job")
    run(spark)
    if args.table:
        read_table(spark, args.table)
    spark.stop()


if __name__ == "__main__":
    main()
