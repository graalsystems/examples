import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

DATA = [
    ("James", "", "Smith", "1991-04-01", "M", 3000),
    ("Michael", "Rose", "", "2000-05-19", "M", 4000),
    ("Robert", "", "Williams", "1978-09-05", "M", 4000),
    ("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
    ("Jen", "Mary", "Brown", "1980-02-17", "F", -1),
]
COLUMNS = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]


def run(spark: SparkSession) -> DataFrame:
    """Build a small DataFrame in memory and aggregate it."""
    df = spark.createDataFrame(DATA, schema=COLUMNS).withColumn("dob", F.to_date("dob"))
    df.printSchema()
    df.show()

    summary = (
        df.where(F.col("salary") > 0)
        .groupBy("gender")
        .agg(F.count("*").alias("people"), F.avg("salary").alias("avg_salary"))
        .orderBy("gender")
    )
    summary.show()
    return summary


def read_table(spark: SparkSession, table: str) -> DataFrame:
    """Read a table of the platform catalog (Iceberg tables are served through Apache Polaris)."""
    log.info("Reading %s", table)
    df = spark.table(table)
    df.printSchema()
    df.show(10, truncate=False)
    return df
