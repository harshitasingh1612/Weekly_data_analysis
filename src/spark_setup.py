"""
This file creates a spark session.
"""
from pyspark.sql import SparkSession

def build_spark_session(app_name:str) -> SparkSession:
    """Build a Spark session with the given configuration.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A Spark session with the given configuration.
    """
    print("Building a spark session!")

    spark = (
        SparkSession.builder.appName(app_name)
        # set the spark configuration for run-time
        .config("spark.sql.shuffle.partitions", 200)
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark