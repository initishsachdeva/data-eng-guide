from pyspark.sql import SparkSession


def get_spark_session():
    return SparkSession.builder \
            .appName("Pyspark Sample Project") \
            .master("local[3]") \
            .enableHiveSupport() \
            .getOrCreate()
    