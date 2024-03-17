import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logger import Log4j
from lib.utils import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


if __name__ == "__main__":
    spark = SparkSession.builder \
                        .appName("Hello Spark") \
                        .master("local[3]") \
                        .getOrCreate()
    logger = Log4j(spark)
    logger.info("Starting HelloSpark")
    sample_df = load_df(spark, "sample.csv")
    print(sample_df)


    #print("test spark application")
    #spark.stop()