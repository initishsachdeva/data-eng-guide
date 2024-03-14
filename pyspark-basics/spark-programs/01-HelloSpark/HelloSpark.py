import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


if __name__ == "__main__":
    spark = SparkSession.builder \
                        .appName("Hello Spark") \
                        .master("local[3]") \
                        .getOrCreate()
    
    print("test spark application")
    #spark.stop()