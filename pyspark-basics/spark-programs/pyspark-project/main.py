import os
import sys
from pyspark.sql import SparkSession
from lib import Utils, DataLoader
import logging as log

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    log.basicConfig(level=log.INFO)
    try:        
        log.info('Creating spark session')
        spark = Utils.get_spark_session()
        log.info('spark session created')
        log.info('Reading google playstore data')
        google_play_store_df = DataLoader.read_data(spark, "pyspark-basics/spark-programs/pyspark-project/data/googleplaystore.csv")
        google_play_store_df.show(5)
        log.info('Data types of columns available in google play store datasets are - > ')
        google_play_store_df.printSchema()

    except Exception as e:
        log.error(e)
        log.error('Error in main function ')
    