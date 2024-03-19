import os
import sys
from pyspark.sql import *
from pyspark import SparkConf
from collections import namedtuple

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])

if __name__ == "__main__":
    conf = SparkConf() \
        .setMaster("local[3]") \
        .setAppName("HelloRDD")

  # sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    linesRDD = sc.textFile("pyspark-basics/spark-programs/02_Hello_RDD/sample.csv")
    partitionedRDD = linesRDD.repartition(2)

    # as records are loaded in form of text, we have to give a schema first
    colsRDD = partitionedRDD.map(lambda line: line.split(","))
    selectRDD = colsRDD.map(lambda cols : SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filteredRDD = selectRDD.filter(lambda r: r.Age < 40)
    kvRDD = filteredRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = countRDD.collect()
    for x in colsList:
        print(x)


