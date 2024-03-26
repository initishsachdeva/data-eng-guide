import re
import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def parse_gender(gender):
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"
    

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("UDF Demo") \
        .master("local[2]") \
        .getOrCreate()
    
    survey_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("pyspark-basics/spark-programs/08_UDF_Demo/survey.csv")
    
    survey_df.show(5)

    # before using any user defined function, we need to register it first
    parse_gender_udf = udf(parse_gender, returnType=StringType())
    # using column expression
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(5)


    # using sql expression 
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(5)