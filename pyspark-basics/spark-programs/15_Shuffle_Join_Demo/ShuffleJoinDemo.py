import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder \
                    .appName("Sfuffle Join Demo") \
                    .master("local[3]") \
                    .getOrCreate()
    
    flight_time_df1 = spark.read.json("pyspark-basics/spark-programs/15_Shuffle_Join_Demo/data/d1/")
    flight_time_df2 = spark.read.json("pyspark-basics/spark-programs/15_Shuffle_Join_Demo/data/d2/")

    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner") 
    
    # when your dataframe is small you can use Broadcast join
    # join_df = flight_time_df1.join(broadcast(flight_time_df2), join_expr, "inner") 

    join_df.collect()
    input("press a key to stop...")

   
