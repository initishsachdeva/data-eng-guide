from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, DoubleType

def change_schema_using_struct():
    playstoreSchemaStruct = StructType([
        StructField("App", StringType()),
        StructField("Category", StringType()),
        StructField("Rating", DoubleType()),
        StructField("Reviews", IntegerType()),
        StructField("Installs", IntegerType()),
        StructField("Type", StringType()),
        StructField("Price", IntegerType()),
        StructField("Genres", IntegerType()),
    ])
    return playstoreSchemaStruct



def get_account_schema():
    schema = """load_date date,active_ind int,account_id string,
        source_sys string,account_start_date timestamp,
        legal_title_1 string,legal_title_2 string,
        tax_id_type string,tax_id string,branch_code string,country string"""
    return schema


def read_data(spark, data_file):
     return spark.read \
            .format("csv") \
            .option("header", "true") \
            .load(data_file) 
    
def load_df(spark, data_file):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(data_file)
