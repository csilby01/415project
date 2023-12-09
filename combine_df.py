from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, reduce
from multiprocessing.pool import ThreadPool
import time
import json

keyspace_name = "airlines_keyspace"


spark = SparkSession.builder \
    .appName('Airline') \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1") \
    .config("spark.cassandra.connection.host", "localhost") \
    .getOrCreate()

def read_table(table_name):
    return spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace_name) \
        .load()

def dataframe_to_json(df):
    # Convert DataFrame to RDD of JSON strings
    json_rdd = df.toJSON()
    # Collect JSON strings and create a JSON array
    json_array = [json.loads(json_str) for json_str in json_rdd.collect()]
    return json_array

def combine_tables():
    airlines_df = read_table("airlines").select("airline_id","IATA/IACO","country","name").withColumnRenamed("name", "airline_name").withColumnRenamed("IATA/IACO", "airline_IATA/IACO").withColumnRenamed("country", "airline_country")
    airports_df = read_table("airports").withColumnRenamed("name", "airport_name").select("airport_id", "airport_name","city","country")
    countries_df = read_table("countries").withColumnRenamed("name", "country_name").withColumnRenamed("iso", "country_iso")
    planes_df = read_table("planes").withColumnRenamed("name", "plane_name")
    routes_df = read_table("routes")
    routes_df = routes_df.withColumn("equipment", split("equipment", " ")).select("*", explode("equipment").alias("plane_id")).drop("equipment")
    
    combined_df = routes_df.join(planes_df, routes_df["plane_id"] == planes_df["IATA/IACO"]).drop("IATA/IACO") #Combine routes with planes = a big table
    airport_filter_df = airports_df\
                    .withColumnRenamed("airport_name", "source_airport_name")\
                    .withColumnRenamed("airport_id", "source_airport_id")\
                    .withColumnRenamed("country", "source_country")\
                    .withColumnRenamed("city", "source_city") #airport with just source id,name,city

    combined_df = combined_df.join(airport_filter_df,combined_df["Source airport ID"] == airport_filter_df["source_airport_id"])

# Next, join routes_df again with airports_df to get the "Destination Airport Name"
    airport_filter_df = airports_df\
                        .withColumnRenamed("airport_name", "destination_airport_name")\
                        .withColumnRenamed("airport_id", "destination_airport_id")\
                        .withColumnRenamed("country", "destination_country")\
                        .withColumnRenamed("city", "destination_city") #airport with just destination id,name,city
    combined_df = combined_df.join(
        airport_filter_df,
        combined_df["Destination airport ID"] == airport_filter_df["destination_airport_id"])

    combined_df = combined_df.join(airlines_df, combined_df['Airline ID'] == airlines_df['airline_id']).drop("Airline ID") #combine airline with big table
    
    return combined_df


def filter_airports(input_list, input_df):
    # Filter only non-empty strings from the input list
    remove_empty = [s for s in input_list if s]

    # Filter the DataFrame based on the conditions
    query = [col("text").contains(s) for s in remove_empty]
    filtered_df = input_df.filter(reduce(lambda x, y: x & y, query))
    # Create four threads to run the query 
    pool = ThreadPool(4)

    # Map the query to 4 subsets of dataframe using thread pooling
    result = pool.map(lambda input, combined_df: show_airports(input, combined_df), [inputs] * 4, combined_df)

    return result

