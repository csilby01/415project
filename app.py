from flask import Flask, jsonify, request, render_template
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, reduce
from multiprocessing.pool import ThreadPool
import pyspark.sql.functions as F
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
    return json.dumps(json_array)

def combine_tables():
    airlines_df = read_table("airlines").select("airline_id","IATA/IACO","country","name").withColumnRenamed("name", "airline_name").withColumnRenamed("IATA/IACO", "airline_IATA/IACO").withColumnRenamed("country", "airline_country")
    airports_df = read_table("airports").withColumnRenamed("name", "airport_name").select("airport_id", "airport_name","city","country","IATA","ICAO")
    planes_df = read_table("planes").withColumnRenamed("name", "plane_name")
    routes_df = read_table("routes")
    routes_df = routes_df.withColumn("equipment", split("equipment", " ")).select("*", explode("equipment").alias("plane_id")).drop("equipment")
    
    combined_df = routes_df.join(planes_df, routes_df["plane_id"] == planes_df["IATA/IACO"]).drop("IATA/IACO") #Combine routes with planes = a big table
    airport_filter_df = airports_df\
                    .withColumnRenamed("airport_name", "source_airport_name")\
                    .withColumnRenamed("airport_id", "source_airport_id")\
                    .withColumnRenamed("country", "source_country")\
                    .withColumnRenamed("IATA", "source_airport_IATA")\
                    .withColumnRenamed("ICAO", "source_airport_ICAO")\
                    .withColumnRenamed("city", "source_city") #airport with just source id,name,city

    combined_df = combined_df.join(airport_filter_df,combined_df["Source airport ID"] == airport_filter_df["source_airport_id"])

# Next, join routes_df again with airports_df to get the "Destination Airport Name"
    airport_filter_df = airports_df\
                        .withColumnRenamed("airport_name", "destination_airport_name")\
                        .withColumnRenamed("airport_id", "destination_airport_id")\
                        .withColumnRenamed("country", "destination_country")\
                        .withColumnRenamed("IATA", "destination_airport_IATA")\
                        .withColumnRenamed("ICAO", "destination_airport_ICAO")\
                        .withColumnRenamed("city", "destination_city") #airport with just destination id,name,city
    combined_df = combined_df.join(
        airport_filter_df,
        combined_df["Destination airport ID"] == airport_filter_df["destination_airport_id"])

    combined_df = combined_df.join(airlines_df, combined_df['Airline ID'] == airlines_df['airline_id']).drop("Airline ID") #combine airline with big table
    
    return combined_df


def filter_airports(json_data, combined_df):
    
    # Extract filter criteria from the JSON
    source_type = ""
    destination_type = ""

    source_value = json_data["source"]["value"]
    destination_value = json_data["destination"]["value"]
    plane_filter = json_data["filters"]["plane"]
    airline_filter = json_data["filters"]["airline"]
    direct_only = json_data["filters"]["direct_only"]

    if json_data["source"]["type"] == "country":
        source_type = "source_country"
    elif json_data["source"]["type"] == "city":
        source_type = "source_city"
    else:
        if len(source_value) == 3:
            source_type = "source_airport_IATA"
        elif len(source_value) == 4:
            source_type = "source_airport_ICAO"
        else:
            source_type = "source_airport_name"
        
    if json_data["destination"]["type"] == "country":
        destination_type = "destination_country"
    elif json_data["destination"]["type"] == "city":
        destination_type = "destination_city"
    else:
        if len(destination_value) == 3:
            destination_type = "destination_airport_IATA"
        elif len(destination_value) == 4:
            destination_type = "destination_airport_ICAO"
        else:
            destination_type = "destination_airport_name"

    # Construct filter conditions
    conditions = []

    # Filter by source and destination countries
    if source_value:
        conditions.append(F.col(source_type) == source_value)
    if destination_value:
        conditions.append(F.col(destination_type) == destination_value)

    # Filter by plane and airline
    if plane_filter:
        if len(plane_filter) < 4:
            conditions.append(F.col("plane_id").contains(plane_filter))
        else:    
            conditions.append(F.col("plane_name").contains(plane_filter))
    if airline_filter:
        if len(airline_filter) < 3:
            conditions.append(F.col("airline_IATA/IACO").contains(airline_filter))
        else:
            conditions.append(F.col("airline_name").contains(airline_filter))

    # Filter by direct_only flag
    if direct_only:
        conditions.append(F.col("stops") == 0)

    combined_condition = None
    for condition in conditions:
        if combined_condition is None:
            combined_condition = condition
        else:
            combined_condition = combined_condition & condition

    # Apply the combined condition to filter the DataFrame
    filtered_df = combined_df.filter(combined_condition)
    
    return dataframe_to_json(filtered_df.select("airline_name","source_airport_name","source_city","source_country","destination_airport_name","destination_city","destination_country","plane_name","stops"))


combined_table = combine_tables()
combined_table.show()

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://localhost:8080"}})


@app.route('/')
def index():
    return "Hello World"

@app.route('/find', methods=['POST'])
def filter():
    data = request.get_json()
    print(data)
    filtered_results = filter_airports(data, combined_table) ## return a filtered data frame
    ## Print filtered results on page

    return str(filtered_results)
    
if __name__ == '__main__':
    app.run(debug=True, port=3000)