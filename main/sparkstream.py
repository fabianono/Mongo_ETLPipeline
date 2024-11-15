from pymongo import MongoClient
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_sparkconnection():
    spark = None
    spark = SparkSession.builder.appName("spark_streaming")\
                                .config("spark.jars",
                                        ".virtualenv/lib/python3.11/site-packages/pyspark/jars/mongo-spark-connector_2.12-10.4.0.jar,"
                                        ".virtualenv/lib/python3.11/site-packages/pyspark/jars/commons-pool2-2.12.0.jar,"
                                        ".virtualenv/lib/python3.11/site-packages/pyspark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar,"
                                        ".virtualenv/lib/python3.11/site-packages/pyspark/jars/kafka-clients-3.5.1.jar,"
                                        ".virtualenv/lib/python3.11/site-packages/pyspark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar")\
                                .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/my_database.my_collection") \
                                .config('spark.mongodb.output.uri',"mongodb://127.0.0.1:27017/my_database.my_collection")\
                                .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logging.info("Spark connection successfully created")

    return spark

def mongo_connect():
    mongodb_uri = "mongodb://127.0.0.1:27017"
    client = MongoClient(mongodb_uri)
    db = client['my_database']
    collection = db['my_collection']

    print("Mongodb connected")
    return collection

def kafkaconnect(sparkconnection):
    spark_df = None
    spark_df = sparkconnection.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers','127.0.0.1:9092') \
                .option('subscribe','users') \
                .option('startingOffsets', 'earliest') \
                .load()
    
    print (spark_df)
    return spark_df

def jsonstream(kafkastream):
    json_df = kafkastream.selectExpr("CAST(value AS STRING) as json_value")

    print(json_df)
    return json_df


# sparkconnected = create_sparkconnection()
# mongo_connect()
# kc = kafkaconnect(sparkconnected)
# jsonstream(kc)


if __name__ == "__main__":
    sparkconnected = create_sparkconnection()
    
    if sparkconnected is not None:
        df = kafkaconnect(sparkconnected)
        finaldf = jsonstream(df)
        mongosession = mongo_connect()

        if mongo_connect is not None:
            streaming = finaldf.writeStream.outputMode("append") \
                                .format("mongo") \
                                .option('checkpointLocation','/tmp/checkpoint') \
                                .option('spark.mongodb.output.uri',"mongodb://127.0.0.1:27017/my_database.my_collection") \
                                .start()
            
            streaming.awaitTermination()