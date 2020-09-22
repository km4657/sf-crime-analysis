import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf





# TODO Create a schema for incoming resources

#JSON, all fields StringType

schema = StructType([StructField('address', StringType(), True),
    StructField('address_type', StringType(), True),
    StructField('agency_id', StringType(), True),
    StructField('call_date', StringType(), True),
    StructField('call_date_time', StringType(), True),
    StructField('city', StringType(), True),
    StructField('common_location', StringType(), True),
    StructField('crime_id', StringType(), True),
    StructField('disposition', StringType(), True),
    StructField('offense_date', StringType(), True),
    StructField('original_crime_type_name', StringType(), True),
    StructField('report_date', StringType(), True),
    StructField('state', StringType(), True)])

def run_spark_job(spark):

    # TODO Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port

    df = spark \
        .readStream \
        .format("kafka") \
        .option("subscribe", "org.udacity.datastreaming.project2.callevents") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger",200) \
        .option("maxRatePerPartition", 1000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()


    # Show schema for the incoming resources for checks
    df.printSchema()

    # TODO extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    
    distinct_table = service_table.select('original_crime_type_name', 'disposition')
    
    distinct_table.printSchema()

    
    # test ingest from kafka with correct schema
    #newquery = (
    #    service_table
    #        .writeStream
    #        .outputMode("append")
    #        .format("console")
    #        .start()
    #)
    #newquery.awaitTermination()


    # count the number of original crime type
    # verify that count increases toward notebook result
    agg_df = distinct_table.groupBy('original_crime_type_name', 'disposition').count().orderBy('count', ascending=False)
    

    # TODO Q1. Submit a screen shot of a batch ingestion of the aggregation
    # TODO write output stream
    

    #query = (
    #     agg_df
    #         .writeStream
    #         .format("console")  # memory = store in-memory table (for testing only in Spark 2.0)
    #        .queryName("counts")  # counts = name of the in-memory table
    #         .outputMode("complete")  # complete = all the counts should be in the table
    #         .start()
    # )


    # TODO attach a ProgressReporter
    #query.awaitTermination()

    # TODO get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath, multiLine=True)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # TODO rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")


    # TODO join on disposition column
    join_query = (
        agg_df.join(radio_code_df, "disposition")
        .writeStream
        .outputMode("complete")
        .format("console")
        .start()
    )

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
