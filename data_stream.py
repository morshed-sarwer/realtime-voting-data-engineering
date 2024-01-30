import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json,col
from pyspark.sql.types import StructType,StructField,IntegerType,TimestampType,StringType

if __name__ == "__main__":
    # print(pyspark.__version__)
    
    # spark = (SparkSession.builder
    #          .appName("VotingState")
    #          .master("local[*]")
    #          .config("spark.jars.packages",
    #                  "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0")
    #          .config("spark.jars","/Users/morshedsarwer/Documents/Data/Projects/realtime-voting-data-engineering/postgresql-42.7.1.jar")
    #          .config("spark.sql.adaptive.enabled","false")
    #          .getOrCreate())
    
    spark = SparkSession.builder \
    .appName("VotingApp") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

    voter_schema = StructType([
        StructField("voter_id",StringType(),True),
        StructField("voter_name",StringType(),True),
        StructField("voter_email",StringType(),True),
        StructField("date_of_birth",StringType(),True),
    ])
    
    # read kafka topics
    
    # votes_df = spark.readStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers","localhost:9092") \
    #     .option("subscribe","voter_topic") \
    #     .option("startingOffsets","earliest") \
    #     .load() \
    #     .selectExprt("CAST (vlaue as STRING)") \
    #     .select(from_json(col("vlaue"), voter_schema).alias("data")) \
    #     .select("data.*")
    
    votes_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "voter_topic") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), voter_schema).alias("data")) \
    .select("data.*")


    
    # dispaly to console
    query = votes_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    query.awaitTermination()
    
    
    # inserting data into postgres database
    
    def process_batch(batch_df, batch_id):
        print(f"Processing Batch ID: {batch_id}")
    
    postgres_url = "jdbc:postgresql://localhost:5432/voting"
    postgres_properties = {
        "user": "postgres",
        "password": "123",
        "driver": "org.postgresql.Driver",
    }
    
    batch_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batch, batch_id: batch.write.jdbc(postgres_url, "spark_table", mode="append", properties=postgres_properties)) \
    .start()
    
    
    # Your existing logic for writing to Kafka
    # batch_df.selectExpr("to_json(struct(*)) AS value") \
    #     .write \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", "localhost:9092") \
    #     .option("topic", "another_topic") \
    #     .mode("append") \
    #     .save()
    