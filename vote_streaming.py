import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,TimestampType,IntegerType
from pyspark.sql.functions import from_json,col,sum, window

spark = (SparkSession.builder
    .appName("RealTimeVoteStreaming") \
        .master("local[*]") \
            .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
                .config("spark.jars","/Users/morshedsarwer/Documents/Data/Projects/realtime-voting-data-engineering/postgresql-42.7.1.jar") \
                    .config("spark.sql.adaptive.enabled","false") \
    ).getOrCreate()

# print(spark)

voter_schema = StructType([
        StructField("voter_id",StringType(),True),
        StructField("voter_name",StringType(),True),
        StructField("voter_email",StringType(),True),
        StructField("date_of_birth",StringType(),True),
        StructField('campaign_platform',StringType(),True),
        StructField('photo_url',StringType(),True),
        StructField('date_of_birth',StringType(),True),
        StructField('gender',StringType(),True),
        StructField('nationality',StringType(),True),
        StructField('registration_number',StringType(),True),
        StructField('address',StructType([
            StructField('street',StringType(),True),
            StructField('city',StringType(),True),
            StructField('state',StringType(),True),
            StructField('country',StringType(),True),
            StructField('postcode',StringType(),True)
        ]),True),
        StructField('email',StringType(),True),
        StructField('phone_number',StringType(),True),
        StructField('cell_number',StringType(),True),
        StructField('picture',StringType(),True),
        StructField('registered_age',IntegerType(),True),
        StructField('voting_time',TimestampType(),True),
        StructField('vote',IntegerType(),True),
        StructField("candidate_id",StringType(),True),
        StructField("candidate_name",StringType(),True),
        StructField("party_affiliation",StringType(),True),
        ])


# reading topic from kafka

vote_df = spark.readStream \
    .format('kafka') \
        .option("kafka.bootstrap.servers","localhost:9092") \
            .option("subscribe","vote_topics") \
                .option("startingOffsets","earliest") \
                    .load() \
                        .selectExpr("CAST(value AS STRING)") \
                            .select(from_json(col("value"), voter_schema).alias("data")) \
                                .select("data.*")
                                


vote_df = vote_df.withColumn("voting_time",col("voting_time").cast(TimestampType())) \
    .withColumn("vote",col("vote").cast(IntegerType()))

    # 
watermark_df = vote_df.withWatermark("voting_time","1 minute")
votes_per_candidate = watermark_df.groupBy("candidate_id","candidate_name",
                                           "party_affiliation","picture") \
                                               .agg(sum("vote").alias("total_votes"))
turnout_by_location = watermark_df.groupBy("address.street").count().alias("total_votes")


# kafka-console-consumer --topic votes_per_candidate --bootstrap-server broker:29092
# votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#         .format("kafka") \
#             .option("kafka.bootstrap.servers","localhost:9092") \
#                 .option("topic","votes_per_candidate") \
#                     .option("checkpointLocation","/Users/morshedsarwer/Documents/Data/Projects/realtime-voting-data-engineering/checkpoints/checkpoint1") \
#                         .outputMode("update") \
#                             .start()

# turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#         .format("kafka") \
#             .option("kafka.bootstrap.servers","localhost:9092") \
#                 .option("topic","turnout_by_location") \
#                     .option("checkpointLocation","/Users/morshedsarwer/Documents/Data/Projects/realtime-voting-data-engineering/checkpoints/checkpoint2") \
#                         .outputMode("update") \
#                             .start()

# votes_per_candidate_to_kafka.awaitTermination()


# to see kafka data Exec command-> kafka-console-consumer --topic votes_per_candidate --bootstrap-server broker:29092
# to see kafka data Exec command-> kafka-console-consumer --topic turnout_by_location --bootstrap-server broker:29092

# Correcting Kafka bootstrap servers option
votes_per_candidate_to_kafka = votes_per_candidate.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "votes_per_candidate") \
    .option("checkpointLocation", "/Users/morshedsarwer/Documents/Data/Projects/realtime-voting-data-engineering/checkpoints/checkpoint1") \
    .outputMode("update") \
    .start()

# Correcting Kafka bootstrap servers option and using turnout_by_location
turnout_by_location_to_kafka = turnout_by_location.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "turnout_by_location") \
    .option("checkpointLocation", "/Users/morshedsarwer/Documents/Data/Projects/realtime-voting-data-engineering/checkpoints/checkpoint2") \
    .outputMode("update") \
    .start()

# Use awaitTermination() once at the end
votes_per_candidate_to_kafka.awaitTermination()



# votes_per_candidate = watermark_df.groupBy("candidate_id","candidate_name",
#                                            "party_affiliation",window("voting_time","1 minute")) \
#                                                .agg(sum("vote").alias("total_votes"))


# votes_per_candidate_print = votes_per_candidate.writeStream \
#         .outputMode("complete") \
#         .format("console") \
#         .start()

# votes_per_candidate_print.awaitTermination()

