from pyspark.sql import SparkSession
from pyspark.sql.functions import window,explode
import pyspark.sql.functions as func
import re
from pyspark.sql.functions import udf, col, split, current_timestamp
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("test1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Define UDF for extracting hashtags from comments
@udf(returnType=StringType())
def extract_hashtags(s):
    hashtags = re.findall(r'#(\w+)', s)
    return ' '.join(['#' + hashtag if not hashtag.startswith('#') else hashtag for hashtag in hashtags])

# Read data from the Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "neutral") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

print("Consumer Stream Processing has Started")

# Extract the comments and hashtags from the Kafka message value
df1 = df.selectExpr("CAST(value AS STRING)", "topic")
df2 = df1.withColumn("hashtags", extract_hashtags(col("value"))) \
        .withColumn("timestamp", current_timestamp())
df2 = df2.withColumn("hashtags", explode(split(col("hashtags"), " ")))
# Create a temporary view of the dataframe
df2.createOrReplaceTempView("Instagram_Comment")

print("Temporary view has been created!")

# Filtering dataframe to include only comments with hashtag and having topic as neutral
filtered_df = spark.sql("SELECT * FROM Instagram_Comment WHERE hashtags != '' AND topic = 'neutral'")
c_df = filtered_df \
            .groupBy(window("timestamp","3 minutes")) \
            .agg(func.count("hashtags").alias("count of hastags"))
# Grouping comments by a 3-minute window and counting the number of comments in each window
count_df = filtered_df \
            .groupBy(window("timestamp","3 minutes"), "hashtags") \
            .agg(func.count("hashtags").alias("count"))

# Find the most frequent hashtags
most_frequent_hashtags_df = count_df \
                                .orderBy(func.desc("count")) \
                                .select("window", "hashtags", "count") \
                                .limit(10)  # Limit to top 10 hashtags
spark.conf.set("spark.sql.streaming.stateStore.stateSchemaCheck", "false")
# Starting the streaming query to write the results to the console
query1= c_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()


query2 = most_frequent_hashtags_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()
query1.awaitTermination()
query2.awaitTermination()

