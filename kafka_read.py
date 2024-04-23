from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf, col,current_timestamp
import mysql.connector
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test3").getOrCreate()

# Define the MySQL database connection properties
hostname = "localhost"
user = "dbtuser"
password = "root" # whatever you have set as your password in mysql
database = "dbt" # or whatever you named your database in mysql
table_name = "instagram_comments"

# Drop the table if it exists
def drop_table_if_exists(tablename):
    db = mysql.connector.connect(host=hostname, user=user, password=password, database=database)
    cursor = db.cursor()
    sql = f"DROP TABLE IF EXISTS {tablename}"
    cursor.execute(sql)
    db.close()

# Define functions for preprocessing the comments
def remove_digits(s):
    return ''.join(c for c in s if not c.isdigit())

def remove_punctuations(s):
    return re.sub(r'[^\w\s#]', '', s)

# Define UDF for extracting hashtags from comments
@udf(returnType = StringType())
def extract_hashtags(s):
    hashtags = re.findall(r'#(\w+)', s)
    return ' #'.join(hashtags)

# Define UDF for preprocessing the comments
@udf(returnType = StringType())
def preprocess(s):
    
    news = remove_punctuations(s)
    return news

# Define the schema of the data
schema = StructType([
    StructField("value", StringType(), True),
    StructField("topic", StringType(), True),
    StructField("hashtags", StringType(), True),
    StructField("timestamp", StringType(), True) # New column added
])

# Read data from the Kafka topics
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "positive,negative,neutral") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

# Extract hashtags from the comments using UDF
df = df.selectExpr("CAST(value AS STRING)", "topic") \
       .withColumn("hashtags", extract_hashtags(col("value")))

# Preprocess the comments using UDF
df = df.withColumn("value", preprocess(col("value")))

# Define the MySQL database target properties
db_target_properties = {"user":user, "password":password}

# Function to write the data to MySQL
def foreach_batch_function(df, epoch_id):
    print("Writing to database")    
    df = df.withColumn("timestamp", current_timestamp())
    df.write.mode("append").jdbc(url=f'jdbc:mysql://{hostname}/{database}', table=table_name, properties=db_target_properties)

# Drop the table if it exists and start the streaming query
drop_table_if_exists(table_name)
out = df.writeStream.outputMode("append").foreachBatch(foreach_batch_function).start()
out.awaitTermination()
