from pyspark.sql import SparkSession
from pyspark.sql.functions import split, window, count
import pyspark.sql.functions as func
from pyspark.sql import SparkSession
from pyspark.sql.functions import window,explode
import pyspark.sql.functions as func
import re
from pyspark.sql.functions import udf, col, split, current_timestamp
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("test3").getOrCreate()

# Read data from the database table
jdbcUrl = "jdbc:mysql://localhost:3306/dbt"
connectionProperties = {
  "user": "dbtuser",
  "password": "root"
}
df = spark.read.jdbc(url=jdbcUrl, table="instagram_comments", properties=connectionProperties)

@udf(returnType=StringType())
def extract_hashtags(s):
    hashtags = re.findall(r'#(\w+)', s)
    return ' '.join(['#' + hashtag if not hashtag.startswith('#') else hashtag for hashtag in hashtags])
# Extract the comment and hashtags from the value column
df1 = df.withColumn('hashtags', extract_hashtags(col("value")))
df1 = df1.withColumn("hashtags", explode(split(col("hashtags"), " ")))
# Create a temporary view of the dataframe
df1.createOrReplaceTempView("Instagram_Comment_view")

# Filter the dataframe to include only positive comments
positive_df = spark.sql("SELECT * FROM Instagram_Comment_view WHERE hashtags != '' AND topic = 'positive'")

# Group the comments by a 30-minute window and count the number of hashtags in each window
count_df = positive_df \
            .groupBy(window("timestamp", "30 minutes")) \
            .agg(func.count("hashtags").alias("count"))
c_df = positive_df \
            .groupBy(window("timestamp","30 minutes"), "hashtags") \
            .agg(func.count("hashtags").alias("count"))

# Find the most frequent hashtags
most_frequent_hashtags_df = c_df \
                                .orderBy(func.desc("count")) \
                                .select("window", "hashtags", "count") \
                                .limit(10)
# Write the results to terminal
count_df.show()
most_frequent_hashtags_df.show()


