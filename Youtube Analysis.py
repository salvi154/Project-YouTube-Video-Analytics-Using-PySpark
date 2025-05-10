# Databricks notebook source
from pyspark.sql.functions import col, to_date, sum , count, desc,when,regexp_replace

# COMMAND ----------


# Load data from DBFS
file_path = "dbfs:/FileStore/tables/INvideos.csv"
df = spark.read.option("header", True).csv(file_path)

df.printSchema()
df.display()

# COMMAND ----------

df = df.withColumn("views", col("views").cast("int")) \
       .withColumn("likes", col("likes").cast("int")) \
       .withColumn("dislikes", col("dislikes").cast("int")) \
       .withColumn("comment_count", col("comment_count").cast("int")) \
       .withColumn("publish_date", to_date(col("publish_time")))

# COMMAND ----------

df.printSchema()
df.display(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Cleaning Steps

# COMMAND ----------

required_cols = ["video_id", "publish_time"]

# Drop rows where any of these columns are null
df = df.dropna(subset=required_cols)

# COMMAND ----------

numeric_columns = ["views", "likes", "dislikes", "comment_count"]

for column in numeric_columns:
    df = df.withColumn(
        column,
        regexp_replace(col(column), "[^0-9]", "").cast("int")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Transformations

# COMMAND ----------

top_videos = df.select("title","channel_title","views") \
    .orderBy(desc("views")).limit(10)

top_videos.display()

# COMMAND ----------

category_stats = df.groupBy("category_id")\
    .agg(count("video_id").alias("video_count"))\
    .orderBy(desc("video_count"))

category_stats.display()

# COMMAND ----------

no_comment = df.withColumn('no_Comment',when(col('comments_disabled') == True,'Video has no comment').when(col('comments_disabled') == False, 'Video has comments').otherwise(df.comments_disabled)).display()

# COMMAND ----------

df = df.withColumn("views", regexp_replace("views","[^0-9]","").cast("int"))


# COMMAND ----------

df = df.fillna({
    'views' : 0,
    'likes' : 0,
    'dislikes' : 0,
    'comment_count' :0
})

# COMMAND ----------

daily_views = df.groupBy('publish_date')\
    .agg(sum("views").alias('total_views'))\
    .orderBy('publish_date')

daily_views.display()



# COMMAND ----------

df = df.withColumn('engagement_rate',(col('likes') + col('comment_count')) / col('views'))

engagement = df.select("title", "channel_title", "engagement_rate") \
            .orderBy(desc("engagement_rate")) \
            .filter(col("views") > 10000)  # filter to avoid skewed data

display(engagement.limit(10))
