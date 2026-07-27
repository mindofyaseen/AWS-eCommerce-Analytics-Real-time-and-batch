import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = "ecomm-analytics-yaseen-2026"
OUTPUT_PATH = f"s3://{BUCKET}/processed/events/"

SCHEMA = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True),
])

raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="ecomm_flink_db",
    table_name="raw",
    transformation_ctx="raw_source",
)
df = raw_dyf.toDF()

df = df.select(
    F.col("event_time"),
    F.col("event_type"),
    F.col("product_id").cast(StringType()),
    F.col("category_id").cast(StringType()),
    F.col("category_code"),
    F.col("brand"),
    F.col("price").cast(DoubleType()),
    F.col("user_id").cast(StringType()),
    F.col("user_session"),
)

df = df.filter(F.col("event_type").isin("view", "cart", "purchase", "remove_from_cart"))
df = df.dropna(subset=["event_time", "event_type", "user_id"])

df = df.withColumn(
    "event_ts",
    F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss z")
).drop("event_time").withColumnRenamed("event_ts", "event_time")

df.write.mode("overwrite").partitionBy("event_type").parquet(OUTPUT_PATH)

job.commit()
