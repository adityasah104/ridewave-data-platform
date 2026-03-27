import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
import env
from env import source_path, target_path, name

args        = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── CONFIG — CHANGE YOUR_NAME BEFORE SAVING ───────────────────────
YOUR_NAME   = name
SOURCE_PATH = source_path
TARGET_PATH = target_path


# ── READ RAW CSV ──────────────────────────────────────────────────
# WHY: Read raw data exactly as it is — no changes yet
df_raw = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(SOURCE_PATH)

print(f"Raw rows: {df_raw.count()}")

df_clean = df_raw \
    .filter(F.col("ride_id").isNotNull()) \
    .withColumn("fare_amount",
                F.col("fare_amount").cast("double")) \
    .withColumn("ride_date",
                F.to_date(F.col("ride_date"), "yyyy-MM-dd")) \
    .withColumn("ride_status",
                F.lower(F.col("ride_status"))) \
    .withColumn("ingestion_timestamp",
                F.lit(datetime.utcnow().isoformat()))

print(f"Clean rows : {df_clean.count()}")
print(f"Rows dropped: {df_raw.count() - df_clean.count()}")

df_clean.write.mode("overwrite").parquet(TARGET_PATH)
print(f"Written to: {TARGET_PATH}")
