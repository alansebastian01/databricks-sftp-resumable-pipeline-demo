# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 1 - Inbound Files -> Bronze
# MAGIC
# MAGIC Uses Databricks Auto Loader with a durable checkpoint.
# MAGIC If the task/cluster fails, rerunning the task resumes from the last committed checkpoint.
# MAGIC Already-processed immutable files are not ingested again.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "sftp_demo")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")

base_path = f"/Volumes/{catalog}/{schema_name}/demo_files"
inbound_path = f"{base_path}/inbound"
checkpoint_path = f"{base_path}/checkpoints/pipeline1"
bronze_table = f"{catalog}.{schema_name}.encounters_bronze"

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, DoubleType
)
from pyspark.sql.functions import current_timestamp, input_file_name

schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("patient_id", StringType(), True),
    StructField("hospital_id", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("diagnosis_code", StringType(), True),
    StructField("amount", DoubleType(), True),
])

source_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("header", "true")
         .schema(schema)
         .load(inbound_path)
         .withColumn("_source_file", input_file_name())
         .withColumn("_ingest_ts", current_timestamp())
)

query = (
    source_df.writeStream
             .format("delta")
             .option("checkpointLocation", checkpoint_path)
             .trigger(availableNow=True)
             .toTable(bronze_table)
)

query.awaitTermination()

print("Pipeline 1 completed.")
print("Bronze table:", bronze_table)
print("Checkpoint:", checkpoint_path)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            COUNT(*) AS bronze_rows,
            COUNT(DISTINCT event_id) AS distinct_event_ids,
            MIN(event_ts) AS min_event_ts,
            MAX(event_ts) AS max_event_ts
        FROM {bronze_table}
    """)
)
