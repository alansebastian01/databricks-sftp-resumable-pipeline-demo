# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline 2 - Bronze -> Silver -> Gold
# MAGIC
# MAGIC The Silver write is implemented with `MERGE`, making a rerun idempotent by `event_id`.
# MAGIC Gold is rebuilt from Silver with `CREATE OR REPLACE TABLE`.
# MAGIC
# MAGIC To demonstrate a persistent failure, run this notebook with:
# MAGIC `fail_after_silver=true`
# MAGIC
# MAGIC The notebook intentionally fails AFTER Silver is written.
# MAGIC Later set `fail_after_silver=false` and use **Repair run**.
# MAGIC The task starts again from the beginning, but the MERGE prevents duplicate Silver rows.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "sftp_demo")
dbutils.widgets.dropdown("fail_after_silver", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
fail_after_silver = dbutils.widgets.get("fail_after_silver").lower() == "true"

bronze_table = f"{catalog}.{schema_name}.encounters_bronze"
silver_table = f"{catalog}.{schema_name}.encounters_silver"
gold_table = f"{catalog}.{schema_name}.daily_hospital_summary"

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, to_date, current_timestamp
)

bronze_df = spark.table(bronze_table)

clean_df = (
    bronze_df
      .filter(col("event_id").isNotNull())
      .filter(col("hospital_id").isNotNull())
      .withColumn("event_id", trim(col("event_id")))
      .withColumn("patient_id", trim(col("patient_id")))
      .withColumn("hospital_id", upper(trim(col("hospital_id"))))
      .withColumn("diagnosis_code", upper(trim(col("diagnosis_code"))))
      .withColumn("event_date", to_date(col("event_ts")))
      .withColumn("_silver_updated_ts", current_timestamp())
)

# Deduplicate by the business key before MERGE.
clean_df.createOrReplaceTempView("incoming_encounters")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table}
USING DELTA
AS
SELECT * FROM incoming_encounters WHERE 1 = 0
""")

# Idempotent write: rerunning the same source produces the same final state.
spark.sql(f"""
MERGE INTO {silver_table} AS t
USING (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY event_id
                   ORDER BY _ingest_ts DESC
               ) AS rn
        FROM incoming_encounters
    )
    WHERE rn = 1
) AS s
ON t.event_id = s.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Silver MERGE completed.")

# COMMAND ----------

# Deliberate failure point for the demo.
# This proves that a partially completed task can be safely repaired
# when the writes before the failure are idempotent.
if fail_after_silver:
    raise RuntimeError(
        "DEMO FAILURE: Silver completed, but Gold step intentionally failed. "
        "Set fail_after_silver=false and Repair the failed run."
    )

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {gold_table}
USING DELTA
AS
SELECT
    hospital_id,
    event_date,
    COUNT(*) AS encounter_count,
    COUNT(DISTINCT patient_id) AS patient_count,
    ROUND(SUM(amount), 2) AS total_amount,
    MAX(_silver_updated_ts) AS last_refresh_ts
FROM {silver_table}
GROUP BY hospital_id, event_date
""")

print("Pipeline 2 completed.")
print("Silver:", silver_table)
print("Gold:", gold_table)

# COMMAND ----------

display(spark.table(gold_table).orderBy("event_date", "hospital_id"))
