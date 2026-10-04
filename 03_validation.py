# Databricks notebook source
# MAGIC %md
# MAGIC # Validation
# MAGIC Use after normal runs, retries, and Repair runs to prove idempotency.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "sftp_demo")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")

bronze = f"{catalog}.{schema_name}.encounters_bronze"
silver = f"{catalog}.{schema_name}.encounters_silver"
gold = f"{catalog}.{schema_name}.daily_hospital_summary"

# COMMAND ----------

display(spark.sql(f"""
SELECT 'bronze' AS layer,
       COUNT(*) AS rows,
       COUNT(DISTINCT event_id) AS distinct_event_ids
FROM {bronze}

UNION ALL

SELECT 'silver' AS layer,
       COUNT(*) AS rows,
       COUNT(DISTINCT event_id) AS distinct_event_ids
FROM {silver}
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT event_id, COUNT(*) AS cnt
FROM {silver}
GROUP BY event_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
"""))

# COMMAND ----------

display(spark.table(gold).orderBy("event_date", "hospital_id"))
