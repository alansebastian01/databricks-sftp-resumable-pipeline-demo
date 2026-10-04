# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup + Generate "SFTP-like" Files
# MAGIC
# MAGIC This notebook simulates a hospital dropping immutable CSV files into an inbound folder.
# MAGIC In a real design, ADF / managed file transfer would copy from SFTP into this ADLS-backed location.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "sftp_demo")
dbutils.widgets.text("batch_hour", "2026-10-04-01")
dbutils.widgets.text("num_rows", "1000")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema")
batch_hour = dbutils.widgets.get("batch_hour")
num_rows = int(dbutils.widgets.get("num_rows"))

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema_name}.demo_files")

base_path = f"/Volumes/{catalog}/{schema_name}/demo_files"
inbound_path = f"{base_path}/inbound"
checkpoint_path = f"{base_path}/checkpoints/pipeline1"
archive_path = f"{base_path}/archive"

print("Inbound:", inbound_path)
print("Checkpoint:", checkpoint_path)

# COMMAND ----------

from datetime import datetime, timedelta
import csv, os, random, uuid

hour_dt = datetime.strptime(batch_hour, "%Y-%m-%d-%H")
batch_dir = f"{inbound_path}/hospital_a/{batch_hour}"
os.makedirs(batch_dir, exist_ok=True)

file_path = f"{batch_dir}/encounters_{batch_hour}.csv"

random.seed(batch_hour)

with open(file_path, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow([
        "event_id",
        "patient_id",
        "hospital_id",
        "event_ts",
        "diagnosis_code",
        "amount"
    ])
    for i in range(num_rows):
        event_id = f"{batch_hour}-{i:06d}"
        patient_id = f"P{random.randint(1, 300):05d}"
        event_ts = hour_dt + timedelta(seconds=random.randint(0, 3599))
        diagnosis = random.choice(["I10", "E11.9", "J45.909", "Z00.00"])
        amount = round(random.uniform(25, 2500), 2)
        w.writerow([
            event_id,
            patient_id,
            "HOSPITAL_A",
            event_ts.strftime("%Y-%m-%d %H:%M:%S"),
            diagnosis,
            amount
        ])

print(f"Generated {num_rows} rows:")
print(file_path)

# COMMAND ----------

# Optional quick inspection
display(
    spark.read.option("header", True).csv(batch_dir)
)
