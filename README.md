# Databricks SFTP -> Lakehouse: Retry, Resume, and Idempotency Demo

## Goal

Demonstrate two logical pipelines:

1. **Pipeline 1: file ingestion**
   - Simulated hospital/SFTP file -> cloud landing folder
   - Databricks Auto Loader
   - durable checkpoint
   - Bronze Delta table

2. **Pipeline 2: transformation**
   - Bronze -> Silver using `MERGE`
   - Silver -> Gold using `CREATE OR REPLACE`
   - deliberate failure after Silver
   - automatic task retries
   - next-day **Repair run**
   - rerun remains idempotent

This demonstrates three separate concepts that are often confused:

- **Checkpoint**: remembers streaming/file-ingestion progress.
- **Retry**: Databricks automatically re-runs a failed task according to the task retry policy.
- **Repair run**: after the job is failed/stopped, manually or programmatically rerun only failed tasks (and optionally downstream tasks).
- **Idempotency**: rerunning a task produces the same final data state instead of duplicates.

---

## Prerequisites

Use a Databricks workspace with Unity Catalog.

Default objects:

- Catalog: `main`
- Schema: `sftp_demo`
- Volume: `main.sftp_demo.demo_files`

If `main` is not writable, change the `catalog` widget in every notebook.

---

## Import the notebooks

Import:

- `00_setup_and_generate.py`
- `01_pipeline1_sftp_to_bronze.py`
- `02_pipeline2_bronze_to_gold.py`
- `03_validation.py`

Databricks recognizes `# Databricks notebook source` files as source-format notebooks.

---

## Create Job: `hospital_sftp_lakehouse_demo`

Create a Lakeflow Job with these tasks.

### Task 1 — `pipeline1_ingest`

Notebook:
`01_pipeline1_sftp_to_bronze`

Recommended retry settings for the demo:

- Max retries: `2`
- Retry interval: `30 seconds`

### Task 2 — `pipeline2_transform`

Depends on:
`pipeline1_ingest`

Notebook:
`02_pipeline2_bronze_to_gold`

Parameters:

- `catalog = main`
- `schema = sftp_demo`
- `fail_after_silver = false`

Recommended retry settings:

- Max retries: `2`
- Retry interval: `30 seconds`

Optional Task 3:
`03_validation`, depends on Pipeline 2.

---

# TEST 1 — Normal run

### Step 1
Run `00_setup_and_generate` with:

- `batch_hour = 2026-10-04-01`
- `num_rows = 1000`

### Step 2
Run the job with:

`fail_after_silver=false`

Expected:

- Pipeline 1 succeeds.
- Pipeline 2 succeeds.
- Bronze has 1000 events.
- Silver has 1000 unique events.
- Gold has the aggregate.

Run `03_validation`.

---

# TEST 2 — Prove file-ingestion idempotency

Without generating any new file, run the full job again.

Expected:

- Auto Loader uses the same checkpoint.
- The old file is not loaded again.
- Bronze remains 1000 events.
- Silver remains 1000 unique events.

This demonstrates checkpoint-based incremental file ingestion.

---

# TEST 3 — Add the next hourly file

Run generator:

- `batch_hour = 2026-10-04-02`
- `num_rows = 500`

Run the job.

Expected:

- Pipeline 1 discovers only the new file.
- Bronze = 1500 events.
- Silver = 1500 events.
- No duplicates.

---

# TEST 4 — Automatic retry

Set Pipeline 2 parameter:

`fail_after_silver=true`

Generate a new hour:

- `batch_hour = 2026-10-04-03`
- `num_rows = 250`

Run the job.

Expected:

1. Pipeline 1 succeeds.
2. Pipeline 2 writes/merges Silver.
3. Pipeline 2 intentionally fails before Gold.
4. Databricks retries Pipeline 2 according to the retry policy.
5. Every retry fails while the failure flag stays `true`.
6. Silver does **not** duplicate rows because it uses `MERGE`.

After retries are exhausted, the job is failed.

---

# TEST 5 — "Next morning" Repair run

This is the scenario:

> The overnight job failed even after retries. Operations fixes the problem the next morning and resumes the failed workflow.

Change the Pipeline 2 task parameter:

`fail_after_silver=false`

Open the failed job run and choose **Repair run**.

Repair the failed task and dependent tasks.

Expected:

- Databricks does **not** need to re-run the already-successful Pipeline 1 task.
- Pipeline 2 starts from the beginning of its notebook.
- Silver MERGE runs again safely.
- No duplicates are created.
- Gold completes.
- The original failed run now has a repair history.

Important:
**Repair Run does not continue at the exact Python line that failed.**
It reruns the failed task. Idempotent task logic is what makes that safe.

---

# TEST 6 — Simulate Pipeline 1 failure and checkpoint resume

For an ingestion-task failure, Auto Loader's checkpoint is the important state.

A real cluster/process failure can occur after one or more micro-batches commit.
When the same stream is restarted with the same checkpoint location:

- committed files stay committed;
- unprocessed files continue from checkpoint;
- already committed files are not duplicated.

Do not delete or change the checkpoint directory for this test.

---

# Architecture

```text
Hospital
   |
   | SFTP
   v
ADF / secure transfer
   |
   v
ADLS / inbound volume
   |
   v
Pipeline 1
Auto Loader
   |
   | checkpoint
   v
Bronze Delta
   |
   v
Pipeline 2
MERGE
   |
   v
Silver Delta
   |
   v
Gold Delta
```

---

# The interview explanation

> "I separate orchestration recovery from data idempotency. Lakeflow Jobs handles retries and Repair runs at the task level. Auto Loader's durable checkpoint tracks file-ingestion progress so a restarted ingestion stream continues from committed state. For downstream transformations, I make each task idempotent with deterministic keys and Delta MERGE/replace semantics. Therefore, if the overnight workflow fails after partially completing, automatic retries are safe; and if retries are exhausted, the next-day Repair run can rerun only the failed tasks without duplicating data."

---

# Production notes

- Keep checkpoint storage durable.
- Never put lifecycle deletion policies on active checkpoints.
- Use one checkpoint location per independent stream.
- Keep source files immutable where possible.
- Use deterministic business keys for `MERGE`.
- Do not rely on `append` for tasks that might be repaired after a partial write.
- Add alerts on final job failure.
- Keep job-run audit information and data-quality metrics.
