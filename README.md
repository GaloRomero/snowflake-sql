# ❄️ Snowflake Fundamentals for Data Engineering

This repository contains a practical and organised collection of SQL scripts designed to help you master the core concepts of **Snowflake**, with a focus on **Data Engineering**. Each section addresses key features commonly used in real-world cloud data pipelines and architectures.

---

## 📁 Repository Contents

### 1. 🔐 Roles, Warehouses, and Context Setup
- Selecting `ROLE`, `WAREHOUSE`, `DATABASE`, and `SCHEMA`.

### 2. 📊 Table Types in Snowflake
- Permanent tables (`PERMANENT`)
- Transient tables (`TRANSIENT`)
- Temporary tables (`TEMPORARY`)

### 3. 👀 Views
- Standard and secure views (`SECURE VIEW`)
- Aggregated and materialised views

### 4. 📦 Stages and Data Loading
- Using internal stages (`@%table`, `@~`, `@STAGE_NAME`)
- Loading data via `COPY INTO` from CSV files
- Creating custom `FILE FORMAT` definitions (CSV, JSON)

### 5. 🌐 Integration with AWS S3
- `STORAGE INTEGRATION` with Amazon S3
- Creating external stages
- Ingesting data from S3 into Snowflake using `COPY INTO`

### 6. 🔄 Snowpipe (Continuous Data Loading)
- Creating `PIPE` objects with `AUTO_INGEST = TRUE`
- Monitoring pipes with `SYSTEM$PIPE_STATUS`
- Setting up S3 event notifications for real-time ingestion

### 7. 🔁 Streams
- `Standard Streams` to track INSERT, UPDATE, and DELETE operations
- `Append-Only Streams` to track only INSERT operations
- ETL use cases for incremental loading

### 8. ⏱️ Tasks
- Automating data loads using tasks
- Scheduled task execution (`SCHEDULE = '1 MINUTE'`)
- Task monitoring with `TASK_HISTORY`

### 9. 🕰️ Time Travel
- Querying table state in the past (`AT TIMESTAMP`, `BEFORE STATEMENT`)
- Recovering deleted records
- Restoring data using table cloning (`CREATE TABLE AS ... BEFORE`)

### 10. 🧬 Cloning
- Cloning entire databases (`CLONE`)
- Restoring dropped objects with `UNDROP`

---

## 🛠️ How to Use the Scripts

1. Copy the contents of the `snowflakeSQL.sql` file.
2. Open Snowflake Web UI or use SnowSQL CLI.
3. Execute the scripts section by section to explore each concept.

---

## 🧠 Requirements

- An active Snowflake account
- A running warehouse (`COMPUTE_WH`)
- Appropriate role permissions (e.g. `ACCOUNTADMIN`)
- (Optional) Configured AWS S3 integration for external stages and Snowpipe

---

## 📚 Additional Resources

- [Snowflake Official Documentation](https://docs.snowflake.com/)
- [SnowSQL CLI](https://docs.snowflake.com/en/user-guide/snowsql)
- [Snowpipe Guide](https://docs.snowflake.com/en/user-guide/data-load-snowpipe)
- [Streams and Tasks](https://docs.snowflake.com/en/user-guide/streams-intro)

---

## ✍️ Author

This script collection was prepared as a practical learning guide for understanding and mastering data engineering concepts using Snowflake, from foundational usage to automation and streaming.

---

