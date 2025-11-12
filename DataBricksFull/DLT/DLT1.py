# Databricks notebook source
spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### A DLT pipeline is defined using Python or SQL code in a notebook. Databricks then manages the execution, orchestration, error handling, and monitoring of these pipelines.
# MAGIC
# MAGIC Let's create a detailed example that covers common DLT features, including:
# MAGIC
# MAGIC Bronze, Silver, Gold Layers: A typical Lakehouse architecture.
# MAGIC Auto Loader: For incremental data ingestion.
# MAGIC Expectations (Data Quality Rules): For enforcing data quality and handling invalid records.
# MAGIC Table Properties: Setting additional metadata.
# MAGIC Generated Columns: Creating new columns based on existing ones.
# MAGIC Streaming and Materialized Views: Understanding STREAMING LIVE TABLE and LIVE TABLE.
# MAGIC DLT UI Configuration: How to set up and run the pipeline.
# MAGIC DLT Pipeline Example: Customer Order Processing
# MAGIC We'll simulate processing customer order data through Bronze, Silver, and Gold layers.

# COMMAND ----------

# MAGIC %md
# MAGIC ### What ever the DataSets created are tied to pipeline ID
# MAGIC
# MAGIC Internal Schema will be created to Maintain Pipeline.
# MAGIC
# MAGIC This Schema Hidden from Developers.

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario:
# MAGIC Raw customer order data arrives in a cloud storage location (e.g., DBFS, S3, ADLS). We want to:
# MAGIC
# MAGIC Bronze Layer: Ingest raw JSON data incrementally into a streaming table.
# MAGIC
# MAGIC Silver Layer:
# MAGIC
# MAGIC           Cleanse and enrich the Bronze data.
# MAGIC           Apply data quality checks (expectations).
# MAGIC           Convert data types, handle missing values.
# MAGIC           Create a derived order_status column.
# MAGIC           Gold Layer:
# MAGIC           Aggregate data for reporting (e.g., daily sales summary).
# MAGIC           DLT Notebook Code (Python)
# MAGIC           You would put this code into a Databricks Notebook. Let's assume the notebook is named customer_orders_dlt_pipeline.py.
# MAGIC
# MAGIC DLT Pipeline Configuration in Databricks UI
# MAGIC Once you have the DLT Python notebook, you'll create and configure the DLT pipeline in the Databricks UI:
# MAGIC
# MAGIC Navigate to the "Workflows" (or "Jobs" in older UI) -> "Delta Live Tables" section.
# MAGIC Click "Create Pipeline".
# MAGIC Here's how you'd fill out the configuration options:
# MAGIC
# MAGIC 1. New Pipeline Screen
# MAGIC
# MAGIC         Pipeline name: Customer_Order_Pipeline
# MAGIC         Product Edition: Choose Advanced to get all features like expectations and enhanced autoscaling.
# MAGIC         Pipeline mode:
# MAGIC         Triggered: (Recommended for most ETL) Runs once and stops. Useful for daily, hourly, or on-demand updates.
# MAGIC         Continuous: Runs continuously, processing new data as it arrives (more for low-latency scenarios).
# MAGIC         For this example, choose Triggered.
# MAGIC         Source Libraries:
# MAGIC         Click "Add Notebook Library".
# MAGIC         Path: Browse to your DLT notebook (e.g., /Users/your_user/customer_orders_dlt_pipeline.py).
# MAGIC         Target schema (Database): orders_dlt_db (this is the database where your DLT tables will be created).
# MAGIC         Storage location: dbfs:/FileStore/dlt_pipeline_storage/customer_orders (This is where DLT stores its internal metadata, checkpoints, and by default, your tables if not specified in dlt.table's path option. For this example, we've explicitly set path in dlt.table decorators).
# MAGIC         Cluster policy: (Optional but recommended) If you have defined cluster policies, select one to ensure consistent cluster configurations.
# MAGIC         Cluster size:
# MAGIC         Workers: Start with 2-4 for development, scale up as needed.
# MAGIC         Minimum workers: 0 (for triggered pipelines, it can scale down to 0 when idle)
# MAGIC         Maximum workers: 8 (adjust based on workload)
# MAGIC         Instance type: Choose appropriate instance types (e.g., i3.xlarge, r5.xlarge, Standard_DS3_v2)
# MAGIC         Photon Acceleration: Enable for performance benefits.
# MAGIC         Advanced options:
# MAGIC         Configuration:
# MAGIC         You can pass key-value pairs here. E.g., for development, you might set spark.databricks.delta.properties.defaults.enableChangeDataFeed = true
# MAGIC         You could also pass parameters from a widget or job parameter to your DLT notebook.
# MAGIC         Tags: Add tags for cost tracking or organization (e.g., project: customer_data, owner: data_team).
# MAGIC         Permissions: Configure who can run, edit, or manage the pipeline.
# MAGIC         Webhooks: Integrate with external systems for notifications (e.g., Slack, custom webhooks).
# MAGIC         Notifications: Email alerts for pipeline start, success, or failure.
# MAGIC 2. Click "Create"
# MAGIC Once created, you'll be redirected to the pipeline's detail page.
# MAGIC
# MAGIC 3. Run the Pipeline
# MAGIC
# MAGIC Click the "Start" button on the pipeline detail page.
# MAGIC You'll see the DAG (Directed Acyclic Graph) visualize the tables and their dependencies.
# MAGIC Monitor the progress, logs, and data quality metrics.
# MAGIC Accessing the Data
# MAGIC After the pipeline runs successfully, you can query the tables:
# MAGIC
# MAGIC SQL
# MAGIC
# MAGIC -- In a separate Databricks SQL query or another notebook:
# MAGIC
# MAGIC SELECT * FROM orders_dlt_db.bronze_raw_orders;
# MAGIC SELECT * FROM orders_dlt_db.silver_processed_orders;
# MAGIC SELECT * FROM orders_dlt_db.gold_daily_sales_summary;
# MAGIC
# MAGIC -- You can also query the view
# MAGIC SELECT * FROM orders_dlt_db.customer_360_view;
# MAGIC
# MAGIC -- Explore the schema
# MAGIC
# MAGIC DESCRIBE orders_dlt_db.silver_processed_orders;
# MAGIC
# MAGIC Key DLT Concepts Used:
# MAGIC
# MAGIC @dlt.table: Decorator to define a DLT table.
# MAGIC
# MAGIC @dlt.view: Decorator to define a DLT view (non-materialized).
# MAGIC
# MAGIC dlt.read_stream("table_name"): Reads incrementally from a DLT streaming table.
# MAGIC
# MAGIC dlt.read("table_name"): Reads from a DLT table in batch mode.
# MAGIC
# MAGIC Auto Loader (cloudFiles format): The recommended way to ingest data incrementally and efficiently from cloud storage. It automatically tracks new files and handles schema evolution.
# MAGIC
# MAGIC table_properties: Allows you to set custom metadata for your tables.
# MAGIC
# MAGIC expect_violations / dlt.expect_*: Data quality rules. DLT monitors these rules and provides actions (drop, fail, quarantine, etc.) when violations occur. The DLT UI shows metrics on expectation compliance.
# MAGIC
# MAGIC Streaming vs. Materialized Tables:
# MAGIC
# MAGIC STREAMING LIVE TABLE (default for dlt.table if readStream is used): Continuously processes new data.
# MAGIC
# MAGIC LIVE TABLE (default for dlt.table if read is used or pipeline_type is batch): Refreshed as a batch.
# MAGIC
# MAGIC comment: Provides descriptions for your tables, visible in the Unity Catalog or Hive Metastore.
# MAGIC
# MAGIC spark.sql.functions: Standard Spark functions used for transformations (e.g., col, trim, upper, when, from_unixtime, window, count, sum).
# MAGIC Window functions: Used for aggregations over specific partitions (e.g., row_number() for deduplication, window() for time-based aggregations).

# COMMAND ----------

Python

# DLT Pipeline: Customer Order Processing

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define storage locations (adjust these to your actual paths)
# For simplicity, we'll use DBFS for this example.
# In a real scenario, these would likely be external cloud storage paths (s3://, adls://)
RAW_DATA_PATH = "/databricks-datasets/structured-streaming/events/" # Example public dataset
BRONZE_TABLE_PATH = "/FileStore/dlt_pipeline_data/bronze_orders"
SILVER_TABLE_PATH = "/FileStore/dlt_pipeline_data/silver_processed_orders"
GOLD_TABLE_PATH = "/FileStore/dlt_pipeline_data/gold_daily_sales"

# --- Bronze Layer: Ingest Raw Data (Streaming Live Table with Auto Loader) ---
@dlt.table(
    name="bronze_raw_orders",
    comment="Raw customer order events ingested incrementally from JSON files.",
    table_properties={
        "quality": "bronze",
        "pipeline_type": "streaming"
    }
)
@dlt.expect_or_drop("valid_json_format", "id IS NOT NULL") # Basic check for valid JSON structure
def create_bronze_raw_orders():
    # Auto Loader for incremental ingestion
    # .option("cloudFiles.format", "json") specifies the file format
    # .option("cloudFiles.schemaLocation", "<path_to_checkpoint>") is crucial for state management
    # For a real pipeline, ensure schemaLocation is a stable, separate path.
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{BRONZE_TABLE_PATH}/_checkpoints/bronze_schema") # Checkpoint location for schema inference and state
            .load(RAW_DATA_PATH)
    )

# --- Silver Layer: Cleanse and Enrich Data (Streaming Live Table) ---
@dlt.table(
    name="silver_processed_orders",
    comment="Cleaned and enriched customer order data.",
    table_properties={
        "quality": "silver",
        "pipeline_type": "streaming"
    },
    # Define expectations for data quality
    # dlt.expect: Fails the pipeline if expectation is violated
    # dlt.expect_or_drop: Drops records that violate the expectation
    # dlt.expect_or_fail: Fails the pipeline AND logs the violating records (requires DLT v2023.29+)
    # dlt.expect_or_halt: Halts the pipeline if the condition is not met
    # dlt.expect_or_drop: Drops records that violate the expectation
    expect_violations = { # Using expect_violations for more granular control over actions
        "valid_order_id": {"expression": "order_id IS NOT NULL", "action": "drop"},
        "valid_customer_id": {"expression": "customer_id IS NOT NULL", "action": "fail"}, # Fail pipeline if customer_id is null
        "valid_price_positive": {"expression": "price > 0", "action": "drop"},
        "valid_timestamp": {"expression": "event_timestamp IS NOT NULL", "action": "drop"},
        "valid_order_status_category": {"expression": "order_status IN ('PENDING', 'COMPLETED', 'CANCELLED')", "action": "quarantine"} # Requires Quarantined Records feature
    }
)
def create_silver_processed_orders():
    return (
        dlt.read_stream("bronze_raw_orders") # Read incrementally from the bronze table
            .select(
                col("id").alias("order_id").cast(StringType()),
                col("device_id").cast(StringType()),
                col("ecommerce_id").alias("customer_id").cast(StringType()), # Renaming for clarity
                col("amount").alias("price").cast(DoubleType()), # Renaming and casting
                from_unixtime(col("event_timestamp")).alias("event_timestamp").cast(TimestampType()), # Convert Unix timestamp
                col("status").alias("raw_status"), # Keep raw status for reference
                # Example of a generated column (derived from raw_status)
                when(col("raw_status") == "success", "COMPLETED")
                .when(col("raw_status") == "failure", "CANCELLED")
                .otherwise("PENDING").alias("order_status")
            )
            .where(col("order_id").isNotNull()) # Additional filter for robustness
    )


# --- Gold Layer: Aggregated Data for Reporting (Materialized Live Table) ---
@dlt.table(
    name="gold_daily_sales_summary",
    comment="Daily sales summary aggregated from processed orders.",
    table_properties={
        "quality": "gold",
        "pipeline_type": "batch" # This table will be refreshed in batch, not streaming
    }
)
def create_gold_daily_sales_summary():
    return (
        dlt.read("silver_processed_orders") # Read as a batch from the silver streaming table
            .groupBy(window(col("event_timestamp"), "1 day").alias("sales_date"))
            .agg(
                count("order_id").alias("total_orders"),
                sum("price").alias("total_revenue"),
                count(when(col("order_status") == "COMPLETED", True)).alias("completed_orders")
            )
            .select(
                col("sales_date").start.cast(DateType()).alias("sale_date_start"), # Extract date part
                col("sales_date").end.cast(DateType()).alias("sale_date_end"),
                col("total_orders"),
                col("total_revenue"),
                col("completed_orders")
            )
    )

# --- Example of a LIVE VIEW (not a table, just a view) ---
@dlt.view(
    name="customer_360_view",
    comment="A live view joining processed orders with customer data (hypothetical)."
)
def create_customer_360_view():
    # In a real scenario, you would join with a customer dimension table
    # For this example, we'll just select from silver_processed_orders
    return dlt.read("silver_processed_orders").select("customer_id", "order_id", "price", "order_status")



# COMMAND ----------


