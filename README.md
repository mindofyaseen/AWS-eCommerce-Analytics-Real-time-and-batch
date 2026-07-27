# AWS eCommerce Analytics Platform: Real-Time & Batch Pipelines

An end-to-end data engineering platform built on AWS that ingests, processes, and analyzes high-throughput eCommerce clickstream events. The system implements a **dual-pipeline architecture**:

1. **Real-Time DDoS & Bot Detection Pipeline:** Ingests live streaming events via Kinesis Data Streams, computes 1-minute tumbling window aggregations using Managed Apache Flink, and triggers serverless alerts via AWS Lambda, DynamoDB, CloudWatch, and Amazon SNS.
2. **Batch Historical Analytics Pipeline:** Ingests streaming data via Amazon Data Firehose into an S3 Data Lake, transforms raw JSON/CSV data into partitioned Snappy-compressed Parquet format using PySpark on AWS Glue, and exposes SQL views via Amazon Athena for AWS QuickSight BI dashboards.

---

## 🏛 Architecture Diagram

![Architecture Diagram](Architecture_Diagram.png)

---

## 🛠 Tech Stack & AWS Services

* **Data Ingestion:** Amazon Kinesis Data Streams, Amazon Data Firehose
* **Stream Processing:** Managed Apache Flink (Kinesis Data Analytics Studio / Apache Zeppelin)
* **Serverless Compute:** AWS Lambda (Python 3.12)
* **Data Lake & Storage:** Amazon S3, Amazon DynamoDB (Pay-Per-Request)
* **Batch Processing & Catalog:** AWS Glue (PySpark ETL Jobs, Glue Data Catalog, Crawlers)
* **Query Engine & BI:** Amazon Athena, AWS QuickSight
* **Monitoring & Alerts:** Amazon CloudWatch Dashboards, Amazon SNS (Email Notifications)
* **Programming & SDKs:** Python 3.12, PySpark, Boto3, AWS CLI v2

---

## 📂 Repository Structure

```text
├── 2026-Jun-sample.csv          # Synthetic eCommerce clickstream dataset (~100k events)
├── Architecture_Diagram.png     # Architectural flow diagram
├── simulate_kinesis_stream.ipynb# Real-time event generator for Kinesis Data Stream
├── simulate_stream.ipynb        # Batch event generator for Amazon Data Firehose
├── etl_to_parquet.py            # AWS Glue PySpark ETL job (Raw JSON -> Partitioned Parquet)
├── lambda_alert_processor.py    # AWS Lambda function for real-time anomaly processing
├── create_quicksight_datasets.py# Automated QuickSight dataset creation script via Boto3
├── dashboard.json               # CloudWatch DDoS monitoring dashboard configuration
├── glue-trust.json              # IAM trust policy for Glue ETL & Crawler roles
├── lambda-trust.json            # IAM trust policy for Lambda Alert Processor role
├── qs-trust.json                # IAM trust policy for QuickSight service role
└── qs-policy.json               # IAM inline policy for QuickSight Athena + S3 access
```

---

## ⚡ Pipelines Deep Dive

### 1. Real-Time DDoS & Bot Anomaly Detection
* **Ingestion:** Clickstream events are pushed to `ecomm-events-stream` (On-Demand capacity mode).
* **Stream Analytics (Apache Flink):** Flink evaluates a 1-minute tumbling window:
  ```sql
  SELECT user_id, COUNT(*) AS event_count,
         TUMBLE_START(event_arrival_time, INTERVAL '1' MINUTE) AS window_start,
         TUMBLE_END(event_arrival_time, INTERVAL '1' MINUTE) AS window_end
  FROM ecomm_events
  GROUP BY user_id, TUMBLE(event_arrival_time, INTERVAL '1' MINUTE)
  HAVING COUNT(*) > 5
  ```
* **Alert Routing & Action:** Detected anomalies are routed to `ecomm-alerts-stream`, triggering the `ecomm-alert-processor` Lambda function to:
  - Write flagged user records into DynamoDB (`ecomm-suspicious-users`).
  - Publish custom metrics (`eComm/DDoS:SuspiciousUserEvents`) to CloudWatch.
  - Dispatch email notifications via Amazon SNS.

### 2. Batch Historical Analytics
* **Delivery:** Data Firehose buffers incoming records (5 MB / 60 seconds) with GZIP compression into `s3://ecomm-analytics-yaseen-2026/raw-stream/`.
* **PySpark Transformation:** AWS Glue job `ecomm-etl-to-parquet` parses ISO timestamps, filters valid event types (`view`, `cart`, `purchase`, `remove_from_cart`), and writes Snappy-compressed Parquet files partitioned by `event_type`.
* **Athena SQL Views:**
  - `v_unique_visitors_daily`: Daily distinct user aggregation.
  - `v_cart_abandonment`: Session-level cart abandonment calculation (~85.4% baseline).
  - `v_top_categories_hourly`: Hourly category event breakdown.
  - `v_brand_insights`: Brand sales performance and average order price.

---

## 🚀 Live Demo & Execution Guide

### Prerequisites
* AWS CLI v2 configured with valid credentials (`us-east-1`).
* Python 3.12+ with `boto3` installed.

### Step 1: Start Ephemeral Resources
1. In AWS Console, create On-Demand Kinesis streams `ecomm-events-stream` and `ecomm-alerts-stream`.
2. Ensure Lambda `ecomm-alert-processor` event source trigger is Enabled.
3. Start Managed Flink Studio notebook `ecomm-ddos-detector` and run the Flink SQL cells in Zeppelin.

### Step 2: Stream Live Events
Run the Jupyter notebook `simulate_kinesis_stream.ipynb` locally to push synthetic traffic.

### Step 3: Verify Live Outputs
* **DynamoDB:** Check table `ecomm-suspicious-users` for real-time flagged users.
* **CloudWatch:** View dashboard `ecomm-ddos-monitor` for metric spikes.
* **SNS:** Check email inbox for instant DDoS attack notifications.

---

## 💰 Cost Optimization & Teardown

To avoid unnecessary AWS charges between work sessions:
1. **Stop Flink Studio:** Stop `ecomm-ddos-detector` in Kinesis Console.
2. **Delete Ephemeral Streams:** Delete `ecomm-events-stream` and `ecomm-alerts-stream`.

All persistent assets (S3 data, Glue database, Lambda function, DynamoDB table, SNS topic, Athena views) operate under AWS Free Tier / Pay-Per-Request models with **$0 idle cost**.

---
*Maintained by Muhammad Yaseen | Data Engineer*
