# 🗺️ AWS eCommerce Analytics — Real-Time Stream Architecture Flow

> ⚠️ Yeh file **actual running values** ke saath hai — jo AWS pe chal raha hai, wohi yahan dikh raha hai.
> Region: us-east-1 (N. Virginia) | Account: 989864147584

---

## 🌐 POORA SYSTEM EK NAZAR ME (FLINK + DIRECT AWS GLUE ARCHITECTURE)

```
                    ┌─────────────────────────────────┐
                    │   APPLICATION (Local Python)     │
                    │   VS Code — run_kinesis.py       │
                    └──────────────┬──────────────────┘
                                   │
                                   ▼
                    ┌───────────────────────┐
                    │  Kinesis Data Stream  │
                    │  ecomm-events-stream  │
                    │  (ACTIVE ✅)          │
                    └──────────┬────────────┘
                               │
                               ▼
                    ┌───────────────────────┐
                    │   Apache Flink        │
                    │   ecomm-ddos-detector │
                    │   (RUNNING ✅)        │
                    └──────────┬────────────┘
                               │
       ┌───────────────────────┴───────────────────────┐
       │ (Real-Time Anomaly Branch)                    │ (Direct Glue Catalog & S3 Sink Branch)
       ▼                                               ▼
┌───────────────┐                             ┌─────────────────┐
│ AWS Lambda    │                             │    AWS Glue     │
│ ecomm-alert-  │                             │  Data Catalog   │
│ processor     │                             │  ecomm_flink_db │
└───────┬───────┘                             └────────┬────────┘
        │                                              │
  ┌─────┼─────────┐                                    ▼
  ▼     ▼         ▼                           ┌─────────────────┐
DynamoDB CW      SNS                          │  Amazon Athena  │
  ✅    ✅       ⚠️                           │  & QuickSight   │
                                              └─────────────────┘
```

---

# 🔵 STREAMING FLOW — STEP BY STEP EXPLANATION

---

## 🖥️ COMPONENT 1: Application (Clickstream Generator)

```
TYPE  : Python Script / Jupyter Notebook
FILE  : run_kinesis.py / simulate_kinesis_stream.ipynb
```

**Kya karta hai:**
- `2026-Jun-sample.csv` dataset se clickstream records padhta hai.
- Records ko JSON payload mein format karke `ecomm-events-stream` Kinesis Stream mein push karta hai.
- Normal user sessions aur DDoS/Bot user bursts simulate karta hai (>5 events/minute).

---

## 📡 COMPONENT 2: Amazon Kinesis Data Stream — ecomm-events-stream

```
NAME    : ecomm-events-stream
STATUS  : ✅ ACTIVE
MODE    : ON_DEMAND (auto-scaling)
SHARDS  : 4
ARN     : arn:aws:kinesis:us-east-1:989864147584:stream/ecomm-events-stream
```

**Kya karta hai:**
- Real-time event ingestion point hai.
- Sub-second latency ke sath multi-shard streaming buffer provide karta hai.

---

## ⚡ COMPONENT 3: Apache Flink — ecomm-ddos-detector (Core Real-time Engine)

```
NAME    : ecomm-ddos-detector
SERVICE : Managed Service for Apache Flink (Kinesis Data Analytics)
STATUS  : ✅ RUNNING
ENGINE  : Apache Flink 1.15 / Flink SQL (Zeppelin)
```

**Flink ke 2 Main Roles Hain:**

1. **Anomaly Detection Branch (To Lambda):**
   - 1-minute tumbling window calculate karta hai.
   - Per-user event count calculate karke agar `COUNT(*) > 5` ho toh use bot mark karta hai aur alert `ecomm-alerts-stream` par bhejta hai.

2. **Direct AWS Glue & S3 Integration Branch:**
   - Flink **AWS Glue Data Catalog** ko as Metastore use karta hai.
   - Glue Catalog me registered schema ke dwara stream output direct S3 Data Lake Parquet format me sinks likh sakta hai.

---

## 🚨 COMPONENT 4: Anomaly Alerting & Persistence (Lambda + DynamoDB + SNS)

```
LAMBDA  : ecomm-alert-processor (Active ✅)
DYNAMODB: ecomm-suspicious-users (Active ✅)
SNS     : ecomm-ddos-alerts (Email Alerting ⚠️)
```

**Kya karta hai:**
- Lambda `ecomm-alerts-stream` se alert events pull karta hai.
- Bot users (`bot_user_001`, `bot_user_002` etc.) ko DynamoDB `ecomm-suspicious-users` me log karta hai.
- CloudWatch metrics publish karta hai aur SNS Topic dwara email alert trigger karta hai.

---

## 📊 COMPONENT 5: AWS Glue & Ad-hoc Analytics (Glue + Athena + QuickSight)

```
CATALOG : AWS Glue Data Catalog (ecomm_flink_db)
ANALYTICS: Amazon Athena (Ad-hoc SQL) & QuickSight (Dashboards)
```

**Direct Connection Flow:**
1. Flink stream ka output **AWS Glue Data Catalog** me automatically schema-mapped hota hai.
2. **AWS Glue Crawler** (`ecomm-crawler`) S3 structured stream sink ko scan karke Athena Tables metadata maintain rakhta hai.
3. Data Analysts **Amazon Athena** ya **QuickSight** se direct real-time analytics queries execute kar sakte hain.

---

## 🛠️ Summary Commands

```powershell
# 1. Run Real-Time Stream Generator:
python run_kinesis.py

# 2. DynamoDB Flagged Bot Users Check Karein:
aws dynamodb scan --table-name ecomm-suspicious-users --region us-east-1 --output table

# 3. Glue Catalog Tables Check Karein:
aws glue get-tables --database-name ecomm_db --region us-east-1
```
