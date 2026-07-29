# 🚀 Guide: Flink + Direct AWS Glue Real-Time Architecture (AWS UI & Python/CLI)

Is guide mein detailed step-by-step instructions hain taake aap is pure **Real-Time Stream & Direct AWS Glue Integration** Project ko **AWS Web Console (UI)** ya **Python/CLI** dono tarikon se start, run, verify aur stop kar sakein.

---

## 🏗 Architecture Diagram (Pure Streaming + Direct Glue)

```
                       ┌──────────────────────────────┐
                       │   APPLICATION (Local Python)  │
                       │   VS Code — run_kinesis.py   │
                       └──────────────┬───────────────┘
                                      │
                                      ▼
                       ┌──────────────────────────────┐
                       │    Kinesis Data Stream       │
                       │    ecomm-events-stream       │
                       └──────────────┬───────────────┘
                                      │
                                      ▼
                       ┌──────────────────────────────┐
                       │     Managed Apache Flink     │
                       │     ecomm-ddos-detector      │
                       └──────┬────────────────┬──────┘
                              │                │
       (Real-Time Anomaly)   │                │ (Direct Glue Catalog & S3 Sink)
                              ▼                ▼
                      ┌──────────────┐   ┌──────────────┐
                      │ Lambda Alert │   │   AWS Glue   │
                      │  Processor   │   │ Data Catalog │
                      └──────┬───────┘   └──────┬───────┘
                             │                  │
                  ┌──────────┼──────────┐       ▼
                  ▼          ▼          ▼  ┌────────────┐
              DynamoDB   CloudWatch    SNS │ QuickSight │
               Table      Metrics    Email │ & Athena   │
                                           └────────────┘
```

---

# 🔷 METHOD 1: Python & AWS CLI Se Run Karna (Fast & Automated)

### **Step 1: Prerequisites Check & Flink Service Start**
PowerShell / Terminal khol kar check karein ke Kinesis Stream active hai aur Flink app running hai:

```powershell
# 1. Kinesis Stream Status Check:
aws kinesis describe-stream-summary --stream-name ecomm-events-stream --region us-east-1 --query "StreamDescriptionSummary.StreamStatus" --output text
aws kinesis describe-stream-summary --stream-name ecomm-alerts-stream --region us-east-1 --query "StreamDescriptionSummary.StreamStatus" --output text

# 2. Flink Application Start Status:
aws kinesisanalyticsv2 describe-application --application-name ecomm-ddos-detector --region us-east-1 --query "ApplicationDetail.ApplicationStatus" --output text

# Agar Flink app STOPPED hai, toh START karein:
aws kinesisanalyticsv2 start-application --application-name ecomm-ddos-detector --region us-east-1
```

---

### **Step 2: Real-Time Stream Generator Run Karein**

```powershell
python run_kinesis.py
```
* **Kya hota hai:** `run_kinesis.py` script normal clickstream events + DDoS bot attack pattern (`bot_attacker_001`, `002`, `003`) Kinesis Stream `ecomm-events-stream` par ingest karta hai.
* **Flink Aggregation:** Flink 1-minute tumbling window mein attack traffic detect karke alert output `ecomm-alerts-stream` par push karta hai.
* **Lambda Trigger:** Lambda alerts ko pick karke DynamoDB `ecomm-suspicious-users` table mein insert karta hai aur SNS Email bhejta hai.

---

### **Step 3: Verification Commands**

```powershell
# 1. DynamoDB Flagged Bot Users Table View Karein:
aws dynamodb scan --table-name ecomm-suspicious-users --region us-east-1 --output table

# 2. AWS Glue Catalog Database & Tables Check Karein:
aws glue get-tables --database-name ecomm_db --region us-east-1 --query "TableList[*].Name"

# 3. Lambda Execution Logs Traces Check Karein:
aws logs tail /aws/lambda/ecomm-alert-processor --region us-east-1 --since 10m
```

---

# 🔶 METHOD 2: AWS Web Console (UI) Se Step-by-Step Run & Inspect Karna

Aap AWS Management Console (Browser) mein har service ko real-time data flow ke mutabiq step-by-step navigate karke observe kar sakte hain.

---

### **Step 0: Stream Generator Start Karein**
Terminal mein ye command chalaayein:
```powershell
python run_kinesis.py
```

---

### **Step-by-Step AWS UI Data Flow Navigation:**

#### **1. Step 1: Kinesis Ingestion Stream (UI)**
1. AWS Console search bar mein search karein **Kinesis**.
2. Left Menu se **Data streams** -> Select `ecomm-events-stream`.
3. **Data viewer** tab kholain -> Shard select karein -> **Get records** click karein.
4. Live incoming clickstream events payload screen par dekhein.

---

#### **2. Step 2: Apache Flink & AWS Glue Catalog Integration (UI)**
1. Kinesis Console me Left Menu se **Analytics applications** -> **Studio notebooks** -> Select `ecomm-ddos-detector`.
2. Status **RUNNING** hone par **Open in Apache Zeppelin** button dabaen:
   - Flink **AWS Glue Data Catalog (`ecomm_db`)** se directly connected hai.
   - Notebook cells execution se Flink tumbling window anomalies calculate karke output alert stream aur Glue Cataloged tables me map karta hai.

---

#### **3. Step 3: Kinesis Alerts Stream (UI)**
1. Kinesis Console me **Data streams** -> `ecomm-alerts-stream` select karein.
2. **Data viewer** tab me **Get records** click karke Flink anomaly alert records (`user_id`, `event_count`, `window_start`) dekhein.

---

#### **4. Step 4: AWS Lambda & CloudWatch Traces (UI)**
1. AWS Console Search Bar mein search karein **Lambda** -> Function `ecomm-alert-processor`.
2. **Monitor** tab -> **View CloudWatch logs** click karke live bot attack log traces trace karein: `[ALERT] Bot behavior detected for user: bot_attacker_001`.

---

#### **5. Step 5: DynamoDB Storage (UI)**
1. AWS Console mein **DynamoDB** -> **Explore items** par click karein.
2. Table select karein: `ecomm-suspicious-users`.
3. Attacker bot user records (`bot_user_001`, `bot_user_002`, `bot_user_003`) live updated list me dekhein.

---

#### **6. Step 6: SNS Real-time Email Alert (UI & Inbox)**
1. Console me **Simple Notification Service (SNS)** open karke Topic `ecomm-ddos-alerts` check karein.
2. Email inbox (`mindofyaseen@gmail.com`) check karke real-time bot alert email notification verify karein.

---

#### **7. Step 7: AWS Glue Catalog & Athena Analytics (UI)**
1. AWS Console me **AWS Glue** service open karein -> **Data Catalog** -> **Databases** -> Select `ecomm_flink_db`.
2. **Amazon Athena** search & open karein:
   - Database dropdown se: **`ecomm_flink_db`** select karein.
   - (Pehli dafa run kar rahe hon toh Settings me S3 Result Location set karein: `s3://ecomm-analytics-yaseen-2026/athena-results/`).
   - Query Editor me query run karein:
     ```sql
     SELECT user_id, event_type, price, category_code 
     FROM ecomm_flink_db.raw_stream 
     LIMIT 20;
     ```
   - SQL Analytics results screen par verify karein!

---

# 🛑 Cost Saving & Cleanup Guide (Kaam Khatam Hone Par Stop Karein)

```powershell
# Flink App Stop Karein (Hourly compute billing pause karne ke liye):
aws kinesisanalyticsv2 stop-application --application-name ecomm-ddos-detector --region us-east-1
```
