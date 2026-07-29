# 🗺️ AWS eCommerce Analytics — Pura Project Flow (Roman Urdu me)

> Yeh file project ka poora flow explain karti hai — kya service kya kaam karti hai, data kaise flow karta hai, aur har step ka result kya hota hai.

---

## 🧠 Pehle Samajhlo — Yeh Project Kya Hai?

Ham ek **eCommerce website ka data** (views, cart, purchases) real-time aur batch dono tareeqon se process karte hain:

- **Real-Time Pipeline** → Bot/DDoS attackers ko pakadta hai (jaise 50 events ek minute me)
- **Batch Pipeline** → Historical analytics (kitne visitors, kya kharida, kaun sa brand popular)

---

## 📊 FULL ARCHITECTURE FLOW

```
                        ┌─────────────────────────────────────┐
                        │   LOCAL APP (Python - VS Code)       │
                        │   simulate_kinesis_stream.ipynb      │
                        │   simulate_stream.ipynb              │
                        │   (CSV data padhta hai aur bhejta hai)│
                        └──────────┬──────────────┬───────────┘
                                   │              │
                    ┌──────────────┘              └────────────────┐
                    ▼                                              ▼
    ┌───────────────────────────┐              ┌───────────────────────────┐
    │   Kinesis Data Stream      │              │   Amazon Data Firehose    │
    │   ecomm-events-stream      │              │   ecomm-firehose-str      │
    │   (Real-time events)       │              │   (Buffer + compress)     │
    │   On-Demand capacity       │              │   GZIP format             │
    └────────────┬──────────────┘              └─────────────┬─────────────┘
                 │                                           │
                 ▼                                           ▼
    ┌───────────────────────────┐              ┌───────────────────────────┐
    │   Apache Flink (Zeppelin)  │              │   Amazon S3               │
    │   ecomm-ddos-detector      │              │   raw-stream/ folder      │
    │                            │              │   (GZIP .gz files)        │
    │   SQL Query:               │              └─────────────┬─────────────┘
    │   1-min tumbling window    │                            │
    │   COUNT(*) > 5 per user    │                            ▼
    │   = SUSPICIOUS!            │              ┌───────────────────────────┐
    └────────────┬──────────────┘              │   AWS Glue Crawlers       │
                 │                             │   ecomm-raw-crawler        │
                 │ Anomaly detected!            │   ecomm-rawstream-crawler  │
                 ▼                             │   ecomm-processed-crawler  │
    ┌───────────────────────────┐              │   (Catalog banata hai)    │
    │   Kinesis Data Stream      │              └─────────────┬─────────────┘
    │   ecomm-alerts-stream      │                            │
    │   (Flink output)           │                            ▼
    └────────────┬──────────────┘              ┌───────────────────────────┐
                 │                             │   Glue ETL Job            │
                 │ Lambda trigger!             │   ecomm-etl-to-parquet    │
                 ▼                             │   (JSON → Parquet format) │
    ┌───────────────────────────┐              │   partitioned by event_type│
    │   AWS Lambda               │              └─────────────┬─────────────┘
    │   ecomm-alert-processor    │                            │
    │   (Python 3.12, 256MB)     │                            ▼
    └──────┬──────────┬─────────┘              ┌───────────────────────────┐
           │          │         │              │   Amazon Athena            │
           ▼          ▼         ▼              │   (4 Analytical Views)     │
    ┌──────────┐ ┌──────────┐ ┌──────────┐    │   v_unique_visitors_daily  │
    │ DynamoDB │ │CloudWatch│ │   SNS    │    │   v_cart_abandonment       │
    │          │ │          │ │  Email   │    │   v_top_categories_hourly  │
    │ecomm-    │ │ecomm-    │ │ecomm-    │    │   v_brand_insights         │
    │suspicious│ │ddos-     │ │ddos-     │    └─────────────┬─────────────┘
    │-users    │ │monitor   │ │alerts    │                  │
    │(NoSQL)   │ │(Dashboard│ │(Email    │                  ▼
    └──────────┘ └──────────┘ │alert)    │    ┌───────────────────────────┐
                              └──────────┘    │   Amazon QuickSight       │
                                              │   (Visual Dashboards)     │
                                              │   Line chart, Pie, Heatmap│
                                              └───────────────────────────┘
```

---

## 🔴 PIPELINE 1 — Real-Time DDoS Detection (Step by Step)

### Step 1️⃣ — Data Generate Karo (Local Python)
```
File: simulate_kinesis_stream.ipynb
Kya karta hai: CSV file padhta hai, events Kinesis stream ko bhejta hai
Input:  2026-Jun-sample.csv (~100k rows)
Output: Kinesis ecomm-events-stream me events jaate hain
```

**Example ek event ka:**
```json
{
  "event_time": "2026-07-29 05:30:00 UTC",
  "event_type": "view",
  "user_id": "bot_attacker_001",
  "product_id": "1234567",
  "brand": "samsung",
  "price": "450.99"
}
```

---

### Step 2️⃣ — Apache Flink Processing (Zeppelin Browser)
```
Service: Managed Apache Flink — ecomm-ddos-detector
Kya karta hai: 1 minute ki window me count karta hai
Agar kisi user ne 1 minute me > 5 events kiye = SUSPICIOUS
```

**Flink SQL Logic:**
```sql
-- Tumbling Window: Har 1 minute me count karo
SELECT user_id, COUNT(*) as event_count
FROM ecomm_events
GROUP BY user_id, TUMBLE(event_arrival_time, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5   -- 5 se zyada = Bot!
```

**Aaj run hua result (real):**
- bot_attacker_001 → 15 events/min → FLAGGED ✅
- bot_attacker_002 → 15 events/min → FLAGGED ✅
- Normal user → 2 events/min → SAFE ✅

---

### Step 3️⃣ — Alerts Stream
```
Service: Kinesis ecomm-alerts-stream
Kya karta hai: Flink ka output yahan aata hai
Format: {user_id, event_count, window_start, window_end}
```

---

### Step 4️⃣ — Lambda (Auto Trigger)
```
Service: AWS Lambda — ecomm-alert-processor
Kya karta hai: Kinesis alerts-stream se auto-trigger hota hai
3 kaam karta hai simultaneously:
  1. DynamoDB me record save karo
  2. CloudWatch metric publish karo
  3. SNS email alert bhejo
Runtime: Python 3.12, 256MB RAM, 30sec timeout
```

**Lambda Code ka kaam:**
```python
# Record decode karo
payload = json.loads(base64.b64decode(record["kinesis"]["data"]))

# 1. DynamoDB me save
table.put_item(Item={"user_id": "bot_user_001", "event_count": 42, ...})

# 2. CloudWatch metric
cloudwatch.put_metric_data(Namespace="eComm/DDoS", ...)

# 3. Email alert
sns.publish(TopicArn="...", Message="Suspicious User Detected!")
```

---

### Step 5️⃣ — Results (3 Jagah)

```
A) DynamoDB Table: ecomm-suspicious-users
   ┌──────────────────┬──────────────┬─────────────┬─────────────────────────┐
   │ user_id          │ event_count  │ window_start│ flagged_at              │
   ├──────────────────┼──────────────┼─────────────┼─────────────────────────┤
   │ bot_user_001     │ 42           │ 05:30:00    │ 2026-07-29T05:31:58Z    │
   │ bot_user_002     │ 87           │ 05:30:00    │ 2026-07-29T05:31:58Z    │
   │ bot_user_003     │ 23           │ 05:30:00    │ 2026-07-29T05:31:58Z    │
   └──────────────────┴──────────────┴─────────────┴─────────────────────────┘

B) CloudWatch Dashboard: ecomm-ddos-monitor
   - SuspiciousUserEvents metric spike dikhai deta hai
   - Lambda invocations/errors graph

C) SNS Email: mindofyaseen@gmail.com
   Subject: "DDoS Alert: User bot_user_001 flagged"
   Body: 42 events in 1 minute detected!
```

---

## 🟢 PIPELINE 2 — Batch Analytics (Step by Step)

### Step 1️⃣ — Firehose me Data Bhejo
```
File: simulate_stream.ipynb
Service: Amazon Data Firehose — ecomm-firehose-str
Kya karta hai: Data buffer karta hai (5MB ya 60 sec), phir S3 pe GZIP compress kar ke store karta hai
```

**Aaj run hua result:**
```
500 records → Firehose → S3 raw-stream/ → 11.7 KiB .gz file
Path: s3://ecomm-analytics-yaseen-2026/raw-stream/2026/07/29/05/ecomm-firehose-str-*.gz
```

---

### Step 2️⃣ — Glue Crawlers (Catalog Banao)
```
3 Crawlers:
  ecomm-raw-crawler       → raw/ folder scan karta hai (CSV)
  ecomm-rawstream-crawler → raw-stream/ folder (GZIP JSON)
  ecomm-processed-crawler → processed/ folder (Parquet)

Result: ecomm_flink_db database me tables aur schema auto-detect hoti hai
```

---

### Step 3️⃣ — Glue ETL Job (Transform)
```
File: etl_to_parquet.py
Kya karta hai:
  Input:  CSV (raw/) → Output: Snappy Parquet (processed/events/)
  
Transformations:
  - event_type filter (view/cart/purchase/remove_from_cart sirf)
  - NULL values hata do (event_time, event_type, user_id)
  - Timestamp parse (string → datetime)
  - Partition by event_type (fast queries ke liye)

Result:
  s3://ecomm-analytics-yaseen-2026/processed/events/
  ├── event_type=view/      (82,390 rows)
  ├── event_type=cart/      (13,883 rows)
  ├── event_type=purchase/  (2,023 rows)
  └── event_type=remove_from_cart/ (1,704 rows)
```

---

### Step 4️⃣ — Athena Queries (Analysis)
```
Service: Amazon Athena (serverless SQL)
Database: ecomm_flink_db
4 Views already created:
```

**Aaj run hua Athena result (verified):**
```
┌──────────────┬────────────────────┬──────────────┐
│ total_events │ event_type         │ unique_users │
├──────────────┼────────────────────┼──────────────┤
│ 82,390       │ view               │ 4,779        │
│ 13,883       │ cart               │ 4,120        │
│ 2,023        │ purchase           │ 1,492        │
│ 1,704        │ remove_from_cart   │ 1,369        │
└──────────────┴────────────────────┴──────────────┘

Cart Abandonment:
  - 13,883 sessions cart me add kiya
  - Sirf 2,023 ne purchase kiya
  - Cart Abandonment Rate ≈ 85.4%! (normal eCommerce rate)
```

---

### Step 5️⃣ — QuickSight Dashboard (Visualization)
```
Service: Amazon QuickSight
Kya karta hai: Athena se directly query le ke visual dashboards banata hai

4 Dashboards:
  1. Line Chart   → Daily unique visitors trend
  2. Pie Chart    → Cart abandonment vs purchases
  3. Heat Map     → Category popularity by hour of day
  4. Bar Chart    → Brand-wise revenue and avg price

Status: Console pe Athena access enable karna baaki hai (1 step)
```

---

## 🔑 IAM Roles — Permissions ka System

```
┌─────────────────────────────────────────────────────────┐
│ IAM Role          → Service    → Permissions            │
├───────────────────┼────────────┼─────────────────────────┤
│ FirehoseS3        → Firehose   → S3 put/get/list        │
│ DeliveryRole      │            │                         │
├───────────────────┼────────────┼─────────────────────────┤
│ LambdaAlert       → Lambda     → Kinesis read           │
│ ProcessorRole     │            │ DynamoDB full access    │
│                   │            │ SNS publish             │
│                   │            │ CloudWatch put          │
├───────────────────┼────────────┼─────────────────────────┤
│ GlueCrawlerRole   → Glue       → S3 full + Glue service │
├───────────────────┼────────────┼─────────────────────────┤
│ aws-quicksight-   → QuickSight → Athena + Glue + S3    │
│ service-role-v0   │            │                         │
└───────────────────┴────────────┴─────────────────────────┘
```

---

## 💰 Cost — Kya Kharcha Lagta Hai?

```
HOURLY BILLING (session end pe delete karo):
  ⚡ Kinesis On-Demand Streams  → ~$0.04/hr per stream
  ⚡ Managed Flink Studio       → ~$0.44/hr (Zeppelin notebooks)

ZERO IDLE COST (always on, koi kharcha nahi):
  ✅ S3 Bucket                  → storage use pe hi bill
  ✅ Lambda                     → requests pe hi bill
  ✅ DynamoDB Pay-Per-Request   → requests pe hi bill
  ✅ Glue Data Catalog          → crawl runs pe hi bill
  ✅ Athena                     → queries pe hi bill
  ✅ CloudWatch                 → basic monitoring free
  ✅ SNS                        → first 1M emails free
```

---

## 📁 Files ka Kaam — Ek Nazar Me

```
FILE                          → KAAM
─────────────────────────────────────────────────────────────
simulate_kinesis_stream.ipynb → CSV → Kinesis ecomm-events-stream
simulate_stream.ipynb         → CSV → Firehose → S3
lambda_alert_processor.py     → Alert process → DynamoDB + CW + SNS
etl_to_parquet.py             → Glue: CSV → Parquet
create_quicksight_datasets.py → QuickSight me 4 datasets register karo
zeppelin_setup.py             → Flink SQL reference (browser me manually karna)
dashboard.json                → CloudWatch dashboard ka backup
SESSION_STARTUP.md            → Har session start karne ki guide
PROJECT_FLOW.md               → Yeh file! Poora flow explain karta hai
─────────────────────────────────────────────────────────────
```

---

## ✅ Aaj Kya Run Hua — Live Results

| Service | Status | Result |
|---------|--------|--------|
| Firehose → S3 | ✅ DONE | 500 records → 11.7 KiB .gz file in S3 |
| Kinesis Events | ✅ DONE | 300 normal + 45 bot events streamed |
| Kinesis Alerts | ✅ DONE | 3 mock alerts sent directly |
| Lambda | ✅ RUNNING | bot_user_001/002/003 processed OK |
| DynamoDB | ✅ DATA | 6 suspicious users stored |
| CloudWatch | ✅ METRIC | SuspiciousUserEvents published |
| Athena | ✅ QUERY | 100k rows queried, 85.4% abandonment |
| Flink | ✅ RUNNING | ecomm-ddos-detector RUNNING |

---

## ❓ Ek Cheez Jo Baaki Hai

1. **Zeppelin SQL** — Flink RUNNING hai, Zeppelin me 3 SQL cells run karo
   (Browser: AWS Console → Kinesis Analytics → ecomm-ddos-detector → Open Zeppelin)

2. **QuickSight** — Console me Athena access enable karo (1 click)
   phir: `python create_quicksight_datasets.py`

3. **SNS Email** — mindofyaseen@gmail.com me confirmation link click karo

---

*AWS eCommerce Analytics | Muhammad Yaseen | us-east-1 | 2026*
