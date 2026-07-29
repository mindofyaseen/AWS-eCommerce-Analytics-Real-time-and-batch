# 🗺️ AWS eCommerce Analytics — Har Component Ki Detail (Live Running State)

> ⚠️ Yeh file **actual running values** ke saath hai — jo abhi AWS pe chal raha hai, wohi yahan dikh raha hai.
> Region: us-east-1 (N. Virginia) | Account: 989864147584

---

## 🌐 POORA SYSTEM EK NAZAR ME (NEW ARCHITECTURE)

```
                    ┌─────────────────────────────────┐
                    │   APPLICATION (Local Python)     │
                    │   VS Code — simulate_kinesis_    │
                    │   stream.ipynb                   │
                    └──────────────┬──────────────────┘
                                   │
                    ┌──────────────┼──────────────────┐
                    │ (main flow)  │     (raw data)    │
                    ▼              │                   ▼
    ┌───────────────────────┐      │    ┌───────────────────────┐
    │  Kinesis Data Streams │      │    │  Amazon Data Firehose │
    │  ecomm-events-stream  │      │    │  ecomm-firehose-str   │
    │  (ACTIVE ✅)          │      │    │  (ACTIVE ✅)          │
    └──────────┬────────────┘      │    └──────────┬────────────┘
               │                   │               │
               ▼                   │               ▼
    ┌───────────────────────┐      │    ┌───────────────────────┐
    │   Apache Flink        │      │    │   Amazon S3           │
    │   ecomm-ddos-detector │      │    │   raw-stream/ folder  │
    │   (RUNNING ✅)        │      │    │   (GZIP .gz files)    │
    │                       │      │    └───────────────────────┘
    │  1-min tumbling window│      │
    │  COUNT(*) > 5 = bot!  │      │
    └──────────┬────────────┘      │
               │                   │
      ┌────────┴────────┐          │
      │                 │          │
      ▼                 ▼          │
┌──────────┐    ┌───────────────┐  │
│  Lambda  │    │   AWS Glue    │  │
│ ecomm-   │    │ ecomm_flink_db│  │
│ alert-   │    │ ETL + Catalog │  │
│ processor│    │               │  │
│(Active ✅│    │  (Active ✅)  │  │
└────┬─────┘    └──────┬────────┘  │
     │                 │           │
  ┌──┼──────┐          ▼           │
  ▼  ▼      ▼   ┌────────────┐    │
 DB  CW    SNS  │  QuickSight│    │
DynDB CWatch Email│  Dashboards│   │
 ✅   ✅    ⚠️  │  (Pending) │    │
              └────────────┘    │
                                │
 Raw Data Path: ────────────────┘
```

---

# 🔵 MAIN FLOW — APPLICATION TO FLINK

---

## 🖥️ COMPONENT 1: Application (Local Python)

```
TYPE  : Local Script (VS Code me run hoti hai)
FILE  : simulate_kinesis_stream.ipynb
```

**Kya karta hai:**
- `2026-Jun-sample.csv` (~100,000 rows) padhta hai
- Batches of 20 records Kinesis Stream ko bhejta hai
- Bot behavior simulate karta hai (same user = 15 events/min → Flink pakad lega)

**Ek sample event:**
```json
{
  "event_time"    : "2026-07-29 05:30:00 UTC",
  "event_type"    : "view",
  "user_id"       : "bot_attacker_001",
  "product_id"    : "4567890",
  "category_code" : "electronics.smartphone",
  "brand"         : "samsung",
  "price"         : "450.99"
}
```

**Aaj run hua:** 300 normal + 45 bot events → `ecomm-events-stream`

---

## 📡 COMPONENT 2: Amazon Kinesis Data Streams — ecomm-events-stream

```
NAME    : ecomm-events-stream
STATUS  : ✅ ACTIVE
MODE    : ON_DEMAND (auto-scaling)
SHARDS  : 4 (auto-managed)
ARN     : arn:aws:kinesis:us-east-1:989864147584:stream/ecomm-events-stream
```

**Kya karta hai:**
- Application se events receive karta hai
- Real-time buffer karta hai
- Apache Flink is stream ko continuously read karta hai

> ⚠️ Ephemeral: Session end pe delete karo — billing hoti hai

---

## ⚡ COMPONENT 3: Apache Flink — ecomm-ddos-detector

```
NAME    : ecomm-ddos-detector
STATUS  : ✅ RUNNING
RUNTIME : ZEPPELIN-FLINK-3_0
MODE    : Interactive Studio Notebook
ARN     : arn:aws:kinesisanalytics:us-east-1:989864147584:application/ecomm-ddos-detector
```

**Kya karta hai:**
- `ecomm-events-stream` continuously read karta hai
- Har user ke liye 1-minute tumbling window me events count karta hai
- 5 se zyada events = SUSPICIOUS → alert output karta hai
- Flink ke output se **2 branches** nikalte hain: Lambda aur AWS Glue

**Flink SQL (Zeppelin me 3 cells — browser se):**

```sql
-- Cell 1: Source (Kinesis se padhna)
CREATE TABLE ecomm_events (
    event_type VARCHAR, user_id VARCHAR, product_id VARCHAR,
    category_code VARCHAR, brand VARCHAR, price DOUBLE,
    event_arrival_time AS PROCTIME()
)
WITH ('connector'='kinesis','stream'='ecomm-events-stream',
      'aws.region'='us-east-1','scan.stream.initpos'='LATEST','format'='json');

-- Cell 2: Sink (Kinesis ko likhna)
CREATE TABLE ecomm_alerts_sink (
    user_id VARCHAR, event_count BIGINT,
    window_start TIMESTAMP(3), window_end TIMESTAMP(3)
)
WITH ('connector'='kinesis','stream'='ecomm-alerts-stream',
      'aws.region'='us-east-1','format'='json');

-- Cell 3: Anomaly Detection (continuously running)
INSERT INTO ecomm_alerts_sink
SELECT user_id, COUNT(*) AS event_count,
    TUMBLE_START(event_arrival_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_arrival_time, INTERVAL '1' MINUTE)   AS window_end
FROM ecomm_events
GROUP BY user_id, TUMBLE(event_arrival_time, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5;
```

**Aaj detect hua (real):**
```
bot_attacker_001 → 15 events/min → FLAGGED → alerts-stream pe bheja
bot_attacker_002 → 15 events/min → FLAGGED → alerts-stream pe bheja
bot_attacker_003 → 15 events/min → FLAGGED → alerts-stream pe bheja
normal_user      →  2 events/min → SAFE
```

---

# 🔴 FLINK BRANCH 1 — LAMBDA PATH

---

## 📡 COMPONENT 4: Kinesis Alerts Stream — ecomm-alerts-stream

```
NAME    : ecomm-alerts-stream
STATUS  : ✅ ACTIVE
MODE    : ON_DEMAND, 4 shards
ARN     : arn:aws:kinesis:us-east-1:989864147584:stream/ecomm-alerts-stream
```

**Kya karta hai:**
- Flink ka anomaly output yahan aata hai
- Lambda is stream ko listen karti hai (auto-trigger)

**Data format:**
```json
{
  "user_id"      : "bot_attacker_001",
  "event_count"  : 15,
  "window_start" : "2026-07-29 05:30:00",
  "window_end"   : "2026-07-29 05:31:00"
}
```

---

## λ COMPONENT 5: AWS Lambda — ecomm-alert-processor

```
NAME     : ecomm-alert-processor
STATUS   : ✅ Active
RUNTIME  : Python 3.12
MEMORY   : 256 MB
TIMEOUT  : 30 seconds
HANDLER  : lambda_alert_processor.lambda_handler
TRIGGER  : Kinesis ecomm-alerts-stream (BatchSize: 10, State: Enabled)
ROLE     : LambdaAlertProcessorRole
LOG GROUP: /aws/lambda/ecomm-alert-processor
```

**Kya karta hai — 3 kaam simultaneously:**

```python
# 1. DynamoDB me suspicious user store karo
table.put_item(Item={
    "user_id"     : "bot_user_001",
    "window_start": "2026-07-29 05:30:00",
    "event_count" : 42,
    "flagged_at"  : "2026-07-29T05:31:58+00:00"
})

# 2. CloudWatch metric publish karo
cloudwatch.put_metric_data(
    Namespace="eComm/DDoS",
    MetricData=[{"MetricName": "SuspiciousUserEvents",
                 "Value": 42.0,
                 "Timestamp": datetime.now(timezone.utc)}]
)

# 3. SNS email alert bhejo
sns.publish(
    TopicArn="arn:aws:sns:us-east-1:989864147584:ecomm-ddos-alerts",
    Subject ="DDoS Alert: User bot_user_001 flagged",
    Message ="42 events in 1 minute detected!"
)
```

**Aaj ki actual Lambda execution log:**
```
START RequestId: da6265ab-7a3f-434c-a62e-a7710d1ddf99
[OK] Processed user_id=bot_user_003, event_count=23
END RequestId: da6265ab-7a3f-434c-a62e-a7710d1ddf99
Duration: 360.52 ms | Memory Used: 97 MB / 256 MB
```

---

## 🗃️ COMPONENT 6: Amazon DynamoDB — ecomm-suspicious-users

```
TABLE   : ecomm-suspicious-users
STATUS  : ✅ ACTIVE
BILLING : PAY_PER_REQUEST
PK      : user_id (HASH)
SK      : window_start (RANGE)
```

**Abhi table me live data:**
```
┌──────────────────┬─────────────┬──────────────────────────────────┐
│ user_id          │ event_count │ flagged_at                       │
├──────────────────┼─────────────┼──────────────────────────────────┤
│ bot_user_001     │ 42          │ 2026-07-29T05:31:58+00:00        │
│ bot_user_002     │ 87          │ 2026-07-29T05:31:58+00:00        │
│ bot_user_003     │ 23          │ 2026-07-29T05:31:58+00:00        │
│ bot_user_777     │ 18          │ 2026-07-28T11:00:49+00:00        │
│ bot_user_888     │ 25          │ 2026-07-28T11:00:49+00:00        │
│ test_user_999    │ 12          │ 2026-07-28T10:59:52+00:00        │
└──────────────────┴─────────────┴──────────────────────────────────┘
Total Items: 6
```

---

## 📊 COMPONENT 7: Amazon CloudWatch — ecomm-ddos-monitor

```
DASHBOARD : ecomm-ddos-monitor
NAMESPACE : eComm/DDoS
METRIC    : SuspiciousUserEvents
STATUS    : ✅ Active
```

**4 Widgets jo dashboard me hain:**
```
1. SuspiciousUserEvents  → real-time spike graph
2. Lambda Invocations    → kitni baar Lambda chali
3. Kinesis Records       → ecomm-alerts-stream me kitna data
4. Lambda Duration       → execution time (ms)
```

Console: `AWS → CloudWatch → Dashboards → ecomm-ddos-monitor`

---

## 📧 COMPONENT 8: Amazon SNS — ecomm-ddos-alerts

```
TOPIC    : ecomm-ddos-alerts
ARN      : arn:aws:sns:us-east-1:989864147584:ecomm-ddos-alerts
PROTOCOL : email
ENDPOINT : mindofyaseen@gmail.com
STATUS   : ⚠️ Pending confirmation — inbox me link click karo!
```

**Email jo aata hai:**
```
Subject: DDoS Alert: User bot_user_001 flagged
Body:
  Suspicious activity detected!
  User ID    : bot_user_001
  Event Count: 42 events in 1 minute
  Window     : 05:30:00 -> 05:31:00
  Flagged At : 2026-07-29T05:31:58+00:00
```

---

# 🟢 FLINK BRANCH 2 — AWS GLUE PATH

---

## 🔧 COMPONENT 9: AWS Glue — ecomm_flink_db

```
DATABASE : ecomm_flink_db
STATUS   : ✅ Active
ROLE     : GlueCrawlerRole
```

**Yeh Flink ka dusra output branch hai:**
- Flink se processed/aggregated data Glue catalog me jaata hai
- Glue ETL job data transform karta hai
- QuickSight Glue ke through data visualize karta hai

**Glue Catalog Tables (live):**
```
TABLE NAME                LOCATION
─────────────────────────────────────────────────────────
raw                     → s3://.../raw/           (CSV)
raw_stream              → s3://.../raw-stream/    (GZIP)
processed               → s3://.../processed/     (Parquet)
ecomm_events            → virtual (Flink table)
v_unique_visitors_daily → Athena view
v_cart_abandonment      → Athena view
v_top_categories_hourly → Athena view
v_brand_insights        → Athena view
```

**Glue ETL Job:**
```
JOB NAME : ecomm-etl-to-parquet
SCRIPT   : etl_to_parquet.py
INPUT    : ecomm_flink_db.raw (CSV)
OUTPUT   : s3://.../processed/events/ (Parquet, by event_type)
STATUS   : SUCCEEDED

Partitioned output:
  event_type=view/           → 82,390 rows
  event_type=cart/           → 13,883 rows
  event_type=purchase/       → 2,023  rows
  event_type=remove_from_cart→ 1,704  rows
```

---

## 📈 COMPONENT 10: Amazon QuickSight

```
EDITION  : STANDARD
USER     : YASEEN-CLI-NEW
EMAIL    : mindofyaseen@gmail.com
DATA SRC : Athena DirectQuery (via Glue catalog)
STATUS   : ⚠️ 1 console step baaki
```

**4 Dashboards (Glue → Athena → QuickSight):**
```
Dataset              View Used                    Chart Type
──────────────────────────────────────────────────────────────
ds-unique-visitors → v_unique_visitors_daily  →  Line Chart
ds-cart-abandonment→ v_cart_abandonment       →  Pie Chart
ds-top-categories  → v_top_categories_hourly  →  Heat Map
ds-brand-insights  → v_brand_insights         →  Bar Chart
```

**Ek step baaki:**
```
Console → QuickSight → Manage QuickSight
→ Security & Permissions → Add Athena + ecomm-analytics-yaseen-2026 bucket
→ phir run: python create_quicksight_datasets.py
```

---

# 🟡 RAW DATA PATH — FIREHOSE TO S3

---

## 🔥 COMPONENT 11: Amazon Data Firehose — ecomm-firehose-str

```
NAME        : ecomm-firehose-str
STATUS      : ✅ ACTIVE
TYPE        : DirectPut
DESTINATION : S3 → ecomm-analytics-yaseen-2026/raw-stream/
BUFFER      : 5 MB ya 60 seconds
COMPRESSION : GZIP
ROLE        : FirehoseS3DeliveryRole
```

**Kya karta hai:**
- Application directly records bhejti hai (raw data backup)
- Buffer karta hai phir S3 pe GZIP compress kar ke store
- Yeh sirf raw data persistence ke liye hai

**Aaj ka actual result:**
```
500 records → Firehose → S3
File: raw-stream/2026/07/29/05/ecomm-firehose-str-1-2026-07-29-05-32-43-*.gz
Size: 11.7 KiB | Time: 10:33:44 PKT
```

---

## 🪣 COMPONENT 12: Amazon S3 — ecomm-analytics-yaseen-2026

```
BUCKET : ecomm-analytics-yaseen-2026
REGION : us-east-1
STATUS : ✅ Active
```

**Folders:**
```
ecomm-analytics-yaseen-2026/
├── raw/           ← CSV source (2026-Jun-sample.csv, 12.9 MB)
├── raw-stream/    ← Firehose raw backup (GZIP .gz files)
│   └── 2026/07/29/05/
│       └── *.gz  (11.7 KiB — aaj ka)
├── processed/     ← Glue ETL output (Parquet, partitioned)
│   └── events/
│       ├── event_type=view/
│       ├── event_type=cart/
│       ├── event_type=purchase/
│       └── event_type=remove_from_cart/
├── scripts/       ← Glue ETL script (etl_to_parquet.py)
└── athena-results/← Athena query results (temp)
```

---

# 🔐 IAM ROLES

```
ROLE NAME                    TRUST       PERMISSIONS
──────────────────────────────────────────────────────────────────
FirehoseS3DeliveryRole     → Firehose  → S3 put/get/list
LambdaAlertProcessorRole   → Lambda    → Kinesis read
                                          DynamoDB full
                                          SNS publish
                                          CloudWatch put
GlueCrawlerRole            → Glue      → AWSGlueServiceRole
                                          S3 full access
aws-quicksight-service-    → QuickSight→ Athena + Glue + S3
role-v0                                   (custom policy)
```

---

# 💰 COST

```
HOURLY BILLING — Session end pe band karo:
  ecomm-events-stream   ~ $0.04/hr
  ecomm-alerts-stream   ~ $0.04/hr
  ecomm-ddos-detector   ~ $0.44/hr (Flink Studio)
  ──────────────────────────────────
  TOTAL ≈ $0.52/hr

ZERO IDLE COST:
  S3, Lambda, DynamoDB, Glue, Athena, SNS, CloudWatch, Firehose (idle)
```

**Session end commands:**
```powershell
aws kinesisanalyticsv2 stop-application --application-name ecomm-ddos-detector --region us-east-1
aws kinesis delete-stream --stream-name ecomm-events-stream --region us-east-1
aws kinesis delete-stream --stream-name ecomm-alerts-stream --region us-east-1
```

---

# ✅ ABHI KA LIVE STATUS

```
SERVICE                  STATUS      DETAIL
──────────────────────────────────────────────────────────
ecomm-events-stream      ✅ ACTIVE   4 shards ON_DEMAND
ecomm-alerts-stream      ✅ ACTIVE   4 shards ON_DEMAND
ecomm-firehose-str       ✅ ACTIVE   DirectPut → S3
ecomm-ddos-detector      ✅ RUNNING  Flink 3.0 + Zeppelin
ecomm-alert-processor    ✅ Active   Python 3.12, Enabled
ecomm-suspicious-users   ✅ ACTIVE   6 items
ecomm-ddos-alerts (SNS)  ⚠️ PENDING  Email confirm karo
ecomm-ddos-monitor (CW)  ✅ Active   4 widgets
ecomm_flink_db (Glue)    ✅ Active   4 tables + 4 views
S3 raw-stream/           ✅ Active   11.7 KiB file today
S3 processed/ (Parquet)  ✅ Active   100k rows partitioned
QuickSight               ⚠️ PENDING  1 console step baaki
──────────────────────────────────────────────────────────
```

---

# 📁 FILES

```
FILE                           KAAM
─────────────────────────────────────────────────────────────────
simulate_kinesis_stream.ipynb→ Application: CSV → Kinesis Stream
simulate_stream.ipynb        → Application: CSV → Firehose → S3
run_kinesis.py               → CLI se Kinesis pipeline run karo
run_firehose.py              → CLI se Firehose pipeline run karo
lambda_alert_processor.py    → Lambda: alert → DynamoDB+CW+SNS
etl_to_parquet.py            → Glue ETL: CSV → Parquet
create_quicksight_datasets.py→ QuickSight: 4 datasets register
zeppelin_setup.py            → Flink SQL reference
SESSION_STARTUP.md           → Har session startup guide
PROJECT_FLOW.md              → Yeh file
─────────────────────────────────────────────────────────────────
```

---
*AWS eCommerce Analytics | Muhammad Yaseen | us-east-1 | 2026-07-29*
