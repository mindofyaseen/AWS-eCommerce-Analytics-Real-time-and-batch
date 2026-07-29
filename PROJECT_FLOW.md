# 🗺️ AWS eCommerce Analytics — Har Component Ki Detail (Live Running State)

> ⚠️ Yeh file **actual running values** ke saath hai — jo abhi AWS pe chal raha hai, wohi yahan dikh raha hai.
> Region: us-east-1 (N. Virginia) | Account: 989864147584

---

## 🌐 POORA SYSTEM EK NAZAR ME

```
╔══════════════════════════════════════════════════════════════════════╗
║              AWS eCommerce Analytics Platform                        ║
║                    us-east-1 (N. Virginia)                          ║
╠══════════════════════════════════════════════════════════════════════╣
║                                                                      ║
║  [LOCAL]              [REAL-TIME PIPELINE]                           ║
║  Python App    ──►  Kinesis Events Stream                            ║
║  VS Code            ecomm-events-stream  ──►  Apache Flink           ║
║                          (ACTIVE)              ecomm-ddos-detector   ║
║  Python App    ──►  Firehose                   (RUNNING)             ║
║  VS Code            ecomm-firehose-str    ──►  Kinesis Alerts Stream ║
║                          (ACTIVE)              ecomm-alerts-stream   ║
║                              │                     (ACTIVE)          ║
║                              ▼                         │             ║
║                    [BATCH PIPELINE]                    ▼             ║
║                    S3 Bucket                      AWS Lambda         ║
║            ecomm-analytics-yaseen-2026        ecomm-alert-processor  ║
║                    (raw-stream/)                   (Active)          ║
║                          │                    ┌────┼────┐           ║
║                          ▼                    ▼    ▼    ▼           ║
║                    AWS Glue                DynamoDB CW  SNS          ║
║                    ecomm_flink_db          (6 rows) 📊  📧           ║
║                          │                                           ║
║                          ▼                                           ║
║                    Amazon Athena ──► Amazon QuickSight               ║
║                    (4 views)         (Dashboards)                    ║
╚══════════════════════════════════════════════════════════════════════╝
```

---

# 🔴 PIPELINE 1 — REAL-TIME DDoS DETECTION

---

## 🖥️ COMPONENT 1: Local Python App (Data Generator)

```
TYPE    : Local Script (VS Code me run hoti hai)
FILES   : simulate_kinesis_stream.ipynb
          simulate_stream.ipynb
PURPOSE : CSV data padhti hai aur AWS services ko bhejti hai
```

**Kya karta hai:**
- `2026-Jun-sample.csv` file kholta hai (~100,000 rows)
- Batches of 20 records bana ke Kinesis ya Firehose ko bhejta hai
- Har batch ke baad thoda wait karta hai (real traffic feel ke liye)

**Aaj jo data bheja gaya (actual):**
```
simulate_kinesis_stream → 300 normal events + 45 bot events
simulate_stream        → 500 records → Firehose
```

**Ek sample event jo bheja gaya:**
```json
{
  "event_time"    : "2026-07-29 05:30:00 UTC",
  "event_type"    : "view",
  "user_id"       : "bot_attacker_001",
  "product_id"    : "4567890",
  "category_code" : "electronics.smartphone",
  "brand"         : "samsung",
  "price"         : "450.99",
  "user_session"  : "sess_bot_attacker_001_3"
}
```

---

## 📡 COMPONENT 2: Amazon Kinesis Data Stream — ecomm-events-stream

```
SERVICE : Amazon Kinesis Data Streams
NAME    : ecomm-events-stream
STATUS  : ✅ ACTIVE
MODE    : ON_DEMAND (auto-scaling)
SHARDS  : 4 (auto-managed)
REGION  : us-east-1
ARN     : arn:aws:kinesis:us-east-1:989864147584:stream/ecomm-events-stream
```

**Kya karta hai:**
- Python app se events receive karta hai
- Data ko real-time buffer karta hai
- Flink application is stream ko continuously padhti hai
- ON_DEMAND mode = traffic ke hisaab se automatically scale hota hai

**Flow:**
```
Python put_records() ──► Kinesis Shard (buffer) ──► Flink reads LATEST
```

**Note:** Yeh resource ephemeral hai — session end pe delete karo (billing hoti hai)

---

## ⚡ COMPONENT 3: Apache Flink — ecomm-ddos-detector

```
SERVICE : Amazon Managed Service for Apache Flink
NAME    : ecomm-ddos-detector
STATUS  : ✅ RUNNING
RUNTIME : ZEPPELIN-FLINK-3_0
MODE    : INTERACTIVE (Studio Notebook)
VERSION : 1
ARN     : arn:aws:kinesisanalytics:us-east-1:989864147584:application/ecomm-ddos-detector
```

**Kya karta hai:**
- `ecomm-events-stream` ko continuously read karta hai
- Har user ke events ek 1-minute tumbling window me count karta hai
- Agar kisi user ne 1 minute me 5 se zyada events kiye = SUSPICIOUS
- Alert `ecomm-alerts-stream` ko bhejta hai

**Flink SQL jo run ho raha hai (3 cells — Zeppelin me):**

```sql
-- CELL 1: Source Table Define karo (Kinesis se padhna)
CREATE TABLE ecomm_events (
    event_time    VARCHAR,
    event_type    VARCHAR,
    product_id    VARCHAR,
    category_id   VARCHAR,
    category_code VARCHAR,
    brand         VARCHAR,
    price         DOUBLE,
    user_id       VARCHAR,
    user_session  VARCHAR,
    event_arrival_time AS PROCTIME()   -- processing time
)
WITH (
    'connector'             = 'kinesis',
    'stream'                = 'ecomm-events-stream',
    'aws.region'            = 'us-east-1',
    'scan.stream.initpos'   = 'LATEST',
    'format'                = 'json'
);

-- CELL 2: Sink Table Define karo (Kinesis ko likhna)
CREATE TABLE ecomm_alerts_sink (
    user_id      VARCHAR,
    event_count  BIGINT,
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3)
)
WITH (
    'connector' = 'kinesis',
    'stream'    = 'ecomm-alerts-stream',
    'aws.region'= 'us-east-1',
    'format'    = 'json'
);

-- CELL 3: Anomaly Detection Query (yeh continuously chalta rehta hai)
INSERT INTO ecomm_alerts_sink
SELECT
    user_id,
    COUNT(*)                                                         AS event_count,
    TUMBLE_START(event_arrival_time, INTERVAL '1' MINUTE)           AS window_start,
    TUMBLE_END(event_arrival_time, INTERVAL '1' MINUTE)             AS window_end
FROM ecomm_events
GROUP BY
    user_id,
    TUMBLE(event_arrival_time, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5;   -- 5 se zyada = bot/attacker!
```

**Real example — aaj kya detect hua:**
```
bot_attacker_001 → 15 events in 1 min → FLAGGED → alert sent
bot_attacker_002 → 15 events in 1 min → FLAGGED → alert sent
bot_attacker_003 → 15 events in 1 min → FLAGGED → alert sent
normal_user_xyz  → 2  events in 1 min → SAFE    → no alert
```

---

## 📡 COMPONENT 4: Amazon Kinesis Data Stream — ecomm-alerts-stream

```
SERVICE : Amazon Kinesis Data Streams
NAME    : ecomm-alerts-stream
STATUS  : ✅ ACTIVE
MODE    : ON_DEMAND (auto-scaling)
SHARDS  : 4 (auto-managed)
ARN     : arn:aws:kinesis:us-east-1:989864147584:stream/ecomm-alerts-stream
```

**Kya karta hai:**
- Flink ka output receive karta hai (flagged users)
- Lambda function is stream se auto-trigger hoti hai
- Data format:
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
SERVICE  : AWS Lambda
NAME     : ecomm-alert-processor
STATUS   : ✅ Active
RUNTIME  : Python 3.12
MEMORY   : 256 MB
TIMEOUT  : 30 seconds
HANDLER  : lambda_alert_processor.lambda_handler
TRIGGER  : Kinesis ecomm-alerts-stream (BatchSize: 10, State: Enabled)
ROLE     : LambdaAlertProcessorRole
ARN      : arn:aws:lambda:us-east-1:989864147584:function:ecomm-alert-processor
LOG GROUP: /aws/lambda/ecomm-alert-processor
```

**Kya karta hai:**
- `ecomm-alerts-stream` me nayi records aate hi automatically trigger hoti hai
- Ek saath 3 kaam karti hai:

```python
# 1️⃣ DynamoDB me suspicious user save karo
table.put_item(Item={
    "user_id"     : "bot_user_001",
    "window_start": "2026-07-29 05:30:00",
    "event_count" : 42,
    "window_end"  : "2026-07-29 05:31:00",
    "flagged_at"  : "2026-07-29T05:31:58.231466+00:00"
})

# 2️⃣ CloudWatch metric publish karo
cloudwatch.put_metric_data(
    Namespace  = "eComm/DDoS",
    MetricData = [{"MetricName": "SuspiciousUserEvents", "Value": 42}]
)

# 3️⃣ Email alert SNS se bhejo
sns.publish(
    TopicArn = "arn:aws:sns:us-east-1:989864147584:ecomm-ddos-alerts",
    Subject  = "DDoS Alert: User bot_user_001 flagged",
    Message  = "42 events in 1 minute detected!"
)
```

**Aaj ki actual Lambda log:**
```
START RequestId: da6265ab-7a3f-434c-a62e-a7710d1ddf99
[OK] Processed user_id=bot_user_003, event_count=23
END RequestId: da6265ab-7a3f-434c-a62e-a7710d1ddf99
Duration: 360.52 ms | Billed: 863 ms | Memory: 97 MB used
```

---

## 🗃️ COMPONENT 6: Amazon DynamoDB — ecomm-suspicious-users

```
SERVICE      : Amazon DynamoDB
TABLE NAME   : ecomm-suspicious-users
STATUS       : ✅ ACTIVE
BILLING      : PAY_PER_REQUEST (sirf use pe charge)
PRIMARY KEY  : user_id (HASH) + window_start (RANGE)
ARN          : arn:aws:dynamodb:us-east-1:989864147584:table/ecomm-suspicious-users
```

**Kya karta hai:**
- Lambda ke processed alerts store karta hai
- PK: user_id = same user ko dobara track kar sako
- SK: window_start = kab pakda gaya record rakha jata hai

**Abhi table me jo data hai (live):**
```
┌──────────────────┬─────────────┬──────────────────────────────────┐
│ user_id          │ event_count │ flagged_at                       │
├──────────────────┼─────────────┼──────────────────────────────────┤
│ bot_user_001     │ 42          │ 2026-07-29T05:31:58.231466+00:00 │
│ bot_user_002     │ 87          │ 2026-07-29T05:31:58.231542+00:00 │
│ bot_user_003     │ 23          │ 2026-07-29T05:31:58.243100+00:00 │
│ bot_user_777     │ 18          │ 2026-07-28T11:00:49.699105+00:00 │
│ bot_user_888     │ 25          │ 2026-07-28T11:00:49.959130+00:00 │
│ test_user_999    │ 12          │ 2026-07-28T10:59:52.337248+00:00 │
└──────────────────┴─────────────┴──────────────────────────────────┘
Total: 6 records (3 aaj + 3 pichle session se)
```

---

## 📊 COMPONENT 7: Amazon CloudWatch — ecomm-ddos-monitor

```
SERVICE   : Amazon CloudWatch
DASHBOARD : ecomm-ddos-monitor
NAMESPACE : eComm/DDoS
METRIC    : SuspiciousUserEvents
STATUS    : ✅ Active
```

**Kya karta hai:**
- Lambda har alert pe ek metric point publish karta hai
- Dashboard me 4 widgets hain:
  1. SuspiciousUserEvents — real-time spike graph
  2. Lambda Invocations + Errors
  3. Kinesis Records (ecomm-alerts-stream)
  4. Lambda Duration (milliseconds)

**Console me kaise dekhen:**
```
AWS Console → CloudWatch → Dashboards → ecomm-ddos-monitor
```

---

## 📧 COMPONENT 8: Amazon SNS — ecomm-ddos-alerts

```
SERVICE   : Amazon Simple Notification Service
TOPIC     : ecomm-ddos-alerts
ARN       : arn:aws:sns:us-east-1:989864147584:ecomm-ddos-alerts
PROTOCOL  : email
ENDPOINT  : mindofyaseen@gmail.com
STATUS    : Subscription pending confirmation
```

**Kya karta hai:**
- Lambda har flagged user pe email bhejta hai
- Subject: "DDoS Alert: User bot_user_001 flagged"
- Body me: user_id, event_count, window time, flagged_at

**⚠️ Abhi email confirm nahi ki — inbox me link click karo!**

---

---

# 🟢 PIPELINE 2 — BATCH ANALYTICS

---

## 🔥 COMPONENT 9: Amazon Data Firehose — ecomm-firehose-str

```
SERVICE      : Amazon Data Firehose
NAME         : ecomm-firehose-str
STATUS       : ✅ ACTIVE
TYPE         : DirectPut (app directly bhejti hai)
DESTINATION  : S3 — ecomm-analytics-yaseen-2026
PREFIX       : raw-stream/
BUFFER SIZE  : 5 MB ya 60 seconds (jo pehle ho)
COMPRESSION  : GZIP (.gz files)
ROLE         : FirehoseS3DeliveryRole
```

**Kya karta hai:**
- Python app directly records bhejti hai (no Kinesis needed)
- Data buffer karta hai, phir S3 me GZIP compress kar ke store karta hai
- Auto-retry on failure

**Aaj bheja gaya (actual):**
```
500 records → 25 batches of 20 → GZIP → S3
File: raw-stream/2026/07/29/05/ecomm-firehose-str-1-2026-07-29-05-32-43-*.gz
Size: 11.7 KiB
Time: 10:33:44 PKT (aaj)
```

---

## 🪣 COMPONENT 10: Amazon S3 — ecomm-analytics-yaseen-2026

```
SERVICE  : Amazon S3
BUCKET   : ecomm-analytics-yaseen-2026
REGION   : us-east-1
STATUS   : ✅ Active
```

**Bucket Structure (Folders):**
```
ecomm-analytics-yaseen-2026/
├── raw/                    ← CSV file yahan hai (manually upload ki thi)
│   └── 2026-Jun-sample.csv    (~100,000 rows, 12.9 MB)
│
├── raw-stream/             ← Firehose ka output (GZIP JSON)
│   └── 2026/07/29/05/
│       └── ecomm-firehose-str-*.gz  (11.7 KiB — aaj ka)
│
├── processed/              ← Glue ETL ka output (Parquet)
│   └── events/
│       ├── event_type=view/           (82,390 rows)
│       ├── event_type=cart/           (13,883 rows)
│       ├── event_type=purchase/       (2,023 rows)
│       └── event_type=remove_from_cart/ (1,704 rows)
│
├── scripts/                ← Glue ETL Python script
│   └── etl_to_parquet.py
│
└── athena-results/         ← Athena query results (temporary)
```

---

## 🕷️ COMPONENT 11: AWS Glue — ecomm_flink_db

```
SERVICE  : AWS Glue Data Catalog
DATABASE : ecomm_flink_db
STATUS   : ✅ Active
ROLE     : GlueCrawlerRole
REGION   : us-east-1
```

**Tables in Catalog (live):**
```
TABLE NAME              LOCATION                                    TYPE
──────────────────────────────────────────────────────────────────────────
raw                   → s3://.../raw/          (CSV — source data)
raw_stream            → s3://.../raw-stream/   (GZIP JSON — Firehose)
processed             → s3://.../processed/    (Parquet — ETL output)
ecomm_events          → null                   (Flink table — virtual)
v_unique_visitors_daily → (view)               (Athena SQL view)
v_cart_abandonment    → (view)                 (Athena SQL view)
v_top_categories_hourly → (view)               (Athena SQL view)
v_brand_insights      → (view)                 (Athena SQL view)
```

**3 Crawlers jo chale:**
```
ecomm-raw-crawler       → raw/ scan kiya (CSV schema detect)
ecomm-rawstream-crawler → raw-stream/ scan (JSON schema detect)
ecomm-processed-crawler → processed/ scan (Parquet schema detect)
```

**Glue ETL Job:**
```
JOB NAME : ecomm-etl-to-parquet
SCRIPT   : etl_to_parquet.py (s3://.../scripts/)
INPUT    : ecomm_flink_db.raw (CSV)
OUTPUT   : s3://.../processed/events/ (Parquet, partitioned by event_type)
STATUS   : SUCCEEDED (pehle run ho chuka hai)

Transformations:
  ✅ event_type filter: sirf view/cart/purchase/remove_from_cart
  ✅ NULL hata do: event_time, event_type, user_id required
  ✅ Timestamp parse: string → datetime format
  ✅ Partition by event_type (fast queries ke liye)
```

---

## 🔍 COMPONENT 12: Amazon Athena — 4 Analytical Views

```
SERVICE   : Amazon Athena
DATABASE  : ecomm_flink_db
ENGINE    : Athena v3 (Presto/Trino based)
RESULTS   : s3://.../athena-results/
STATUS    : ✅ Active
```

**4 Views jo create hain:**

### View 1: v_unique_visitors_daily
```sql
-- Har din ke unique visitors
SELECT
    DATE(event_time) AS event_date,
    COUNT(DISTINCT user_id) AS unique_visitors
FROM processed
GROUP BY DATE(event_time)
ORDER BY event_date
```

### View 2: v_cart_abandonment
```sql
-- Kितne sessions ne cart me add kiya but purchase nahi kiya
SELECT
    user_session,
    MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS added_to_cart,
    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchased
FROM processed
GROUP BY user_session
```

### View 3: v_top_categories_hourly
```sql
-- Har ghante me kaun sa category zyada dekha gaya
SELECT
    HOUR(event_time) AS hour_of_day,
    category_code,
    COUNT(*) AS view_count
FROM processed
WHERE event_type = 'view'
GROUP BY HOUR(event_time), category_code
ORDER BY view_count DESC
```

### View 4: v_brand_insights
```sql
-- Kaun sa brand zyada purchase hua aur avg price kya tha
SELECT
    brand,
    COUNT(*) AS total_purchases,
    AVG(price) AS avg_price
FROM processed
WHERE event_type = 'purchase'
GROUP BY brand
ORDER BY total_purchases DESC
```

**Aaj run hua Athena result (actual):**
```
┌──────────────┬────────────────────┬──────────────┐
│ total_events │ event_type         │ unique_users │
├──────────────┼────────────────────┼──────────────┤
│ 82,390       │ view               │ 4,779        │
│ 13,883       │ cart               │ 4,120        │
│ 2,023        │ purchase           │ 1,492        │
│ 1,704        │ remove_from_cart   │ 1,369        │
└──────────────┴────────────────────┴──────────────┘

Cart Abandonment Rate = (13,883 - 2,023) / 13,883 × 100 = 85.4%
(Industry average bhi ~70-80% hoti hai — realistic!)
```

---

## 📈 COMPONENT 13: Amazon QuickSight (Pending)

```
SERVICE   : Amazon QuickSight
EDITION   : STANDARD
USER      : YASEEN-CLI-NEW
EMAIL     : mindofyaseen@gmail.com
DATA SRC  : ecomm-athena-source (Athena DirectQuery)
STATUS    : ⚠️ Subscribed — Athena access enable karna baaki
```

**4 Datasets (create hone hain):**
```
ds-unique-visitors → v_unique_visitors_daily → Line Chart
ds-cart-abandonment → v_cart_abandonment → Pie Chart
ds-top-categories → v_top_categories_hourly → Heat Map
ds-brand-insights → v_brand_insights → Bar Chart
```

**Ek step baaki:**
```
Console → QuickSight → Manage QuickSight
→ Security & Permissions
→ Add Athena + S3 bucket ecomm-analytics-yaseen-2026
→ phir: python create_quicksight_datasets.py
```

---

## 🔐 IAM ROLES — Permissions System

```
ROLE NAME                   → TRUST      → POLICIES
──────────────────────────────────────────────────────────────────
FirehoseS3DeliveryRole      → Firehose   → S3 put/get/list
                                           (raw-stream/ prefix)

LambdaAlertProcessorRole    → Lambda     → AWSLambdaKinesisExecutionRole
                                           AmazonDynamoDBFullAccess
                                           AmazonSNSFullAccess
                                           CloudWatchFullAccess

GlueCrawlerRole             → Glue       → AWSGlueServiceRole
                                           AmazonS3FullAccess

aws-quicksight-service-role → QuickSight → QuickSightAthenaS3Access
-v0                                        (custom policy)
```

---

## 💰 COST — Abhi Kya Chal Raha Hai

```
HOURLY BILLING (ABHI RUNNING — session end pe band karo):
╔══════════════════════════════════════════════════════╗
║  ecomm-events-stream   ~ $0.04/hr (On-Demand)       ║
║  ecomm-alerts-stream   ~ $0.04/hr (On-Demand)       ║
║  ecomm-ddos-detector   ~ $0.44/hr (Flink Studio)    ║
║  ─────────────────────────────────────────────────  ║
║  TOTAL RUNNING COST  ≈  $0.52/hr                    ║
╚══════════════════════════════════════════════════════╝

ZERO IDLE COST (kuch nahi lagta jab use na ho):
  ✅ S3          → storage: ~$0.023/GB/month
  ✅ Lambda      → first 1M requests FREE
  ✅ DynamoDB    → PAY_PER_REQUEST (idle = $0)
  ✅ Glue Catalog→ first million objects FREE
  ✅ Athena      → $5 per TB scanned
  ✅ SNS         → first 1M emails FREE
  ✅ CloudWatch  → basic monitoring FREE
  ✅ Firehose    → $0.029/GB (sirf jab data bhejo)
```

**Session end karne pe yeh 3 commands:**
```powershell
aws kinesisanalyticsv2 stop-application --application-name ecomm-ddos-detector --region us-east-1
aws kinesis delete-stream --stream-name ecomm-events-stream --region us-east-1
aws kinesis delete-stream --stream-name ecomm-alerts-stream --region us-east-1
```

---

## 📁 REPOSITORY FILES — Har File Ka Kaam

```
FILE                           KAAM
─────────────────────────────────────────────────────────────────────
2026-Jun-sample.csv          → Source data (100k eCommerce events)
simulate_kinesis_stream.ipynb→ CSV → Kinesis ecomm-events-stream
simulate_stream.ipynb        → CSV → Firehose ecomm-firehose-str
run_kinesis.py               → CLI se Kinesis pipeline run karo
run_firehose.py              → CLI se Firehose pipeline run karo
lambda_alert_processor.py    → Lambda: alert → DynamoDB + CW + SNS
etl_to_parquet.py            → Glue Job: CSV/JSON → Parquet
create_quicksight_datasets.py→ QuickSight me 4 datasets register
dashboard.json               → CloudWatch dashboard ka backup/export
zeppelin_setup.py            → Flink SQL reference (browser me karna)
glue-trust.json              → Glue IAM trust policy
lambda-trust.json            → Lambda IAM trust policy
qs-trust.json                → QuickSight IAM trust policy
qs-policy.json               → QuickSight custom IAM policy
SESSION_STARTUP.md           → Har session ka startup guide (CLI)
PROJECT_FLOW.md              → Yeh file — poora system explained
─────────────────────────────────────────────────────────────────────
```

---

## ✅ ABHI KA LIVE STATUS (2026-07-29)

```
SERVICE                    STATUS      DETAIL
──────────────────────────────────────────────────────────────────
ecomm-events-stream        ✅ ACTIVE   4 shards, ON_DEMAND
ecomm-alerts-stream        ✅ ACTIVE   4 shards, ON_DEMAND
ecomm-firehose-str         ✅ ACTIVE   DirectPut → S3
ecomm-ddos-detector        ✅ RUNNING  Zeppelin-Flink-3.0
ecomm-alert-processor      ✅ Active   Python 3.12, trigger Enabled
ecomm-suspicious-users     ✅ ACTIVE   6 items stored
ecomm-ddos-alerts (SNS)    ⚠️ PENDING  Email confirmation baaki
ecomm-ddos-monitor (CW)    ✅ Active   4 widgets
ecomm_flink_db (Glue)      ✅ Active   4 tables + 4 views
S3 raw-stream/             ✅ Active   11.7 KiB file today
S3 processed/ (Parquet)    ✅ Active   100k rows partitioned
Athena views               ✅ Active   All 4 views working
QuickSight                 ⚠️ PENDING  Athena access step baaki
──────────────────────────────────────────────────────────────────
```

---

*AWS eCommerce Analytics Platform | Muhammad Yaseen | us-east-1*
*Last Updated: 2026-07-29 | All values are actual live AWS readings*
