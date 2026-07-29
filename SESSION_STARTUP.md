# ?? Session Startup Guide — AWS eCommerce Analytics

> Har session start karne pe yeh steps follow karo (PowerShell me)
> Sab commands c:\Users\mindo\Documents\Boto directory se run karo

---

## ? Quick Status Check (Pehle Karo)

```powershell
# Check karo kya streams already exist hain ya nahi
aws kinesis list-streams --region us-east-1

# Flink app ka status
aws kinesisanalyticsv2 describe-application --application-name ecomm-ddos-detector --region us-east-1 --query "ApplicationDetail.ApplicationStatus"

# Lambda trigger status
aws lambda list-event-source-mappings --function-name ecomm-alert-processor --region us-east-1 --query "EventSourceMappings[0].State"
```

---

## Step 1: Kinesis Streams Recreate Karo

```powershell
aws kinesis create-stream --stream-name ecomm-events-stream --stream-mode-details StreamMode=ON_DEMAND --region us-east-1
aws kinesis create-stream --stream-name ecomm-alerts-stream --stream-mode-details StreamMode=ON_DEMAND --region us-east-1
Start-Sleep -Seconds 30
aws kinesis list-streams --region us-east-1
```

---

## Step 2: Lambda Trigger Re-Enable Karo

```powershell
aws lambda update-event-source-mapping --uuid 5b61f29c-44d5-4164-a8bc-10ceeb3f3d98 --enabled --region us-east-1
aws lambda list-event-source-mappings --function-name ecomm-alert-processor --region us-east-1 --query "EventSourceMappings[0].State"
```

---

## Step 3: Flink Studio Start Karo

```powershell
aws kinesisanalyticsv2 start-application --application-name ecomm-ddos-detector --region us-east-1
# ~3-4 minutes wait karo RUNNING hone ke liye
aws kinesisanalyticsv2 describe-application --application-name ecomm-ddos-detector --region us-east-1 --query "ApplicationDetail.ApplicationStatus"
```

Phir AWS Console ? Kinesis Analytics ? ecomm-ddos-detector ? Open Apache Zeppelin

---

## Step 4: Flink SQL in Zeppelin (3 Cells — Browser Only)

### Cell 1 — Source Table
```sql
%flink.ssql
CREATE TABLE ecomm_events (
    event_time VARCHAR, event_type VARCHAR, product_id VARCHAR,
    category_id VARCHAR, category_code VARCHAR, brand VARCHAR,
    price DOUBLE, user_id VARCHAR, user_session VARCHAR,
    event_arrival_time AS PROCTIME()
)
WITH (
    'connector' = 'kinesis', 'stream' = 'ecomm-events-stream',
    'aws.region' = 'us-east-1', 'scan.stream.initpos' = 'LATEST', 'format' = 'json'
)
```

### Cell 2 — Sink Table
```sql
%flink.ssql
CREATE TABLE ecomm_alerts_sink (
    user_id VARCHAR, event_count BIGINT,
    window_start TIMESTAMP(3), window_end TIMESTAMP(3)
)
WITH (
    'connector' = 'kinesis', 'stream' = 'ecomm-alerts-stream',
    'aws.region' = 'us-east-1', 'format' = 'json'
)
```

### Cell 3 — Anomaly Detection (Continuously Running)
```sql
%flink.ssql
INSERT INTO ecomm_alerts_sink
SELECT user_id, COUNT(*) AS event_count,
    TUMBLE_START(event_arrival_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_arrival_time, INTERVAL '1' MINUTE) AS window_end
FROM ecomm_events
GROUP BY user_id, TUMBLE(event_arrival_time, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5
```

---

## Step 5: Real-Time Pipeline Test

```powershell
# VS Code me kholke run karo:
# simulate_kinesis_stream.ipynb  -- Real-time DDoS detection
# simulate_stream.ipynb           -- Batch Firehose pipeline
```

---

## Step 6: Verify

```powershell
# DynamoDB rows check
aws dynamodb scan --table-name ecomm-suspicious-users --region us-east-1 --select COUNT --query "Count"

# S3 Firehose data
aws s3 ls s3://ecomm-analytics-yaseen-2026/raw-stream/ --region us-east-1
```

- CloudWatch: AWS Console ? CloudWatch ? Dashboards ? ecomm-ddos-monitor
- SNS: mindofyaseen@gmail.com inbox check karo

---

## ?? Session END pe Resources Delete Karo

```powershell
aws kinesisanalyticsv2 stop-application --application-name ecomm-ddos-detector --region us-east-1
aws kinesis delete-stream --stream-name ecomm-events-stream --region us-east-1
aws kinesis delete-stream --stream-name ecomm-alerts-stream --region us-east-1
Write-Host "Resources stopped — billing stopped!"
```

---

## Pending One-Time Steps

| Step | Action |
|------|--------|
| SNS Email Confirmation | Click link in mindofyaseen@gmail.com |
| QuickSight Athena Access | Console ? Manage QuickSight ? Security & Permissions ? Add Athena + ecomm-analytics-yaseen-2026 |
| QuickSight Datasets | python create_quicksight_datasets.py |

---
*AWS eCommerce Analytics | Muhammad Yaseen | us-east-1*
