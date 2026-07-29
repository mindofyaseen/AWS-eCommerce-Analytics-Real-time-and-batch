import json
import boto3
import base64
from datetime import datetime, timezone

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
sns = boto3.client("sns", region_name="us-east-1")
cloudwatch = boto3.client("cloudwatch", region_name="us-east-1")

TABLE_NAME = "ecomm-suspicious-users"
TOPIC_ARN = "arn:aws:sns:us-east-1:989864147584:ecomm-ddos-alerts"


def lambda_handler(event, context):
    table = dynamodb.Table(TABLE_NAME)
    processed = 0

    for record in event["Records"]:
        try:
            raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            payload = json.loads(raw)

            user_id = str(payload.get("user_id", "unknown"))
            event_count = int(payload.get("event_count", 0))
            window_start = str(payload.get("window_start", ""))
            window_end = str(payload.get("window_end", ""))
            flagged_at = datetime.now(timezone.utc)  # datetime object (not string)

            # DynamoDB — store flagged user
            table.put_item(Item={
                "user_id": user_id,
                "window_start": window_start,
                "event_count": event_count,
                "window_end": window_end,
                "flagged_at": flagged_at.isoformat(),
            })

            # CloudWatch — Timestamp must be datetime object, NOT a string
            cloudwatch.put_metric_data(
                Namespace="eComm/DDoS",
                MetricData=[{
                    "MetricName": "SuspiciousUserEvents",
                    "Dimensions": [{"Name": "user_id", "Value": user_id}],
                    "Value": float(event_count),
                    "Unit": "Count",
                    "Timestamp": flagged_at,  # datetime object — boto3 requirement
                }],
            )

            # SNS — email alert
            sns.publish(
                TopicArn=TOPIC_ARN,
                Subject=f"DDoS Alert: User {user_id} flagged",
                Message=(
                    f"Suspicious activity detected!\n\n"
                    f"User ID    : {user_id}\n"
                    f"Event Count: {event_count} events in 1 minute\n"
                    f"Window     : {window_start} -> {window_end}\n"
                    f"Flagged At : {flagged_at.isoformat()}\n"
                ),
            )

            processed += 1
            print(f"[OK] Processed user_id={user_id}, event_count={event_count}")

        except Exception as e:
            print(f"[ERROR] Failed to process record: {e}")
            # Continue processing remaining records
            continue

    return {"statusCode": 200, "processed": processed}

