"""
Batch Pipeline Runner
CSV -> Firehose -> S3 raw-stream/
Sirf pehle 500 rows bhejta hai (demo ke liye fast)
"""
import boto3, csv, json, time, random

STREAM_NAME = "ecomm-firehose-str"
CSV_FILE = "2026-Jun-sample.csv"
REGION = "us-east-1"
BATCH_SIZE = 20
MAX_ROWS = 500  # demo ke liye limited

firehose = boto3.client("firehose", region_name=REGION)

def send_batch(records):
    entries = [{"Data": (json.dumps(r) + "\n").encode("utf-8")} for r in records]
    resp = firehose.put_record_batch(DeliveryStreamName=STREAM_NAME, Records=entries)
    failed = resp.get("FailedPutCount", 0)
    if failed > 0:
        print(f"  WARNING: {failed} records fail hue")
    return len(entries) - failed

def main():
    print("=" * 50)
    print("BATCH PIPELINE: CSV -> Firehose -> S3")
    print("=" * 50)
    
    with open(CSV_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        batch = []
        total_sent = 0
        total_read = 0
        
        for row in reader:
            if total_read >= MAX_ROWS:
                break
            batch.append(row)
            total_read += 1
            
            if len(batch) >= BATCH_SIZE:
                sent = send_batch(batch)
                total_sent += sent
                print(f"  Batch {total_sent//BATCH_SIZE}: {sent} records -> Firehose OK")
                batch = []
                time.sleep(0.5)
        
        if batch:
            sent = send_batch(batch)
            total_sent += sent
        
    print(f"\nFirehose Result:")
    print(f"  Total rows read : {total_read}")
    print(f"  Total sent OK   : {total_sent}")
    print(f"  S3 path         : s3://ecomm-analytics-yaseen-2026/raw-stream/")
    print(f"  Note: Firehose buffer 60sec ya 5MB pe S3 me flush karta hai")

if __name__ == "__main__":
    main()
