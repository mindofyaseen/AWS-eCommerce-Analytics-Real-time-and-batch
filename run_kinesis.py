"""
Real-Time Pipeline Runner
CSV -> Kinesis ecomm-events-stream -> Flink -> Lambda -> DynamoDB/CW/SNS

Flink > 5 events per minute per user detect karta hai.
Is script me intentionally kuch users ko bari taadad me events bheje jate hain
taake Flink anomaly detect kare.
"""
import boto3, csv, json, time, random

STREAM_NAME = "ecomm-events-stream"
CSV_FILE = "2026-Jun-sample.csv"
REGION = "us-east-1"
BATCH_SIZE = 20
MAX_ROWS = 300  # demo ke liye

client = boto3.client("kinesis", region_name=REGION)

def send_batch(records):
    entries = [
        {"Data": json.dumps(r).encode("utf-8"), "PartitionKey": str(r.get("user_id", "default"))}
        for r in records
    ]
    resp = client.put_records(StreamName=STREAM_NAME, Records=entries)
    failed = resp.get("FailedRecordCount", 0)
    return len(entries) - failed

def main():
    print("=" * 55)
    print("REAL-TIME PIPELINE: CSV -> Kinesis -> Flink -> Lambda")
    print("=" * 55)
    
    # Step 1: Normal events bhejo CSV se
    print("\n[1/2] Normal events stream kar rahe hain...")
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
                print(f"  Batch {total_sent//BATCH_SIZE}: {sent} records -> Kinesis OK")
                batch = []
                time.sleep(0.3)
        
        if batch:
            total_sent += send_batch(batch)
    
    print(f"  Normal events sent: {total_sent}")
    
    # Step 2: Bot behavior simulate karo - same user_id se bahut saare events
    print("\n[2/2] Bot/DDoS behavior simulate kar rahe hain...")
    print("  (Same user_id se > 5 events/minute -> Flink flag karega)")
    
    bot_users = ["bot_attacker_001", "bot_attacker_002", "bot_attacker_003"]
    bot_events = []
    
    now_ts = "2026-07-29 05:30:00 UTC"
    for bot_id in bot_users:
        for i in range(15):  # 15 events per user in same window = DDoS!
            bot_events.append({
                "event_time": now_ts,
                "event_type": "view",
                "product_id": str(random.randint(1000000, 9999999)),
                "category_id": str(random.randint(100, 999)),
                "category_code": "electronics.smartphone",
                "brand": "samsung",
                "price": str(round(random.uniform(100, 900), 2)),
                "user_id": bot_id,
                "user_session": f"sess_{bot_id}_{i}"
            })
    
    # Bot events ek hi minute me bhejo
    for i in range(0, len(bot_events), BATCH_SIZE):
        chunk = bot_events[i:i+BATCH_SIZE]
        sent = send_batch(chunk)
        print(f"  Bot batch: {sent} events sent for users: {set(e['user_id'] for e in chunk)}")
        time.sleep(0.2)
    
    print(f"\nKinesis Results:")
    print(f"  Normal events : {total_sent}")
    print(f"  Bot events    : {len(bot_events)} (3 users x 15 events each)")
    print(f"\nFlink agle 1 minute me bot users detect karega...")
    print(f"Lambda -> DynamoDB -> CloudWatch -> SNS email trigger hoga!")

if __name__ == "__main__":
    main()
