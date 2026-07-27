"""
Zeppelin REST API ke through Flink SQL tables aur queries create karta hai.
Managed Flink Studio Notebook ka Zeppelin endpoint use karta hai.
"""
import requests
import json
import time

ZEPPELIN_BASE = "https://d1bfc10dd1b161b2488fd6f50b64c484.kinesisdataanalyticsextensions.us-east-1.amazonaws.com"
AUTH_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhcHBsaWNhdGlvbk1ldGFEYXRhQ2lwaGVyVGV4dFN0cmluZyI6IkFZQURlQy9pSmdNY1NNQyt4R1o0d3JaUXJ4WUFYd0FCQUJWaGQzTXRZM0o1Y0hSdkxYQjFZbXhwWXkxclpYa0FSRUUwTVhRMVFuTnhiM3AwVTFwVlluUXpUWFpZZDFCTWFEQnVZbTlVYWxsRWJEbDZaRGsxUzBGQ2FraGxTMDlNSzA5MlIwVnBVMmNyYVcxNWREZEdabUkxVVQwOUFBRUFCMkYzY3kxcmJYTUFTMkZ5YmpwaGQzTTZhMjF6T25WekxXVmhjM1F0TVRvNU9EQTRORGs0TmpBM016STZhMlY1THpNMFpEaGxNell6TFRnek15MHROR1kxWXkwNFlXRXdMVFJpWldZM05tWmtNV013WXdDNEFRSUJBSGcvMEk0VlBNWXI3U01PVWtkQURnM1pOVllxUS8xSHg1Y3BOV1ZIV1YyK0VnR09KVTBlQlE0Yjd1eTBISEVmTWNWQkFBQUFmakI0QmdrcWhraUc5dzBCQndhZ2J6QnRBZ0VBTUdnR0NTcUdTSWIzRFFFSEFUQWVCZ2xnaGtnQlpRTUVBUzR3RVFRTUl6a3lxaWtNblMvaFl2OVhBZ0VRZ0RzM20yUVM5MzFBWVR1aklQVjdlclJCTFJpdzRkVlFJVlkzMDNVMWpKOEMxaGFRWDlxZWdMTFViR29kSmpqNzY1cmVRYlU0dzJQVU40YVpkQUlBQUFBQURBQUFFQUFBQUFBQUFBQUFBQUFBQUFBdEN5c0tvV3EyK2p0Q3Bmem1xaHd0Ly8vLy93QUFBQUVBQUFBQUFBQUFBQUFBQUFFQUFBSytQYWVXS3ZnV2ZObmttalNwYlNCUVpOVUFaYXVxN09MTU1QWDZkR3lNTC80THpHUkJLM0drWGlCQy91Y1IrWDc5aXgwanFZYWU3eTBsZnFmQjdJeUNKQk5wS3FwM2dPUkdoL3JPRTFNaUxHT3QxdFhPN1ZCWUoxWmIrRWhiWnNYWTQ2NzFRMTJVSlVTdk9DVy91ckVQMTQxelZRN3B4djlLV0pGRWlhS0tiUm5rVExIdm1DeUUzTlA3elJQODZmL1NUNXFKL1N5aVNTWGQxZUhHV3NsNmZONXRZZTdyZG5hZzNyYitBSHJHVU02MHRWZU9JRlFadHhucUFEa3hxbWwzazN3eEtVZWtKRmdLTVVvT3lwdU01ZVFtNzZNK1ZKeDFtaE1EbW1nLzNYakJDYTNXS3lVRElHQjdYcEtFRDM3Rmo2MFhlWU9UeGw3ZnZMc0RUR0VWL2M4K0czanM3TkpTbFNPeER2dnpSeDRldDY3TkpiS0ErMkY4cDdHajAzUWJmd3FFWDdrVGVLbHppWEdWeGwxcWV5SnY2YXNzMTMwRzRLUWhQMjIwMHRJZHI1OEkzVnlMV2JrczZLbG9razhLZE1ETXVqR0IyT1VxeVNJWGpYRWhhM3VZOElQQmZJWTNoZ2dqMG5ZaUdPSjdYeGZOMnN4ZEdUYzQ1Z2w1Z3pCdkQ0UW5mbC9WL05RK3VmL3dIRmt4c1VGc1RqVTBuNy83UVFjQnpIZlE4UklIdlJyZkJETVAvNk9mc3FLU0VGK0UzcUVYd3FMbnRLc2l1dG94TUg0a2JhckNROW9acFJBcC84NW4vOG9kbFlSdkJxZmJ4TnB6cTYvZmlVMlk3bllIQkFlREFQVEh5alBaZjdhQjRnLzBlU1ZMYXhFT3B2QUlsRXNvK08xbnJaMXNMUE9RNSs4SG9PTzRqdVdLdEhJQzd1YTZiVmhoRVk3Qmp2enRsK1AxK0h1bTFxcVBJY3EyVGVRN2xiS3k0aXBzRjRqNFNXUWF0Mnlzd3gxeTNmNWxxcWNsbGQzVzBXZVVqb2ozTWxDV0p3TlF5eXZTcHFQbUdNL1d3Y25kcFFudldpMnZ2WHAycUp6RFBhYXJHb1JkdlJZL0JYeTBienRwN3BUZFp1RGp5aGNmM2l2UXg0czJNWVZ4b1ZPUEdUSGx6YnVtVEZocDA3Nk52RVpGZ2h0V295b2NvdmlLZk5LTW16K2RwVEFNSEt1MmVuc1Mxc2RXL3B3WlB3S3M2N2NwRjloNXV2WnFhK3ZhYzFzSmJnQm5NR1VDTVFDU1ZjbjZwZmJLWUZMcWJaVlBqM0pyVyszS1owSnV1dWJQOUFid3F0cDNuZzYyaGlLRXZjL3JoM0d1UG05L0Y2a0NNRXpzdWd2Rzh0M0hkZzhUV0hZRGM2eEpISEZxM2w4bUxsUVpsZmc4RlJnaS9iVlN6QUVsb0lmaEFtcmNuSzVrVkE9PSIsImVuY3lwdGVkUGxhaW5UZXh0S2V5IjoiQVFJQkFIZy8wSTRWUE1ZcjdTTU9Va2RBRGczWk5WWXFRLzFIeDVjcE5XVkhXVjIrRWdIOXlDVGZIRHJYVVd1TEQ4ZVd5WmtnQUFBQW9qQ0Jud1lKS29aSWh2Y05BUWNHb0lHUk1JR09BZ0VBTUlHSUJna3Foa2lHOXcwQkJ3RXdIZ1lKWUlaSUFXVURCQUV1TUJFRURIQllIVWxUYVV6QVBTb2Uzd0lCRUlCYmpVMm43Yit0anR3cXBBRG11azhyR0twS2x1TjV4Nlp5YTR0ellsWTlkV0hnaTM0ZmhXRE12SXVoWWN2alh6eEFmejJyYXJuQzFhbWZyNU5rb3draE1FVmtjV0hXcWZnNDdBTThxdEc4bWRHdFc0aFQrbXMvVWFoT3BRPT0iLCJ2ZXJzaW9uIjoiTWc9PSIsInN1YiI6Ijk4OTg2NDE0NzU4NCIsImV4cCI6MTc4NDk5ODM4MCwiaWF0IjoxNzg0OTU1MTgwfQ.O1OYV2V6pGTH0V0FZduFvVEskkmRNy7eEuIdwLbMj3Y"

TABLE_DDL = """%flink.ssql

CREATE TABLE ecomm_events (
    event_time VARCHAR,
    event_type VARCHAR,
    product_id VARCHAR,
    category_id VARCHAR,
    category_code VARCHAR,
    brand VARCHAR,
    price DOUBLE,
    user_id VARCHAR,
    user_session VARCHAR,
    event_arrival_time AS PROCTIME()
)
WITH (
    'connector' = 'kinesis',
    'stream' = 'ecomm-events-stream',
    'aws.region' = 'us-east-1',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
)"""

SINK_DDL = """%flink.ssql

CREATE TABLE ecomm_alerts_sink (
    user_id VARCHAR,
    event_count BIGINT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3)
)
WITH (
    'connector' = 'kinesis',
    'stream' = 'ecomm-alerts-stream',
    'aws.region' = 'us-east-1',
    'format' = 'json'
)"""

ANOMALY_INSERT = """%flink.ssql

INSERT INTO ecomm_alerts_sink
SELECT
    user_id,
    COUNT(*) AS event_count,
    TUMBLE_START(event_arrival_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_arrival_time, INTERVAL '1' MINUTE) AS window_end
FROM ecomm_events
GROUP BY
    user_id,
    TUMBLE(event_arrival_time, INTERVAL '1' MINUTE)
HAVING COUNT(*) > 5"""


def try_zeppelin_api():
    session = requests.Session()
    session.verify = True

    print("Step 1: Session establish karo...")
    try:
        r = session.get(f"{ZEPPELIN_BASE}/zeppelin/?authToken={AUTH_TOKEN}", timeout=15, allow_redirects=True)
        print(f"  Status: {r.status_code}")
        print(f"  Cookies: {list(session.cookies.keys())}")
    except Exception as e:
        print(f"  Session error: {e}")
        return False

    print("Step 2: Zeppelin API version check...")
    try:
        r = session.get(f"{ZEPPELIN_BASE}/zeppelin/api/version", timeout=10)
        print(f"  Status: {r.status_code}, Body: {r.text[:200]}")
    except Exception as e:
        print(f"  API error: {e}")
        return False

    print("Step 3: Notes list...")
    try:
        r = session.get(f"{ZEPPELIN_BASE}/zeppelin/api/notebook", timeout=10)
        print(f"  Status: {r.status_code}")
        if r.status_code == 200:
            notes = r.json().get("body", [])
            print(f"  Found {len(notes)} notes")
            return True, session, notes
    except Exception as e:
        print(f"  Error: {e}")

    return False, None, []


def create_note_and_run(session, note_name="ecomm-ddos-setup"):
    print(f"\nCreating note '{note_name}'...")
    payload = {"name": note_name, "defaultInterpreterGroup": "flink"}
    r = session.post(f"{ZEPPELIN_BASE}/zeppelin/api/notebook", json=payload, timeout=15)
    print(f"  Create note status: {r.status_code}, body: {r.text[:300]}")
    if r.status_code != 200:
        return False

    note_id = r.json()["body"]
    print(f"  Note ID: {note_id}")

    paragraphs = [
        ("Table DDL - ecomm_events", TABLE_DDL),
        ("Sink DDL - ecomm_alerts_sink", SINK_DDL),
        ("Anomaly INSERT query", ANOMALY_INSERT),
    ]

    paragraph_ids = []
    for title, text in paragraphs:
        print(f"\n  Adding paragraph: {title}")
        para_payload = {"title": title, "text": text}
        r = session.post(f"{ZEPPELIN_BASE}/zeppelin/api/notebook/{note_id}/paragraph", json=para_payload, timeout=15)
        print(f"    Status: {r.status_code}")
        if r.status_code == 200:
            pid = r.json()["body"]
            paragraph_ids.append(pid)
            print(f"    Paragraph ID: {pid}")

    print(f"\nRunning all paragraphs sequentially...")
    for i, pid in enumerate(paragraph_ids):
        title = paragraphs[i][0]
        print(f"  Running: {title}")
        r = session.post(f"{ZEPPELIN_BASE}/zeppelin/api/notebook/job/{note_id}/{pid}", timeout=15)
        print(f"    Run status: {r.status_code}")
        time.sleep(10)

        status_r = session.get(f"{ZEPPELIN_BASE}/zeppelin/api/notebook/job/{note_id}/{pid}", timeout=15)
        if status_r.status_code == 200:
            status_data = status_r.json().get("body", {})
            print(f"    Paragraph status: {status_data.get('status', 'unknown')}")

    return True


if __name__ == "__main__":
    result = try_zeppelin_api()
    if isinstance(result, tuple) and result[0]:
        success, session, notes = result
        create_note_and_run(session)
    else:
        print("\n[RESULT] Zeppelin REST API browser session se accessible nahi hai.")
        print("Manually Zeppelin open karo aur yeh SQLs run karo:")
        print("\n--- SQL 1: ecomm_events table ---")
        print(TABLE_DDL)
        print("\n--- SQL 2: ecomm_alerts_sink table ---")
        print(SINK_DDL)
        print("\n--- SQL 3: Anomaly INSERT ---")
        print(ANOMALY_INSERT)
