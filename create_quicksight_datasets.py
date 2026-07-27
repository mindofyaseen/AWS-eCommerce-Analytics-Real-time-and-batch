"""
Run this AFTER enabling Athena access in QuickSight console:
  QuickSight Console -> top-right (person icon) -> Manage QuickSight
  -> Security & permissions -> Add or remove -> enable Amazon Athena + S3 bucket

Creates 4 datasets and 1 analysis.
"""
import boto3
import json

QS = boto3.client("quicksight", region_name="us-east-1")
ACCOUNT_ID = "989864147584"
DS_ID = "ecomm-athena-source"
USER_ARN = "arn:aws:quicksight:us-east-1:989864147584:user/default/YASEEN-CLI-NEW"

FULL_PERMS = [
    "quicksight:DescribeDataSet",
    "quicksight:DescribeDataSetPermissions",
    "quicksight:PassDataSet",
    "quicksight:DescribeIngestion",
    "quicksight:ListIngestions",
    "quicksight:UpdateDataSet",
    "quicksight:DeleteDataSet",
    "quicksight:CreateIngestion",
    "quicksight:CancelIngestion",
    "quicksight:UpdateDataSetPermissions",
]

VIEWS = [
    ("ds-unique-visitors", "Unique Visitors Per Day", "v_unique_visitors_daily"),
    ("ds-cart-abandonment", "Cart Abandonment Sessions", "v_cart_abandonment"),
    ("ds-top-categories",  "Top Categories by Hour",   "v_top_categories_hourly"),
    ("ds-brand-insights",  "Brand Marketing Insights", "v_brand_insights"),
]

def create_dataset(dataset_id, name, table_name):
    print(f"Creating dataset: {name} ...")
    try:
        resp = QS.create_data_set(
            AwsAccountId=ACCOUNT_ID,
            DataSetId=dataset_id,
            Name=name,
            ImportMode="DIRECT_QUERY",
            PhysicalTableMap={
                "main": {
                    "RelationalTable": {
                        "DataSourceArn": f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:datasource/{DS_ID}",
                        "Catalog": "AwsDataCatalog",
                        "Schema": "ecomm_flink_db",
                        "Name": table_name,
                        "InputColumns": [],
                    }
                }
            },
            Permissions=[{"Principal": USER_ARN, "Actions": FULL_PERMS}],
        )
        print(f"  Created: {resp['DataSetId']} status {resp['ResponseMetadata']['HTTPStatusCode']}")
        return resp["Arn"]
    except QS.exceptions.ResourceExistsException:
        print(f"  Already exists, skipping.")
        return f"arn:aws:quicksight:us-east-1:{ACCOUNT_ID}:dataset/{dataset_id}"


if __name__ == "__main__":
    arns = []
    for ds_id, name, table in VIEWS:
        arn = create_dataset(ds_id, name, table)
        arns.append(arn)

    print("\n4 QuickSight datasets ready!")
    print("Now open QuickSight console and create Analysis/Dashboard from these datasets:")
    for ds_id, name, _ in VIEWS:
        print(f"  - {name}  (id: {ds_id})")
    print("\nDashboard types to create:")
    print("  1. Line chart: day vs unique_visitors")
    print("  2. Pie/bar: added_to_cart=1 vs purchased=1 (abandonment rate)")
    print("  3. Heat map: category_code x hour_of_day x view_count")
    print("  4. Bar chart: brand x purchases, avg_purchase_price")
