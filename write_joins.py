import requests
import json

DATAHUB_URL = "http://localhost:8080"

joins_by_table = {
    "olist_source.main.olist_orders": {
        "join_with_olist_customers": "customer_id | SAFE | FK relationship",
        "join_with_olist_order_items": "order_id | SAFE | one order many items",
        "join_with_olist_order_payments": "order_id | SAFE | one order many payments",
        "join_with_olist_order_reviews": "order_id | SAFE | use LEFT JOIN",
    },
    "olist_source.main.olist_order_items": {
        "join_with_olist_products": "product_id | SAFE | FK relationship",
        "join_with_olist_sellers": "seller_id | SAFE | FK relationship",
    },
    "olist_source.main.olist_products": {
        "join_with_product_category_name_translation": "product_category_name | SAFE | translation lookup",
    },
}

headers = {"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}

for table, props in joins_by_table.items():
    urn = f"urn:li:dataset:(urn:li:dataPlatform:sqlite,{table},PROD)"
    payload = {"proposal": {"entityType": "dataset", "entityUrn": urn, "aspectName": "datasetProperties", "changeType": "UPSERT", "aspect": {"contentType": "application/json", "value": json.dumps({"customProperties": props})}}}
    resp = requests.post(f"{DATAHUB_URL}/aspects?action=ingestProposal", headers=headers, json=payload)
    if resp.status_code == 200:
        print(f"✓ Wrote {len(props)} joins for {table.split(chr(46))[-1]}")
    else:
        print(f"✗ Failed: {resp.status_code} {resp.text[:100]}")

print("Done! Refresh DataHub olist_orders Properties tab")
