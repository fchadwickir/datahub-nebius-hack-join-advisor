import requests, os, sqlite3, json

NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
DB_PATH = "/Users/franciscachadwick/static-assets/datasets/olist-ecommerce/olist_dirty.db"
DATAHUB_URL = "http://localhost:8080"

conn = sqlite3.connect(DB_PATH)

orphan_statuses = conn.execute(
    "SELECT o.order_status, COUNT(*) as cnt FROM olist_orders o "
    "LEFT JOIN olist_customers c ON o.customer_id = c.customer_id "
    "WHERE c.customer_id IS NULL GROUP BY o.order_status ORDER BY cnt DESC"
).fetchall()

orphan_dates = conn.execute(
    "SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month, COUNT(*) as cnt "
    "FROM olist_orders o LEFT JOIN olist_customers c ON o.customer_id = c.customer_id "
    "WHERE c.customer_id IS NULL GROUP BY month ORDER BY cnt DESC LIMIT 5"
).fetchall()

seller_orphan_sample = conn.execute(
    "SELECT oi.seller_id, COUNT(*) as cnt FROM olist_order_items oi "
    "LEFT JOIN olist_sellers s ON oi.seller_id = s.seller_id "
    "WHERE s.seller_id IS NULL GROUP BY oi.seller_id LIMIT 5"
).fetchall()

conn.close()

print("Orphan order statuses:", orphan_statuses)
print("Orphan order months:", orphan_dates)
print("Orphan seller samples:", seller_orphan_sample)

prompt = (
    "You are a senior data engineer analyzing join quality in a Brazilian e-commerce dataset.\n\n"
    "FINDINGS FROM TESTING olist_dirty.db:\n\n"
    "1. olist_orders + olist_customers (join on customer_id):\n"
    "   - 92% key overlap - 7,955 orphan orders (orders with no matching customer)\n"
    "   - Orphan order statuses: " + str(orphan_statuses) + "\n"
    "   - Orphan orders by month: " + str(orphan_dates) + "\n"
    "   - Cardinality: 1:1\n\n"
    "2. olist_order_items + olist_sellers (join on seller_id):\n"
    "   - 71.8% key overlap - 5,632 orphan order items (items with no matching seller)\n"
    "   - Sample orphan seller IDs: " + str(seller_orphan_sample) + "\n"
    "   - Cardinality: 1:many (max 1911 items per seller)\n\n"
    "Reason holistically about each issue:\n"
    "- WHY do these orphans exist? Sync lag, data model issue, or intentional design?\n"
    "- Which analyses are SAFE despite the orphans?\n"
    "- Which analyses are RISKY because of the orphans?\n"
    "- What should an analyst do before running a join on these tables?\n\n"
    "Give specific, actionable business-context reasoning like a senior data engineer explaining to a junior analyst."
)

resp = requests.post(
    "https://api.studio.nebius.com/v1/chat/completions",
    headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
    json={"model": "meta-llama/Llama-3.3-70B-Instruct",
          "messages": [{"role": "user", "content": prompt}],
          "max_tokens": 1500}
)

analysis = resp.json()["choices"][0]["message"]["content"]
print()
print("=== Level 5: Holistic Reasoning ===")
print(analysis)

# Write back to DataHub
print("\nWriting holistic analysis to DataHub...")
urn = "urn:li:dataset:(urn:li:dataPlatform:sqlite,olist_source.main.olist_orders,PROD)"
props = {
    "level5_orphan_analysis": analysis[:500],
    "level5_orphan_statuses": str(orphan_statuses),
    "level5_orphan_months": str(orphan_dates),
    "level5_reasoning": "holistic"
}
payload = {
    "proposal": {
        "entityType": "dataset",
        "entityUrn": urn,
        "aspectName": "datasetProperties",
        "changeType": "UPSERT",
        "aspect": {
            "contentType": "application/json",
            "value": json.dumps({"customProperties": props})
        }
    }
}
r = requests.post(
    DATAHUB_URL + "/aspects?action=ingestProposal",
    headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"},
    json=payload
)
print("Written to DataHub!" if r.status_code == 200 else "Failed: " + str(r.status_code))
print("\nLevel 5 complete!")
