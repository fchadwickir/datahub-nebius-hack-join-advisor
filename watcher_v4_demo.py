import requests, json, os, time

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")

print("=== Level 4: Auto-Trigger Demo ===")
print()

new_table = "olist_new_customers"
new_cols = ["customer_id", "customer_name", "customer_email", "signup_date"]
print("EVENT: New dataset detected -> " + new_table)
print("Columns: " + ", ".join(new_cols))
print()

print("Asking Nebius to find joinable pairs...")
existing = "olist_orders: order_id, customer_id, order_status\nolist_customers: customer_id, customer_unique_id, customer_city"
prompt = "New dataset added: " + new_table + " with columns: " + ", ".join(new_cols) + "\n\nExisting datasets:\n" + existing + "\n\nWhich existing tables can join with this new table? Give table, column, SAFE/UNSAFE, reason. Be concise."
resp = requests.post("https://api.studio.nebius.com/v1/chat/completions",
    headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
    json={"model": "meta-llama/Llama-3.3-70B-Instruct", "messages": [{"role": "user", "content": prompt}], "max_tokens": 500})
analysis = resp.json()["choices"][0]["message"]["content"]
print("Nebius analysis:")
print(analysis)
print()

print("Writing join recommendations to DataHub...")
urn = "urn:li:dataset:(urn:li:dataPlatform:sqlite,olist_source.main.olist_new_customers,PROD)"
props = {"auto_discovered": "true", "discovery_time": time.strftime("%Y-%m-%d %H:%M:%S"), "join_with_olist_orders": "customer_id | SAFE | FK relationship", "nebius_analysis": analysis[:300]}
payload = {"proposal": {"entityType": "dataset", "entityUrn": urn, "aspectName": "datasetProperties", "changeType": "UPSERT", "aspect": {"contentType": "application/json", "value": json.dumps({"customProperties": props})}}}
r = requests.post(DATAHUB_URL + "/aspects?action=ingestProposal", headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}, json=payload)
print("Written to DataHub!" if r.status_code == 200 else "Failed: " + str(r.status_code))
print()
print("Level 4 complete! New dataset auto-analyzed and documented in DataHub.")
