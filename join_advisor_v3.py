import os, sqlite3, requests, json

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
DB_PATH = "/Users/franciscachadwick/static-assets/datasets/olist-ecommerce/olist_dirty.db"

JOIN_PAIRS = [
    ("olist_orders", "olist_customers", "customer_id", "customer_id"),
    ("olist_orders", "olist_order_items", "order_id", "order_id"),
    ("olist_orders", "olist_order_payments", "order_id", "order_id"),
    ("olist_order_items", "olist_products", "product_id", "product_id"),
    ("olist_order_items", "olist_sellers", "seller_id", "seller_id"),
]

def test_join(conn, table_a, table_b, col_a, col_b):
    total_a = conn.execute("SELECT COUNT(*) FROM " + table_a).fetchone()[0]
    total_b = conn.execute("SELECT COUNT(*) FROM " + table_b).fetchone()[0]
    distinct_a = conn.execute("SELECT COUNT(DISTINCT " + col_a + ") FROM " + table_a).fetchone()[0]
    distinct_b = conn.execute("SELECT COUNT(DISTINCT " + col_b + ") FROM " + table_b).fetchone()[0]
    matched = conn.execute("SELECT COUNT(DISTINCT a." + col_a + ") FROM " + table_a + " a INNER JOIN " + table_b + " b ON a." + col_a + " = b." + col_b).fetchone()[0]
    orphans_a = conn.execute("SELECT COUNT(*) FROM " + table_a + " a LEFT JOIN " + table_b + " b ON a." + col_a + " = b." + col_b + " WHERE b." + col_b + " IS NULL").fetchone()[0]
    max_per_key = conn.execute("SELECT MAX(cnt) FROM (SELECT " + col_a + ", COUNT(*) as cnt FROM " + table_a + " GROUP BY " + col_a + ")").fetchone()[0]
    overlap_pct = round(matched / distinct_a * 100, 1) if distinct_a > 0 else 0
    cardinality = "1:1" if max_per_key == 1 else "1:many (max " + str(max_per_key) + " per key)"
    type_a = [row[2] for row in conn.execute("PRAGMA table_info(" + table_a + ")") if row[1] == col_a]
    type_b = [row[2] for row in conn.execute("PRAGMA table_info(" + table_b + ")") if row[1] == col_b]
    type_a = type_a[0] if type_a else "unknown"
    type_b = type_b[0] if type_b else "unknown"
    type_ok = type_a.upper() == type_b.upper()
    if type_ok:
        sql = "SELECT * FROM " + table_a + " a JOIN " + table_b + " b ON a." + col_a + " = b." + col_b + ";"
    else:
        sql = "SELECT * FROM " + table_a + " a JOIN " + table_b + " b ON CAST(a." + col_a + " AS TEXT) = CAST(b." + col_b + " AS TEXT);"
    return {
        "table_a": table_a, "table_b": table_b, "col_a": col_a, "col_b": col_b,
        "total_a": total_a, "total_b": total_b, "distinct_a": distinct_a, "distinct_b": distinct_b,
        "matched": matched, "overlap_pct": overlap_pct, "orphans_a": orphans_a,
        "cardinality": cardinality, "type_a": type_a, "type_b": type_b, "type_ok": type_ok, "sql": sql
    }

def write_to_datahub(table_name, props):
    resp = requests.get(DATAHUB_URL + "/openapi/v3/entity/dataset?count=20")
    urn = None
    for e in resp.json().get("entities", []):
        try:
            if e["datasetProperties"]["value"]["name"] == table_name:
                urn = e["urn"]
                break
        except:
            pass
    if not urn:
        return False
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
    return r.status_code == 200

print("Level 3: Cross-Source Join Advisor")
print("=" * 50)
conn = sqlite3.connect(DB_PATH)
results = []

print("Step 1: Testing joins in olist_dirty.db...")
for table_a, table_b, col_a, col_b in JOIN_PAIRS:
    r = test_join(conn, table_a, table_b, col_a, col_b)
    results.append(r)
    icon = "OK  " if r["overlap_pct"] > 90 else "WARN" if r["overlap_pct"] > 70 else "FAIL"
    print(icon + " " + table_a + " + " + table_b)
    print("   Overlap: " + str(r["overlap_pct"]) + "% (" + str(r["matched"]) + " of " + str(r["distinct_a"]) + " keys match)")
    print("   Cardinality: " + r["cardinality"])
    print("   Types: " + r["type_a"] + " vs " + r["type_b"] + " - " + ("OK" if r["type_ok"] else "MISMATCH!"))
    print("   Orphans in A: " + str(r["orphans_a"]) + " rows")
    print("   SQL: " + r["sql"])
    print()

conn.close()

print("Step 2: Asking Nebius to analyze results...")
summary = json.dumps(results, indent=2)
prompt = "You are a data engineering expert. I tested these joins in olist_dirty.db (which has planted data issues):\n" + summary + "\n\nFor each join give: safety rating (SAFE/RISKY/UNSAFE), key insight in 1 sentence, and any warnings about orphans, cardinality, or type issues. Be concise and actionable."
resp = requests.post(
    "https://api.studio.nebius.com/v1/chat/completions",
    headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
    json={"model": "meta-llama/Llama-3.3-70B-Instruct", "messages": [{"role": "user", "content": prompt}], "max_tokens": 1500}
)
analysis = resp.json()["choices"][0]["message"]["content"]
print("\nNebius Analysis:")
print("=" * 50)
print(analysis)

print("\nStep 3: Writing results back to DataHub...")
table_props = {}
for r in results:
    t = r["table_a"]
    if t not in table_props:
        table_props[t] = {}
    table_props[t]["join_" + r["table_b"] + "_overlap"] = str(r["overlap_pct"]) + "%"
    table_props[t]["join_" + r["table_b"] + "_cardinality"] = r["cardinality"]
    table_props[t]["join_" + r["table_b"] + "_orphans"] = str(r["orphans_a"]) + " rows"
    table_props[t]["join_" + r["table_b"] + "_sql"] = r["sql"]

for table, props in table_props.items():
    ok = write_to_datahub(table, props)
    print(("OK " if ok else "FAIL") + " Wrote " + str(len(props)) + " properties to " + table)

print("\nLevel 3 complete!")
