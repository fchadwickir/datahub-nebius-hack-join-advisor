import os
import time
import sqlite3
import requests
import json

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
DB_PATH = "/Users/franciscachadwick/static-assets/datasets/olist-ecommerce/olist_dirty.db"
POLL_INTERVAL = 10

KNOWN_JOINS = {
    "olist_orders": [
        ("olist_customers", "customer_id", "customer_id"),
        ("olist_order_items", "order_id", "order_id"),
        ("olist_order_payments", "order_id", "order_id"),
    ],
    "olist_order_items": [
        ("olist_products", "product_id", "product_id"),
        ("olist_sellers", "seller_id", "seller_id"),
    ],
}

def get_all_datasets():
    datasets = {}
    try:
        resp = requests.get(DATAHUB_URL + "/openapi/v3/entity/dataset?count=50")
        for e in resp.json().get("entities", []):
            try:
                name = e["datasetProperties"]["value"]["name"]
                urn = e["urn"]
                cols = [f["fieldPath"] for f in e["schemaMetadata"]["value"]["fields"]]
                datasets[name] = {"urn": urn, "columns": cols}
            except:
                pass
    except Exception as ex:
        print("Error fetching datasets: " + str(ex))
    return datasets

def test_join(table_a, table_b, col_a, col_b):
    try:
        conn = sqlite3.connect(DB_PATH)
        distinct_a = conn.execute("SELECT COUNT(DISTINCT " + col_a + ") FROM " + table_a).fetchone()[0]
        matched = conn.execute("SELECT COUNT(DISTINCT a." + col_a + ") FROM " + table_a + " a INNER JOIN " + table_b + " b ON a." + col_a + " = b." + col_b).fetchone()[0]
        orphans = conn.execute("SELECT COUNT(*) FROM " + table_a + " a LEFT JOIN " + table_b + " b ON a." + col_a + " = b." + col_b + " WHERE b." + col_b + " IS NULL").fetchone()[0]
        max_per_key = conn.execute("SELECT MAX(cnt) FROM (SELECT " + col_a + ", COUNT(*) as cnt FROM " + table_a + " GROUP BY " + col_a + ")").fetchone()[0]
        conn.close()
        overlap = round(matched / distinct_a * 100, 1) if distinct_a > 0 else 0
        cardinality = "1:1" if max_per_key == 1 else "1:many"
        sql = "SELECT * FROM " + table_a + " a JOIN " + table_b + " b ON a." + col_a + " = b." + col_b + ";"
        return {"overlap": overlap, "orphans": orphans, "cardinality": cardinality, "sql": sql}
    except Exception as ex:
        return {"error": str(ex)}

def ask_nebius_about_new_dataset(new_table, new_cols, all_datasets):
    other_tables = {k: v["columns"] for k, v in all_datasets.items() if k != new_table and not k.startswith("v_")}
    schema_text = "\n".join([t + ": " + ", ".join(c) for t, c in other_tables.items()])
    prompt = ("A new dataset was added to DataHub: '" + new_table + "' with columns: " + ", ".join(new_cols) + "\n\n"
              "Existing datasets:\n" + schema_text + "\n\n"
              "Which existing tables can this new table be joined with? For each candidate join give:\n"
              "- table name\n- join column\n- safety (SAFE/RISKY/UNSAFE)\n- reason\n\nBe concise.")
    resp = requests.post(
        "https://api.studio.nebius.com/v1/chat/completions",
        headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
        json={"model": "meta-llama/Llama-3.3-70B-Instruct",
              "messages": [{"role": "user", "content": prompt}],
              "max_tokens": 800}
    )
    return resp.json()["choices"][0]["message"]["content"]

def write_to_datahub(urn, props):
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

def analyze_new_dataset(new_table, new_cols, all_datasets, urn):
    print("\n" + "=" * 50)
    print("NEW DATASET DETECTED: " + new_table)
    print("Columns: " + ", ".join(new_cols))
    print("=" * 50)

    print("Step 1: Asking Nebius to identify join candidates...")
    nebius_analysis = ask_nebius_about_new_dataset(new_table, new_cols, all_datasets)
    print("Nebius says:\n" + nebius_analysis)

    print("\nStep 2: Testing known joins against SQLite...")
    props = {
        "auto_discovered": "true",
        "discovery_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "nebius_analysis": nebius_analysis[:500]
    }

    if new_table in KNOWN_JOINS:
        for table_b, col_a, col_b in KNOWN_JOINS[new_table]:
            result = test_join(new_table, table_b, col_a, col_b)
            if "error" not in result:
                safety = "SAFE" if result["overlap"] > 90 else "RISKY" if result["overlap"] > 70 else "UNSAFE"
                print("  " + safety + " join with " + table_b + ": " + str(result["overlap"]) + "% overlap, " + str(result["orphans"]) + " orphans")
                props["join_" + table_b + "_overlap"] = str(result["overlap"]) + "%"
                props["join_" + table_b + "_cardinality"] = result["cardinality"]
                props["join_" + table_b + "_orphans"] = str(result["orphans"]) + " rows"
                props["join_" + table_b + "_sql"] = result["sql"]
                props["join_" + table_b + "_safety"] = safety

    print("\nStep 3: Writing join analysis to DataHub...")
    ok = write_to_datahub(urn, props)
    print("Written to DataHub!" if ok else "Failed to write to DataHub")
    print("=" * 50)

def watch():
    print("Level 4: DataHub Schema Watcher - Always On")
    print("Polling every " + str(POLL_INTERVAL) + " seconds for new datasets...")
    print("Press Ctrl+C to stop\n")

    known = get_all_datasets()
    print("Tracking " + str(len(known)) + " existing datasets:")
    for name in known:
        print("  - " + name)
    print("\nWatching for new datasets...\n")

    while True:
        time.sleep(POLL_INTERVAL)
        current = get_all_datasets()

        new_tables = set(current.keys()) - set(known.keys())
        if new_tables:
            for table in new_tables:
                analyze_new_dataset(
                    table,
                    current[table]["columns"],
                    current,
                    current[table]["urn"]
                )
        else:
            print("[" + time.strftime("%H:%M:%S") + "] No new datasets — watching...")

        known = current

if __name__ == "__main__":
    watch()
