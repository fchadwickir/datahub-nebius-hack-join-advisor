import os, time, sqlite3, requests, json

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
DB_PATH = "/Users/franciscachadwick/static-assets/datasets/olist-ecommerce/olist_dirty.db"
POLL_INTERVAL = 10

def get_all_datasets():
    datasets = {}
    try:
        resp = requests.get(DATAHUB_URL + "/openapi/v3/entity/dataset?count=50")
        for e in resp.json().get("entities", []):
            urn = e.get("urn", "")
            try:
                name = urn.split(",")[1].split(".")[-1]
                cols = []
                try:
                    cols = [f["fieldPath"] for f in e["schemaMetadata"]["value"]["fields"]]
                except:
                    pass
                datasets[name] = {"urn": urn, "columns": cols}
            except:
                pass
    except Exception as ex:
        print("Error: " + str(ex))
    return datasets

def write_to_datahub(urn, props):
    payload = {
        "proposal": {
            "entityType": "dataset", "entityUrn": urn,
            "aspectName": "datasetProperties", "changeType": "UPSERT",
            "aspect": {"contentType": "application/json", "value": json.dumps({"customProperties": props})}
        }
    }
    r = requests.post(DATAHUB_URL + "/aspects?action=ingestProposal",
        headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}, json=payload)
    return r.status_code == 200

def analyze_new_dataset(name, cols, all_datasets, urn):
    print("\n" + "=" * 50)
    print("NEW DATASET DETECTED: " + name)
    print("Columns: " + ", ".join(cols) if cols else "Columns: (fetching from schema)")
    print("=" * 50)

    other = {k: v["columns"] for k, v in all_datasets.items() if k != name and not k.startswith("v_") and v["columns"]}
    schema_text = "\n".join([t + ": " + ", ".join(c) for t, c in other.items()])

    print("Asking Nebius to find joinable pairs...")
    prompt = ("New dataset added: " + name + " with columns: " + ", ".join(cols) + "\n\n"
              "Existing datasets:\n" + schema_text + "\n\n"
              "Which existing tables can join with this new table? "
              "For each: table name, join column, SAFE/UNSAFE, reason. Be concise.")

    resp = requests.post(
        "https://api.studio.nebius.com/v1/chat/completions",
        headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
        json={"model": "meta-llama/Llama-3.3-70B-Instruct",
              "messages": [{"role": "user", "content": prompt}], "max_tokens": 500}
    )
    analysis = resp.json()["choices"][0]["message"]["content"]
    print("Nebius says:\n" + analysis)

    props = {
        "auto_discovered": "true",
        "discovery_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "nebius_join_analysis": analysis[:400]
    }
    ok = write_to_datahub(urn, props)
    print("Written to DataHub!" if ok else "Failed to write")
    print("=" * 50)

def watch():
    print("Level 4: DataHub Watcher - Always On (FIXED)")
    print("Polling every " + str(POLL_INTERVAL) + " seconds...")
    print("Press Ctrl+C to stop\n")

    known = get_all_datasets()
    print("Tracking " + str(len(known)) + " datasets:")
    for name in known:
        print("  - " + name)
    print("\nWatching for new datasets...\n")

    while True:
        time.sleep(POLL_INTERVAL)
        current = get_all_datasets()
        new_tables = set(current.keys()) - set(known.keys())
        if new_tables:
            for table in new_tables:
                analyze_new_dataset(table, current[table]["columns"], current, current[table]["urn"])
        else:
            print("[" + time.strftime("%H:%M:%S") + "] Watching " + str(len(current)) + " datasets — no new ones...")
        known = current

if __name__ == "__main__":
    watch()
