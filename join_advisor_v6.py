import requests, os, sqlite3, json, time

NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
DB_PATH = "/Users/franciscachadwick/static-assets/datasets/olist-ecommerce/olist_dirty.db"
DATAHUB_URL = "http://localhost:8080"
MODEL = "meta-llama/Llama-3.3-70B-Instruct"

TABLES = ["olist_orders", "olist_customers", "olist_order_items", "olist_products", "olist_sellers", "olist_order_payments"]

def nebius(prompt, max_tokens=1000):
    resp = requests.post(
        "https://api.studio.nebius.com/v1/chat/completions",
        headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
        json={"model": MODEL, "messages": [{"role": "user", "content": prompt}], "max_tokens": max_tokens}
    )
    return resp.json()["choices"][0]["message"]["content"]

def write_datahub(table_name, props):
    urn = "urn:li:dataset:(urn:li:dataPlatform:sqlite,olist_source.main." + table_name + ",PROD)"
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

def get_schema(conn, table):
    cols = [row[1] for row in conn.execute("PRAGMA table_info(" + table + ")")]
    return cols

def test_join_pair(conn, table_a, table_b, col):
    try:
        distinct_a = conn.execute("SELECT COUNT(DISTINCT " + col + ") FROM " + table_a).fetchone()[0]
        matched = conn.execute("SELECT COUNT(DISTINCT a." + col + ") FROM " + table_a + " a JOIN " + table_b + " b ON a." + col + " = b." + col).fetchone()[0]
        orphans = conn.execute("SELECT COUNT(*) FROM " + table_a + " a LEFT JOIN " + table_b + " b ON a." + col + " = b." + col + " WHERE b." + col + " IS NULL").fetchone()[0]
        max_per = conn.execute("SELECT MAX(cnt) FROM (SELECT " + col + ", COUNT(*) cnt FROM " + table_a + " GROUP BY " + col + ")").fetchone()[0]
        overlap = round(matched / distinct_a * 100, 1) if distinct_a > 0 else 0
        return {"overlap": overlap, "orphans": orphans, "max_per_key": max_per}
    except:
        return None

# ═══════════════════════════════════════════════
print("=" * 60)
print("LEVEL 6: MULTI-AGENT JOIN ADVISOR")
print("DataHub = shared memory | Nebius = reasoning engine")
print("=" * 60)
print()

conn = sqlite3.connect(DB_PATH)
schemas = {t: get_schema(conn, t) for t in TABLES}

# ═══════════════════════════════════════════════
print("AGENT 1: DISCOVERY AGENT")
print("Finding join candidates across all tables...")
print("-" * 40)

schema_text = "\n".join([t + ": " + ", ".join(c) for t, c in schemas.items()])
discovery_prompt = (
    "You are a Discovery Agent. Find all possible join pairs between these tables:\n\n" +
    schema_text + "\n\n" +
    "Output ONLY lines in this exact format (no other text):\n" +
    "TABLE_A + TABLE_B | column\n\n" +
    "Example: olist_orders + olist_customers | customer_id\n\n" +
    "List all join candidates now:"
)
discovery_result = nebius(discovery_prompt, 500)
print("Discovery Agent found:")
print(discovery_result)

# Parse join candidates
join_candidates = []
for line in discovery_result.split("\n"):
    line = line.strip()
    if "+" in line and "|" in line:
        try:
            parts = line.split("|")
            tables_part = parts[0].strip()
            col = parts[1].strip()
            if "+" in tables_part:
                t1, t2 = [t.strip() for t in tables_part.split("+")]
                if t1 in TABLES and t2 in TABLES:
                    join_candidates.append((t1, t2, col))
        except:
            pass

print("\nParsed " + str(len(join_candidates)) + " join candidates")

# Write discovery results to DataHub (shared memory)
for t1, t2, col in join_candidates:
    write_datahub(t1, {"agent1_candidate_join_" + t2: col, "agent1_status": "discovered"})
print("Agent 1 wrote discoveries to DataHub")
print()

# ═══════════════════════════════════════════════
print("AGENT 2: VALIDATOR AGENT")
print("Testing each join candidate against real data...")
print("-" * 40)

validation_results = []
for t1, t2, col in join_candidates[:5]:  # test top 5
    result = test_join_pair(conn, t1, t2, col)
    if result:
        safety = "SAFE" if result["overlap"] > 90 else "RISKY" if result["overlap"] > 70 else "UNSAFE"
        print(safety + " " + t1 + " + " + t2 + " on " + col + " (" + str(result["overlap"]) + "% overlap, " + str(result["orphans"]) + " orphans)")
        validation_results.append({"t1": t1, "t2": t2, "col": col, "safety": safety, **result})
        # Write validation to DataHub
        write_datahub(t1, {
            "agent2_join_" + t2 + "_overlap": str(result["overlap"]) + "%",
            "agent2_join_" + t2 + "_orphans": str(result["orphans"]),
            "agent2_join_" + t2 + "_safety": safety,
            "agent2_status": "validated"
        })

conn.close()
print("\nAgent 2 wrote validation results to DataHub")
print()

# ═══════════════════════════════════════════════
print("AGENT 3: SQL GENERATOR AGENT")
print("Generating optimized JOIN SQL for validated pairs...")
print("-" * 40)

safe_joins = [r for r in validation_results if r["safety"] in ["SAFE", "RISKY"]]
validation_summary = "\n".join([
    r["t1"] + " + " + r["t2"] + " on " + r["col"] + ": " + r["safety"] + " (" + str(r["overlap"]) + "% overlap, " + str(r["orphans"]) + " orphans)"
    for r in validation_results
])

sql_prompt = (
    "You are a SQL Generator Agent. Generate optimized JOIN SQL for these validated pairs:\n\n" +
    validation_summary + "\n\n" +
    "For each SAFE or RISKY join, generate:\n" +
    "1. The JOIN SQL with proper handling for orphans (use LEFT JOIN where needed)\n" +
    "2. A WHERE clause filter if needed\n" +
    "3. One-line warning for RISKY joins\n\n" +
    "Be concise. Show SQL only, no long explanations."
)
sql_result = nebius(sql_prompt, 1000)
print("SQL Generator output:")
print(sql_result)

# Write SQL to DataHub
for r in safe_joins:
    write_datahub(r["t1"], {
        "agent3_join_" + r["t2"] + "_sql": "SELECT * FROM " + r["t1"] + " a " + ("JOIN" if r["safety"] == "SAFE" else "LEFT JOIN") + " " + r["t2"] + " b ON a." + r["col"] + " = b." + r["col"] + ";",
        "agent3_status": "sql_generated"
    })

print("\nAgent 3 wrote generated SQL to DataHub")
print()

# ═══════════════════════════════════════════════
print("=" * 60)
print("MULTI-AGENT SUMMARY")
print("=" * 60)
print("Agent 1 (Discovery):  Found " + str(len(join_candidates)) + " join candidates -> wrote to DataHub")
print("Agent 2 (Validator):  Tested " + str(len(validation_results)) + " pairs with real data -> wrote to DataHub")
print("Agent 3 (SQL Gen):    Generated SQL for " + str(len(safe_joins)) + " safe/risky joins -> wrote to DataHub")
print()
print("All agents coordinated via DataHub as shared memory.")
print("Level 6 complete!")
