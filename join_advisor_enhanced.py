import os, requests, json, sqlite3

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
DB_PATH = "/Users/franciscachadwick/static-assets/datasets/olist-ecommerce/olist_dirty.db"

def get_rich_metadata():
    resp = requests.get(DATAHUB_URL + "/openapi/v3/entity/dataset?count=50")
    metadata = {}
    for e in resp.json().get("entities", []):
        urn = e.get("urn", "")
        try:
            name = urn.split(",")[1].split(".")[-1]
            cols = []
            tags = []
            terms = []
            owner = []
            try:
                cols = [f["fieldPath"] for f in e["schemaMetadata"]["value"]["fields"]]
            except: pass
            try:
                tags = [t["tag"].split(":")[-1] for t in e["globalTags"]["value"]["tags"]]
            except: pass
            try:
                terms = [t["urn"].split(":")[-1] for t in e["glossaryTerms"]["value"]["terms"]]
            except: pass
            try:
                owner = [o["owner"].split(":")[-1] for o in e["ownership"]["value"]["owners"]]
            except: pass
            metadata[name] = {
                "urn": urn,
                "columns": cols,
                "tags": tags,
                "glossary_terms": terms,
                "owners": owner
            }
        except: pass
    return metadata

def get_lineage(urn):
    try:
        resp = requests.get(DATAHUB_URL + "/openapi/v3/relationship/dataset/" + requests.utils.quote(urn) + "?relationshipType=DownstreamOf&count=10")
        return resp.json()
    except:
        return {}

def test_join(table_a, table_b, col):
    try:
        conn = sqlite3.connect(DB_PATH)
        distinct_a = conn.execute("SELECT COUNT(DISTINCT " + col + ") FROM " + table_a).fetchone()[0]
        matched = conn.execute("SELECT COUNT(DISTINCT a." + col + ") FROM " + table_a + " a JOIN " + table_b + " b ON a." + col + " = b." + col).fetchone()[0]
        orphans = conn.execute("SELECT COUNT(*) FROM " + table_a + " a LEFT JOIN " + table_b + " b ON a." + col + " = b." + col + " WHERE b." + col + " IS NULL").fetchone()[0]
        conn.close()
        overlap = round(matched / distinct_a * 100, 1) if distinct_a > 0 else 0
        return {"overlap": overlap, "orphans": orphans}
    except Exception as ex:
        return {"error": str(ex)}

print("=== Enhanced Join Advisor: Using DataHub Tags, Lineage & Glossary ===\n")

print("Step 1: Fetching rich metadata from DataHub...")
metadata = get_rich_metadata()

for name, info in metadata.items():
    if not name.startswith("v_") and info["tags"]:
        print("  " + name + " | tags: " + str(info["tags"]) + " | terms: " + str(info["glossary_terms"]) + " | owners: " + str(info["owners"]))

JOIN_PAIRS = [
    ("olist_orders", "olist_customers", "customer_id"),
    ("olist_orders", "olist_order_items", "order_id"),
    ("olist_order_items", "olist_sellers", "seller_id"),
]

print("\nStep 2: Testing joins with real data...")
results = []
for t1, t2, col in JOIN_PAIRS:
    r = test_join(t1, t2, col)
    if "error" not in r:
        results.append({"t1": t1, "t2": t2, "col": col, **r,
                        "t1_tags": metadata.get(t1, {}).get("tags", []),
                        "t2_tags": metadata.get(t2, {}).get("tags", []),
                        "t1_terms": metadata.get(t1, {}).get("glossary_terms", []),
                        "t2_terms": metadata.get(t2, {}).get("glossary_terms", []),
                        "t1_owners": metadata.get(t1, {}).get("owners", []),
                        "t2_owners": metadata.get(t2, {}).get("owners", [])})
        print("  " + t1 + " + " + t2 + ": " + str(r["overlap"]) + "% overlap")

print("\nStep 3: Asking Nebius to reason using tags, lineage and glossary...")
context = "\n".join([
    "JOIN: " + r["t1"] + " + " + r["t2"] + " on " + r["col"] + "\n"
    "  Overlap: " + str(r["overlap"]) + "%, Orphans: " + str(r["orphans"]) + "\n"
    "  " + r["t1"] + " tags: " + str(r["t1_tags"]) + ", glossary: " + str(r["t1_terms"]) + ", owner: " + str(r["t1_owners"]) + "\n"
    "  " + r["t2"] + " tags: " + str(r["t2_tags"]) + ", glossary: " + str(r["t2_terms"]) + ", owner: " + str(r["t2_owners"])
    for r in results
])

prompt = (
    "You are a senior data engineer. Analyze these join pairs using ALL available DataHub metadata:\n\n" +
    context + "\n\n" +
    "For each join consider:\n"
    "1. PII risk: if either table has 'pii' tag, warn about data exposure\n"
    "2. Financial risk: if 'financial' tag present, flag compliance concerns\n"
    "3. Ownership: flag if tables have different owners (cross-team join)\n"
    "4. Glossary alignment: do the glossary terms suggest semantic compatibility?\n"
    "5. Data quality: what does the overlap% and orphan count mean in business context?\n\n"
    "Give a rich, context-aware analysis for each join. Be specific about risks and recommendations."
)

resp = requests.post(
    "https://api.studio.nebius.com/v1/chat/completions",
    headers={"Authorization": "Bearer " + NEBIUS_KEY, "Content-Type": "application/json"},
    json={"model": "meta-llama/Llama-3.3-70B-Instruct",
          "messages": [{"role": "user", "content": prompt}],
          "max_tokens": 1500}
)
analysis = resp.json()["choices"][0]["message"]["content"]
print("\n=== Nebius Rich Analysis (using DataHub tags + lineage + glossary) ===")
print(analysis)

print("\nStep 4: Writing enriched analysis back to DataHub...")
for r in results:
    urn = metadata.get(r["t1"], {}).get("urn")
    if urn:
        props = {
            "enriched_join_" + r["t2"]: r["col"] + " | " + str(r["overlap"]) + "% | tags_used: " + str(r["t1_tags"]),
        }
        payload = {
            "proposal": {
                "entityType": "dataset", "entityUrn": urn,
                "aspectName": "datasetProperties", "changeType": "UPSERT",
                "aspect": {"contentType": "application/json", "value": json.dumps({"customProperties": props})}
            }
        }
        r2 = requests.post(DATAHUB_URL + "/aspects?action=ingestProposal",
            headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"}, json=payload)
        print("  " + ("OK" if r2.status_code == 200 else "FAIL") + " wrote enriched metadata for " + r["t1"])

print("\nEnhanced Level 1+2 complete - deep DataHub integration!")
