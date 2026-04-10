import requests
import json
import os

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ["NEBIUS_API_KEY"]

# Step 1: Get all datasets and their schemas from DataHub
print("Fetching schemas from DataHub...")
resp = requests.get(f"{DATAHUB_URL}/openapi/v3/entity/dataset?count=20")
data = resp.json()

tables = {}
for entity in data["entities"]:
    name = entity["datasetProperties"]["value"]["name"]
    fields = entity["schemaMetadata"]["value"]["fields"]
    columns = [f["fieldPath"] for f in fields]
    tables[name] = columns
    print(f"  Found: {name} → {columns}")

# Step 2: Ask Nebius which columns can be joined
print("\nAsking Nebius to identify join candidates...")
schema_text = "\n".join([f"{t}: {', '.join(c)}" for t, c in tables.items()])

prompt = f"""You are a data analyst. Here are the tables and their columns from a Brazilian e-commerce dataset:

{schema_text}

Identify which tables can be joined and on which columns. For each join, say:
- Table A + Table B → join on column X
- Whether the join is safe (explain briefly)

Be concise."""

nebius_resp = requests.post(
    "https://api.studio.nebius.com/v1/chat/completions",
    headers={"Authorization": f"Bearer {NEBIUS_KEY}", "Content-Type": "application/json"},
    json={"model": "deepseek-ai/DeepSeek-R1-0528", "messages": [{"role": "user", "content": prompt}], "max_tokens": 1000}
)

result = nebius_resp.json()
answer = result["choices"][0]["message"]["content"]
print("\n=== JOIN ADVISOR REPORT ===")
print(answer)
