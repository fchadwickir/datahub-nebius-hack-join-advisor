import os
import json
import requests

DATAHUB_URL = "http://localhost:8080"
NEBIUS_KEY = os.environ.get("NEBIUS_API_KEY")
NEBIUS_URL = "https://api.studio.nebius.com/v1/chat/completions"

# ── TOOLS ──────────────────────────────────────────────
def get_tables():
    resp = requests.get(f"{DATAHUB_URL}/openapi/v3/entity/dataset?count=20")
    entities = resp.json()["entities"]
    tables = []
    for e in entities:
        try:
            name = e["datasetProperties"]["value"]["name"]
            if not name.startswith("v_"):
                tables.append(name)
        except:
            pass
    return tables

def get_schema(table_name):
    resp = requests.get(f"{DATAHUB_URL}/openapi/v3/entity/dataset?count=20")
    for e in resp.json()["entities"]:
        try:
            name = e["datasetProperties"]["value"]["name"]
            if name == table_name:
                cols = [f["fieldPath"] for f in e["schemaMetadata"]["value"]["fields"]]
                return {"table": table_name, "columns": cols}
        except:
            pass
    return {"error": f"Table {table_name} not found"}

def write_join_to_datahub(table_a, table_b, join_column, safety, reason):
    resp = requests.get(f"{DATAHUB_URL}/openapi/v3/entity/dataset?count=20")
    urn = None
    for e in resp.json()["entities"]:
        try:
            if e["datasetProperties"]["value"]["name"] == table_a:
                urn = e["urn"]
                break
        except:
            pass
    if not urn:
        return {"error": f"URN not found for {table_a}"}
    
    payload = {
        "proposal": {
            "entityType": "dataset",
            "entityUrn": urn,
            "aspectName": "datasetProperties",
            "changeType": "UPSERT",
            "aspect": {
                "contentType": "application/json",
                "value": json.dumps({"customProperties": {
                    f"join_with_{table_b}": f"{join_column} | {safety} | {reason}"
                }})
            }
        }
    }
    r = requests.post(
        f"{DATAHUB_URL}/aspects?action=ingestProposal",
        headers={"Content-Type": "application/json", "X-RestLi-Protocol-Version": "2.0.0"},
        json=payload
    )
    if r.status_code == 200:
        return {"success": f"Wrote join: {table_a} + {table_b} on {join_column}"}
    return {"error": f"Failed: {r.status_code}"}

# ── TOOL DEFINITIONS FOR NEBIUS ────────────────────────
tools = [
    {
        "type": "function",
        "function": {
            "name": "get_tables",
            "description": "Get all base tables from DataHub",
            "parameters": {"type": "object", "properties": {}, "required": []}
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": "Get columns for a specific table from DataHub",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {"type": "string", "description": "The table name"}
                },
                "required": ["table_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "write_join_to_datahub",
            "description": "Write a join recommendation back to DataHub as a custom property",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_a": {"type": "string"},
                    "table_b": {"type": "string"},
                    "join_column": {"type": "string"},
                    "safety": {"type": "string", "enum": ["SAFE", "UNSAFE"]},
                    "reason": {"type": "string"}
                },
                "required": ["table_a", "table_b", "join_column", "safety", "reason"]
            }
        }
    }
]

def call_tool(name, args):
    print(f"  🔧 Calling tool: {name}({args})")
    if name == "get_tables":
        return get_tables()
    elif name == "get_schema":
        return get_schema(args["table_name"])
    elif name == "write_join_to_datahub":
        return write_join_to_datahub(args["table_a"], args["table_b"], args["join_column"], args["safety"], args["reason"])
    return {"error": "Unknown tool"}

# ── AGENT LOOP ─────────────────────────────────────────
def run_agent(user_command):
    print(f"\n🤖 Agent starting with command: '{user_command}'")
    messages = [
        {"role": "system", "content": "You are a data engineering agent. You have access to DataHub (a metadata platform) and can read table schemas and write join recommendations back. When asked to analyze joins, get the tables, get their schemas, identify safe joins, and write them back to DataHub. Be thorough but concise."},
        {"role": "user", "content": user_command}
    ]

    while True:
        resp = requests.post(
            NEBIUS_URL,
            headers={"Authorization": f"Bearer {NEBIUS_KEY}", "Content-Type": "application/json"},
            json={"model": "meta-llama/Llama-3.3-70B-Instruct", "messages": messages, "tools": tools, "max_tokens": 2000}
        )
        
        msg = resp.json()["choices"][0]["message"]
        finish_reason = resp.json()["choices"][0]["finish_reason"]
        messages.append(msg)

        if finish_reason == "tool_calls" and msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                tool_name = tc["function"]["name"]
                tool_args = json.loads(tc["function"]["arguments"])
                result = call_tool(tool_name, tool_args)
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(result)
                })
        else:
            content = msg.get("content", "")
            import re
            clean = re.sub(r'<think>.*?</think>', '', content, flags=re.DOTALL).strip()
            print(f"\n✅ Agent finished!\n\n{clean}")
            break

# ── RUN ────────────────────────────────────────────────
if __name__ == "__main__":
    run_agent("Analyze the olist e-commerce dataset in DataHub. Get all tables, identify which tables can be safely joined and on which columns, then write the top 3 most important join recommendations back to DataHub.")
