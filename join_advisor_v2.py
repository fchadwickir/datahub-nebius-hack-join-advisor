import os
import requests
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.metadata.schema_classes import DatasetPropertiesClass

DATAHUB_URL = "http://localhost:8080"

# Known joins from Nebius analysis
joins = [
    {"table_a": "olist_orders", "table_b": "olist_customers", "column": "customer_id", "safety": "SAFE", "reason": "FK relationship"},
    {"table_a": "olist_orders", "table_b": "olist_order_items", "column": "order_id", "safety": "SAFE", "reason": "one order many items"},
    {"table_a": "olist_orders", "table_b": "olist_order_payments", "column": "order_id", "safety": "SAFE", "reason": "one order many payments"},
    {"table_a": "olist_orders", "table_b": "olist_order_reviews", "column": "order_id", "safety": "SAFE", "reason": "use LEFT JOIN"},
    {"table_a": "olist_order_items", "table_b": "olist_products", "column": "product_id", "safety": "SAFE", "reason": "FK relationship"},
    {"table_a": "olist_order_items", "table_b": "olist_sellers", "column": "seller_id", "safety": "SAFE", "reason": "FK relationship"},
    {"table_a": "olist_products", "table_b": "product_category_name_translation", "column": "product_category_name", "safety": "SAFE", "reason": "translation lookup"},
]

emitter = DatahubRestEmitter(gms_server=DATAHUB_URL)

print("Writing join recommendations to DataHub...")
for join in joins:
    urn = make_dataset_urn(platform="sqlite", name=f"olist_source.main.{join['table_a']}", env="PROD")
    properties = DatasetPropertiesClass(
        customProperties={
            f"join_with_{join['table_b']}": f"{join['column']} | {join['safety']} | {join['reason']}"
        }
    )
    from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
    from datahub.metadata.schema_classes import DatasetSnapshotClass, MetadataChangeEventClass
    snapshot = DatasetSnapshotClass(urn=urn, aspects=[properties])
    mce = MetadataChangeEventClass(proposedSnapshot=snapshot)
    emitter.emit_mce(mce)
    print(f"  ✓ {join['table_a']} + {join['table_b']} → {join['column']}")

print("\n✅ Level 2 complete! Open DataHub → click olist_orders → Properties tab to see joins.")
