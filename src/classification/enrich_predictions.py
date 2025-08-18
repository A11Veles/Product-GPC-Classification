# src/classification/attach_names_and_bricks_local.py
import numpy as np
import pandas as pd
import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

# Local files
PRODUCTS_CSV     = "data/train_val.csv"  
PRODUCT_NAME_COL = "Item_Name"       
GPC_CSV          = "outputs/gpc.csv"     
OUT_CSV          = "outputs/preds_with_names_and_bricks.csv"

RESULT_TABLE = f"{TD_DB}.train_predictions_fc"

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    preds = pd.read_sql(f"SELECT row_id, gpc_id, score FROM {RESULT_TABLE}", con)

prods = pd.read_csv(PRODUCTS_CSV)
prods["row_id"] = np.arange(1, len(prods) + 1, dtype=np.int64)
names = prods[["row_id", PRODUCT_NAME_COL]].rename(columns={PRODUCT_NAME_COL: "item_name"})


gpc = pd.read_csv(GPC_CSV)
gpc = gpc.rename(columns={
    "gpc_code": "gpc_id",
    "gpc_name": "brick_name",
    "BrickDefinition_Includes": "includes",
    "BrickDefinition_Excludes": "excludes",
})
gpc["gpc_id"] = gpc["gpc_id"].astype(int)

def build_def(row):
    inc = str(row.get("includes", "") or "").strip()
    exc = str(row.get("excludes", "") or "").strip()
    parts = []
    if inc: parts.append(f"Includes: {inc}")
    if exc: parts.append(f"Excludes: {exc}")
    return " | ".join(parts) if parts else None

gpc["brick_definition"] = gpc.apply(build_def, axis=1)
gpc_small = gpc[["gpc_id", "brick_name", "brick_definition"]]

merged = (
    preds.merge(names, on="row_id", how="left")
         .merge(gpc_small, on="gpc_id", how="left")
    )[["row_id", "item_name", "gpc_id", "brick_name", "brick_definition", "score"]]


merged.to_csv(OUT_CSV, index=False)
print(f"✅ Wrote {len(merged)} rows to {OUT_CSV}")
print(merged.head(10).to_string(index=False))