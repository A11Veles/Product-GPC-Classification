import pandas as pd
from teradataml import *
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

HOST, USER, PWD = TD_HOST, TD_USER, TD_PASS
DB, TABLE = TD_DB, "original_labels_fc"
CSV = "src/category_embeddings.csv" 

create_context(host=HOST, username=USER, password=PWD, logmech="TD2")

df = pd.read_csv(CSV)
feat = sorted([c for c in df.columns if c.startswith("v")], key=lambda x: int(x[1:]))
df = df.astype({"row_id": "int64", **{c: "float64" for c in feat}})

execute_sql(f"""CREATE MULTISET TABLE {DB}.{TABLE} (
  row_id BIGINT, {", ".join(f"{c} FLOAT" for c in feat)}
) PRIMARY INDEX (row_id);""")

copy_to_sql(df, table_name=TABLE, schema_name=DB, if_exists="replace",
            index=False, types={"row_id":"BIGINT", **{c:"FLOAT" for c in feat}})

print(f"Loaded {len(df)} rows into {DB}.{TABLE}.")
