import numpy as np, pandas as pd, teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

PARQUET = "gpc_bricks_embeddings.parquet"
TABLE = f"{TD_DB}.gpc_labels_fc"

df = pd.read_parquet(PARQUET)
dim = int(df["embedding_dim"].iloc[0])
cols = ", ".join(["gpc_id"] + [f"v{i}" for i in range(1, dim+1)])
q = f"INSERT INTO {TABLE} ({cols}) VALUES ({', '.join(['?']*(1+dim))})"

emb = np.vstack(df["embedding"])
gpc_ids = df["gpc_code"].astype(int).to_numpy()
rows = [tuple([int(gpc_ids[i]), *emb[i]]) for i in range(len(df))]

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    con.cursor().executemany(q, rows)

print(f"✅ Appended {len(rows)} rows to {TABLE}")
