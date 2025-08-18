import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

ITEM_EMBEDDINGS_TABLE = f"{TD_DB}.train_embeddings_fc"  
GPC_EMBEDDINGS_TABLE  = f"{TD_DB}.gpc_labels_fc"         
RESULT_TABLE          = f"{TD_DB}.train_predictions_fc" 

vector_cols        = ", ".join([f"v{i}" for i in range(1, 1024 + 1)])
vector_cols_quoted = ", ".join([f"'v{i}'" for i in range(1, 1024 + 1)])

sql = f"""
DELETE FROM {RESULT_TABLE};

INSERT INTO {RESULT_TABLE} (row_id, gpc_id, score)
SELECT row_id, gpc_id, score
FROM (
  SELECT
    o.Target_ID    AS row_id,
    o.Reference_ID AS gpc_id,
    1 - o.Distance AS score,  -- cosine distance -> similarity
    ROW_NUMBER() OVER (PARTITION BY o.Target_ID ORDER BY o.Distance ASC) AS rn
  FROM TD_SYSFNLIB.TD_VectorDistance (
    ON (SELECT row_id, {vector_cols} FROM {ITEM_EMBEDDINGS_TABLE}) AS TargetTable
    ON (SELECT gpc_id,  {vector_cols} FROM {GPC_EMBEDDINGS_TABLE})  AS ReferenceTable DIMENSION
    USING
      TargetIDColumn       ('row_id')
      RefIDColumn          ('gpc_id')
      TargetFeatureColumns ({vector_cols_quoted})
      RefFeatureColumns    ({vector_cols_quoted})
      DistanceMeasure      ('cosine')
  ) AS o
) s
WHERE rn = 1;  -- change to <= K for top-K
"""

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    con.cursor().execute(sql)

print(f"✅ {RESULT_TABLE} refreshed with top-1 cosine similarity (in-DB TD_VectorDistance).")