# src/classification/similarity.py
import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

TOP_K = 5
DIM   = 1024
T_TRAIN = f"{TD_DB}.train_embeddings_fc"   # row_id, v1..v{DIM}
T_GPC   = f"{TD_DB}.gpc_labels_fc"         # gpc_id, v1..v{DIM}
T_OUT   = f"{TD_DB}.train_predictions_fc"  # row_id, gpc_id, score, pred_rank

# Feature column lists per docs
vcols = ",".join([f"'v{i}'" for i in range(1, DIM+1)])

sql = f"""
DELETE FROM {T_OUT};

INSERT INTO {T_OUT} (row_id, gpc_id, score, pred_rank)
SELECT
  row_id,
  gpc_id,
  1 - distance AS score,         -- cosine distance -> similarity
  d."rank"     AS pred_rank      -- quote reserved word
FROM TD_SYSFNLIB.TD_VectorDistance (
  ON {T_TRAIN}    AS TargetTable    PARTITION BY ANY
  ON {T_GPC}      AS ReferenceTable PARTITION BY ANY
  USING
    TargetIDColumn        ('row_id')
    TargetFeatureColumns  ({vcols})
    RefIDColumn           ('gpc_id')
    RefFeatureColumns     ({vcols})
    DistanceType          ('COSINE')   -- some pages use DistanceMeasure
    TopK                  ({TOP_K})
) AS d;
"""

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    con.cursor().execute(sql)

print(f"✅ Populated {T_OUT} with Top-{TOP_K} cosine similarity via TD_VectorDistance")
