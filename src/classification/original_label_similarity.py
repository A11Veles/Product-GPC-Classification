import teradatasql
import pandas as pd
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

ITEM_EMB  = f"{TD_DB}.products_labels_fc"     # has: row_id, v1..vN
REF_EMB   = f"{TD_DB}.original_labels_fc"     # has: <some id>, v1..vN
RESULT    = f"{TD_DB}.original_label_predictions_fc"

def get_columns(con, db, table):
    sql = f"""
    SELECT ColumnName
    FROM DBC.ColumnsV
    WHERE DatabaseName = '{db}'
      AND TableName    = '{table.split('.')[-1]}'
    ORDER BY ColumnId
    """
    return pd.read_sql(sql, con)["ColumnName"].tolist()

def pick_ref_id(colnames):
    for c in ["gpc_id", "label_id", "orig_label_id", "id", "row_id"]:
        if c in colnames:
            return c
    raise ValueError(f"No suitable ID column in {REF_EMB}. Found: {colnames}")

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    cur = con.cursor()

    item_cols = get_columns(con, TD_DB, ITEM_EMB)
    ref_cols  = get_columns(con, TD_DB, REF_EMB)

    if "row_id" not in item_cols:
        raise ValueError(f"{ITEM_EMB} must contain 'row_id'. Found: {item_cols}")

    ref_id_col = pick_ref_id(ref_cols)  # could be 'row_id' in your case
    # output alias for the reference id — force a DIFFERENT name than 'row_id'
    ref_id_out = "orig_label_id"               # choose what you like: 'gpc_id'/'orig_label_id'/...

    # ensure shared v* features & consistent order
    item_feats = [c for c in item_cols if c.startswith("v")]
    ref_feats  = [c for c in ref_cols  if c.startswith("v")]
    feats      = sorted(set(item_feats).intersection(ref_feats),
                        key=lambda x: int(x[1:]) if x[1:].isdigit() else x)
    if not feats:
        raise ValueError("No shared v* feature columns across both tables.")

    vec_cols         = ", ".join(feats)
    vec_cols_quoted  = ", ".join(f"'{c}'" for c in feats)

    # (re)create result table – output columns: row_id, gpc_id, score
    try:
        cur.execute(f"DROP TABLE {RESULT};")
    except Exception:
        pass

    create_sql = f"""
    CREATE MULTISET TABLE {RESULT} AS
    (
      SELECT
        o.Target_ID    AS row_id,
        o.Reference_ID AS {ref_id_out},
        1 - o.Distance AS score
      FROM TD_SYSFNLIB.TD_VectorDistance
      (
        ON (SELECT row_id, {vec_cols} FROM {ITEM_EMB}) AS TargetTable
        ON (SELECT {ref_id_col}, {vec_cols} FROM {REF_EMB})  AS ReferenceTable DIMENSION
        USING
          TargetIDColumn       ('row_id')
          RefIDColumn          ('{ref_id_col}')
          TargetFeatureColumns ({vec_cols_quoted})
          RefFeatureColumns    ({vec_cols_quoted})
          DistanceMeasure      ('cosine')
      ) AS o
      QUALIFY ROW_NUMBER() OVER (PARTITION BY o.Target_ID ORDER BY o.Distance) = 1
    ) WITH DATA
    PRIMARY INDEX (row_id);
    """
    cur.execute(create_sql)

print(f"✅ Rebuilt {RESULT} with top‑1 cosine similarity. Columns: row_id, {ref_id_out}, score")
