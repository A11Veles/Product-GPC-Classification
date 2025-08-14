import pandas as pd
import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB, TD_TRAIN, TD_EMB, TD_GPC

def run_query(sql, params=None):
    with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
        return pd.read_sql(sql, con, params=params)

# 1) Do the tables exist?
exists = run_query(
    """
    SELECT TableName
    FROM DBC.TablesV
    WHERE DatabaseName = ?
      AND TableName IN (?, ?, ?)
    ORDER BY TableName;
    """,
    (TD_DB, TD_TRAIN.split(".")[1], TD_EMB.split(".")[1], TD_GPC.split(".")[1])
)

# 2) How many rows are in train_data?
cnt = run_query(f"SELECT COUNT(*) AS n FROM {TD_TRAIN};")

# 3) Peek at a few rows
sample = run_query(f"SELECT TOP 5 row_id, Item_Name, Brand, class FROM {TD_TRAIN};")

# 4) Show schema
schema = run_query(
    """
    SELECT ColumnName, ColumnType, ColumnLength, Nullable
    FROM DBC.ColumnsV
    WHERE DatabaseName = ? AND TableName = ?
    ORDER BY ColumnId;
    """,
    (TD_DB, TD_TRAIN.split(".")[1])
)

# ---- DISPLAY ----
print("\n-- Tables present? --")
print(exists.to_string(index=False))

print("\n-- Row count in train_data --")
print(cnt.to_string(index=False))

print("\n-- Sample rows from train_data --")
print(sample.to_string(index=False))

print("\n-- Schema for train_data --")
print(schema.to_string(index=False))