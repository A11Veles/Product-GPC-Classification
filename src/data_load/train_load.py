# src/data_load/train_load.py
# Minimal CSV -> Teradata append using VARBYTE stage, UTF-16LE bytes + FROM_BYTES(...,'UNICODE')

import math
import pandas as pd
import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

CSV_PATH = "data/train_val.csv"
TARGET   = f"{TD_DB}.train_data"

# Column caps (characters) from your DDL
CAPS = {
    "Item_Name": 255,
    "class": 100,
    "Brand": 150,
    "Weight": 50,
    "Size_of_units": 50,
    "Pack": 50,
    "Unit": 50,
}

# 1) Read CSV and align headers
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
df = df.rename(columns={
    "T.Price": "T_Price",
    "Number of units": "Number_of_units",
    "Size of units": "Size_of_units",
})
cols = ["Item_Name","class","Brand","Weight","Number_of_units","Size_of_units","Price","T_Price","Pack","Unit"]
df = df[cols].where(pd.notnull(df), None)

# 2) Coerce numerics safely
for c in ["Number_of_units","Price","T_Price"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

def is_nan(v):
    return v is None or (isinstance(v, float) and math.isnan(v))

def trim(s, cap):
    if s is None: return None
    s = str(s)
    return s[:cap] if len(s) > cap else s

def to_int(v):
    return None if is_nan(v) else int(float(v))

def to_float(v):
    return None if is_nan(v) else float(v)

# 3) Prepare parameter rows
#    - Trim text to column limits
#    - Encode text as UTF-16LE bytes (works with FROM_BYTES(...,'UNICODE'))
rows = []
for rec in df.to_dict(orient="records"):
    rec["Item_Name"]     = trim(rec["Item_Name"], CAPS["Item_Name"])
    rec["class"]         = trim(rec["class"], CAPS["class"])
    rec["Brand"]         = trim(rec["Brand"], CAPS["Brand"])
    rec["Weight"]        = trim(rec["Weight"], CAPS["Weight"])
    rec["Size_of_units"] = trim(rec["Size_of_units"], CAPS["Size_of_units"])
    rec["Pack"]          = trim(rec["Pack"], CAPS["Pack"])
    rec["Unit"]          = trim(rec["Unit"], CAPS["Unit"])

    rows.append((
        rec["Item_Name"].encode("utf-16le") if isinstance(rec["Item_Name"], str) else None,
        rec["class"].encode("utf-16le") if isinstance(rec["class"], str) else None,
        rec["Brand"].encode("utf-16le") if isinstance(rec["Brand"], str) else None,
        rec["Weight"].encode("utf-16le") if isinstance(rec["Weight"], str) else None,
        to_int(rec["Number_of_units"]),
        rec["Size_of_units"].encode("utf-16le") if isinstance(rec["Size_of_units"], str) else None,
        to_float(rec["Price"]),
        to_float(rec["T_Price"]),
        rec["Pack"].encode("utf-16le") if isinstance(rec["Pack"], str) else None,
        rec["Unit"].encode("utf-16le") if isinstance(rec["Unit"], str) else None,
    ))

# 4) Stage as VARBYTE then convert to UNICODE in DB
create_stage = """
CREATE VOLATILE TABLE stage_vb (
  Item_Name_vb     VARBYTE(64000),
  class_vb         VARBYTE(64000),
  Brand_vb         VARBYTE(64000),
  Weight_vb        VARBYTE(64000),
  Number_of_units  INTEGER,
  Size_of_units_vb VARBYTE(64000),
  Price            DECIMAL(10,2),
  T_Price          DECIMAL(10,2),
  Pack_vb          VARBYTE(64000),
  Unit_vb          VARBYTE(64000)
) ON COMMIT PRESERVE ROWS;
"""

ins_stage = "INSERT INTO stage_vb VALUES (?,?,?,?,?,?,?,?,?,?)"

merge_sql = f"""
INSERT INTO {TARGET}
( Item_Name, "class", Brand, Weight, Number_of_units, Size_of_units, Price, T_Price, Pack, Unit )
SELECT
  FROM_BYTES(Item_Name_vb,'UNICODE'),
  FROM_BYTES(class_vb,'UNICODE'),
  FROM_BYTES(Brand_vb,'UNICODE'),
  FROM_BYTES(Weight_vb,'UNICODE'),
  Number_of_units,
  FROM_BYTES(Size_of_units_vb,'UNICODE'),
  Price,
  T_Price,
  FROM_BYTES(Pack_vb,'UNICODE'),
  FROM_BYTES(Unit_vb,'UNICODE')
FROM stage_vb;
"""

drop_stage = "DROP TABLE stage_vb;"

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    cur = con.cursor()
    cur.execute(create_stage)
    cur.executemany(ins_stage, rows)   # stage all rows as bytes/numbers
    cur.execute(merge_sql)             # convert bytes -> UNICODE in DB and append
    cur.execute(drop_stage)            # cleanup
    con.commit()

print(f"✅ Appended {len(rows)} rows into {TARGET}")