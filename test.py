import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB

TABLE = f"{TD_DB}.gpc_labels_fc"

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    cur = con.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE}")
    count = cur.fetchone()[0]

print(f"Rows in {TABLE}: {count}")

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    cur = con.cursor()
    cur.execute(f"SELECT * FROM {TABLE} SAMPLE 5")
    for row in cur.fetchall():
        print(row)
