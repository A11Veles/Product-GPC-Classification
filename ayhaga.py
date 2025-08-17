import teradatasql
from config.settings import TD_HOST, TD_USER, TD_PASS

def print_rows(title, rows, max_rows=200):
    print(f"\n=== {title} ===")
    if not rows:
        print("(no rows)")
        return
    for i, r in enumerate(rows):
        print(r)
        if i+1 >= max_rows:
            print(f"...({len(rows)-max_rows} more)")
            break

schemas = ["TD_SYSFNLIB", "TD_SYSFUNC_EXT", "SYSUIF", "SYSLIB"]
help_forms = [
    "HELP FUNCTION {s}.TD_VectorDistance;",
    "HELP TABLE OPERATOR {s}.TD_VectorDistance;",
    "HELP TABLE {s}.TD_VectorDistance;"
]

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    cur = con.cursor()

    # 1) Where is it and what kind is it?
    cur.execute("""
        SELECT DatabaseName, TableName, TableKind
        FROM dbc.TablesV
        WHERE UPPER(TableName) LIKE 'TD_VECTORDISTANCE%'
        ORDER BY 1,2;
    """)
    rows = cur.fetchall()
    print_rows("dbc.TablesV (location / kind)", rows)

    # 2) Try HELP in common forms until one works; show the raw signature text
    got_help = False
    for s in schemas:
        for form in help_forms:
            sql = form.format(s=s)
            try:
                cur.execute(sql)
                help_rows = cur.fetchall()
                print_rows(f"{sql.strip()} output", help_rows)
                schema_found = s
                got_help = True
                break
            except Exception:
                pass
        if got_help:
            break
    if not got_help:
        print("\n(No HELP variant succeeded — function may be missing or named differently here.)")

    # 3) Optional: try data dictionary views for table functions/operators (if present)
    # These may or may not exist on your system; failures are fine.
    try:
        cur.execute("""
            SELECT * FROM dbc.TVFInfoV
            WHERE UPPER(SpecificName) = 'TD_VECTORDISTANCE'
               OR UPPER(ExternalName) LIKE '%TD_VECTORDISTANCE%'
            ORDER BY 1,2;
        """)
        print_rows("dbc.TVFInfoV", cur.fetchall(), max_rows=200)
    except Exception:
        pass

    try:
        cur.execute("""
            SELECT * FROM dbc.TVFParamV
            WHERE UPPER(SpecificName) = 'TD_VECTORDISTANCE'
            ORDER BY ParamOrder;
        """)
        print_rows("dbc.TVFParamV (parameters)", cur.fetchall(), max_rows=500)
    except Exception:
        pass
