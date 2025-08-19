# import teradatasql
# import pandas as pd

# TD_HOST="iteration7-w9og53takluu3v27.env.clearscape.teradata.com"
# TD_USER="demo_user"
# TD_PASS="n8888888"

# LABELS_CLAUSE = """
# Labels(
#  'Condiments, Dressings & Marinades','Furniture','Personal care, skin & body care','null',
#  'Tea, Coffee & Hot Drinks','Sweets & Desserts','Hair, Shower, Bath & Soap','Fruits',
#  'Nuts, Dates & Dried Fruits','Vegetables & Fruits','Home Appliances',
#  'Sauces, Dressings & Condiments','Baby Care','Tea and Coffee','Disposables & Napkins',
#  'Tins, Jars & Packets','Chips & Crackers','Soft Drinks & Juices','Cooking Ingredients',
#  'Dairy & Eggs','Bakery','Vegetables & Herbs','Biscuits & Cakes','Candles & Air Fresheners',
#  'Water','Rice, Pasta & Pulses','Poultry','Beef & Processed Meat','Home Textile',
#  'Cleaning Supplies','Beef & Lamb Meat','Chocolates, Sweets & Desserts','Jams, Spreads & Syrups'
# )
# """

# with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
#     cur = con.cursor()

#     # 0) (Optional) sanity probe of column names
#     probe = """
#     SEL TableName, ColumnId, ColumnName
#     FROM DBC.ColumnsV
#     WHERE DatabaseName='demo_user'
#       AND TableName IN ('cleaned_final_with_id','original_labels_lookup','original_label_predictions_fc')
#     ORDER BY TableName, ColumnId;
#     """
#     print(pd.read_sql(probe, con))

#     # 1) Drop & create results table explicitly (avoid CTAS parser edge cases)
#     try:
#         cur.execute("DROP TABLE demo_user.results;")
#     except Exception as e:
#         if "3807" not in str(e):
#             raise

#     cur.execute("""
#     CREATE MULTISET TABLE demo_user.results
#     (
#       actual_class    VARCHAR(512),
#       predicted_class VARCHAR(512)
#     );
#     """)
#     print("✅ Created demo_user.results")

#     # 2) Populate results via INSERT ... SELECT (no derived-table aliases)
#     # insert_sql = """
#     # INSERT INTO demo_user.results (actual_class, predicted_class)
#     # SELECT
#     #   cf."class"      AS actual_class,
#     #   lkp."class"     AS predicted_class
#     # FROM demo_user.original_label_predictions_fc p
#     # JOIN demo_user.cleaned_final_with_id cf
#     #   ON cf.row_id = p.row_id
#     # JOIN demo_user.original_labels_lookup lkp
#     #   ON lkp.gpc_id = p.gpc_id;
#     # """

#     insert_sql = """
#     INSERT INTO demo_user.results (actual_class, predicted_class)
#     SELECT
#         cf."class"      AS actual_class,
#         lkp."class"     AS predicted_class
#     FROM demo_user.original_label_predictions_fc p
#     JOIN demo_user.cleaned_final_with_id cf
#         ON cf.row_id = p.row_id
#     JOIN demo_user.original_labels_lookup lkp
#         ON lkp.row_id = p.gpc_id;   -- NOTE: lookup uses row_id as the label id
#     """

#     cur.execute(insert_sql)
#     print(pd.read_sql("SEL COUNT(*) AS n FROM demo_user.results;", con))

#     # 3) Run evaluator with your explicit labels
#     eval_sql = f"""
#     SELECT * FROM TD_ClassificationEvaluator (
#        ON demo_user.results AS InputTable
#        OUT VOLATILE TABLE OutputTable(classification_metrics)
#        USING
#            ObservationColumn('actual_class')
#            PredictionColumn('predicted_class')
#            {LABELS_CLAUSE}
#     ) AS dt;
#     """
#     cur.execute(eval_sql)

# #     # 4) Fetch metrics
# #     df = pd.read_sql("SELECT * FROM classification_metrics ORDER BY metric_name, class_label;", con)

# # print("🔎 Classification metrics (head):")
# # print(df.head(20))


# # --- Fetch the volatile output table robustly (no ORDER BY first) ---
# df = pd.read_sql("SELECT * FROM classification_metrics;", con)

# print("✅ Got classification_metrics. Columns detected:")
# print(list(df.columns))

# # Try common column-name variants for sorting
# metric_cols = ["metric_name", "Metric", "metric"]
# label_cols  = ["class_label", "ClassLabel", "label", "Label"]
# value_cols  = ["value", "Value", "metric_value", "MetricValue"]

# metric_col = next((c for c in metric_cols if c in df.columns), None)
# label_col  = next((c for c in label_cols  if c in df.columns), None)

# if metric_col and label_col:
#     df = df.sort_values([metric_col, label_col])
# elif metric_col:
#     df = df.sort_values([metric_col])

# print("\n🔎 Head:")
# print(df.head(20))


import teradatasql
import pandas as pd

TD_HOST="iteration7-w9og53takluu3v27.env.clearscape.teradata.com"
TD_USER="demo_user"
TD_PASS="n8888888"

LABELS_CLAUSE = """
Labels(
 'Condiments, Dressings & Marinades','Furniture','Personal care, skin & body care','null',
 'Tea, Coffee & Hot Drinks','Sweets & Desserts','Hair, Shower, Bath & Soap','Fruits',
 'Nuts, Dates & Dried Fruits','Vegetables & Fruits','Home Appliances',
 'Sauces, Dressings & Condiments','Baby Care','Tea and Coffee','Disposables & Napkins',
 'Tins, Jars & Packets','Chips & Crackers','Soft Drinks & Juices','Cooking Ingredients',
 'Dairy & Eggs','Bakery','Vegetables & Herbs','Biscuits & Cakes','Candles & Air Fresheners',
 'Water','Rice, Pasta & Pulses','Poultry','Beef & Processed Meat','Home Textile',
 'Cleaning Supplies','Beef & Lamb Meat','Chocolates, Sweets & Desserts','Jams, Spreads & Syrups'
)
"""

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    cur = con.cursor()

    # 0) (Optional) sanity probe of column names
    probe = """
    SEL TableName, ColumnId, ColumnName
    FROM DBC.ColumnsV
    WHERE DatabaseName='demo_user'
      AND TableName IN ('cleaned_final_with_id','original_labels_lookup','original_label_predictions_fc')
    ORDER BY TableName, ColumnId;
    """
    print(pd.read_sql(probe, con))

    # 1) Drop & create results table explicitly (avoid CTAS parser edge cases)
    try:
        cur.execute("DROP TABLE demo_user.results;")
    except Exception as e:
        if "3807" not in str(e):
            raise

    cur.execute("""
    CREATE MULTISET TABLE demo_user.results
    (
      actual_class    VARCHAR(512),
      predicted_class VARCHAR(512)
    );
    """)
    print("✅ Created demo_user.results")

    # 2) Populate results via INSERT ... SELECT
    insert_sql = """
    INSERT INTO demo_user.results (actual_class, predicted_class)
    SELECT
        cf."class"      AS actual_class,
        lkp."class"     AS predicted_class
    FROM demo_user.original_label_predictions_fc p
    JOIN demo_user.cleaned_final_with_id cf
        ON cf.row_id = p.row_id
    JOIN demo_user.original_labels_lookup lkp
        ON lkp.row_id = p.gpc_id;   -- your lookup uses 'row_id' as the label id
    """
    cur.execute(insert_sql)
    print(pd.read_sql("SEL COUNT(*) AS n FROM demo_user.results;", con))

    # 3) Persist evaluator output via CTAS (no VOLATILE, no OUT TABLE syntax)
    try:
        cur.execute("DROP TABLE demo_user.classification_metrics;")
    except Exception as e:
        if "3807" not in str(e):
            raise

    eval_ctas = f"""
    CREATE MULTISET TABLE demo_user.classification_metrics AS
    (
      SELECT *
      FROM TD_ClassificationEvaluator (
         ON demo_user.results AS InputTable
         USING
             ObservationColumn('actual_class')
             PredictionColumn('predicted_class')
             {LABELS_CLAUSE}
      ) AS dt
    ) WITH DATA;
    """
    cur.execute(eval_ctas)
    print("✅ Created demo_user.classification_metrics")

    # 4) Fetch metrics (persistent table)
    df = pd.read_sql("SELECT * FROM demo_user.classification_metrics;", con)

print("✅ Columns:", list(df.columns))

# Optional: sort by best-guess column names if present
metric_cols = ["metric_name", "Metric", "metric"]
label_cols  = ["class_label", "ClassLabel", "label", "Label"]

metric_col = next((c for c in metric_cols if c in df.columns), None)
label_col  = next((c for c in label_cols  if c in df.columns), None)

if metric_col and label_col:
    df = df.sort_values([metric_col, label_col])
elif metric_col:
    df = df.sort_values([metric_col])

print("\n🔎 Head:")
print(df.head(20))