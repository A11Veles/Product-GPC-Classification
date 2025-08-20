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

#     # 2) Populate results via INSERT ... SELECT
#     insert_sql = """
#     INSERT INTO demo_user.results (actual_class, predicted_class)
#     SELECT
#         cf."class"      AS actual_class,
#         lkp."class"     AS predicted_class
#     FROM demo_user.original_label_predictions_fc p
#     JOIN demo_user.cleaned_final_with_id cf
#         ON cf.row_id = p.row_id
#     JOIN demo_user.original_labels_lookup lkp
#         ON lkp.row_id = p.gpc_id;   -- your lookup uses 'row_id' as the label id
#     """
#     cur.execute(insert_sql)
#     print(pd.read_sql("SEL COUNT(*) AS n FROM demo_user.results;", con))

#     # 3) Persist evaluator output via CTAS (no VOLATILE, no OUT TABLE syntax)
#     try:
#         cur.execute("DROP TABLE demo_user.classification_metrics;")
#     except Exception as e:
#         if "3807" not in str(e):
#             raise

#     eval_ctas = f"""
#     CREATE MULTISET TABLE demo_user.classification_metrics AS
#     (
#       SELECT *
#       FROM TD_ClassificationEvaluator (
#          ON demo_user.results AS InputTable
#          USING
#              ObservationColumn('actual_class')
#              PredictionColumn('predicted_class')
#              {LABELS_CLAUSE}
#       ) AS dt
#     ) WITH DATA;
#     """
#     cur.execute(eval_ctas)
#     print("✅ Created demo_user.classification_metrics")

#     # 4) Fetch metrics (persistent table)
#     df = pd.read_sql("SELECT * FROM demo_user.classification_metrics;", con)

# print("✅ Columns:", list(df.columns))

# # Optional: sort by best-guess column names if present
# metric_cols = ["metric_name", "Metric", "metric"]
# label_cols  = ["class_label", "ClassLabel", "label", "Label"]

# metric_col = next((c for c in metric_cols if c in df.columns), None)
# label_col  = next((c for c in label_cols  if c in df.columns), None)

# if metric_col and label_col:
#     df = df.sort_values([metric_col, label_col])
# elif metric_col:
#     df = df.sort_values([metric_col])

# print("\n🔎 Head:")
# print(df.head(20))



# import teradatasql
# import pandas as pd

# TD_HOST="iteration7-w9og53takluu3v27.env.clearscape.teradata.com"
# TD_USER="demo_user"
# TD_PASS="n8888888"

# LABELS_IN_ORDER = [
#  'Condiments, Dressings & Marinades','Furniture','Personal care, skin & body care','null',
#  'Tea, Coffee & Hot Drinks','Sweets & Desserts','Hair, Shower, Bath & Soap','Fruits',
#  'Nuts, Dates & Dried Fruits','Vegetables & Fruits','Home Appliances',
#  'Sauces, Dressings & Condiments','Baby Care','Tea and Coffee','Disposables & Napkins',
#  'Tins, Jars & Packets','Chips & Crackers','Soft Drinks & Juices','Cooking Ingredients',
#  'Dairy & Eggs','Bakery','Vegetables & Herbs','Biscuits & Cakes','Candles & Air Fresheners',
#  'Water','Rice, Pasta & Pulses','Poultry','Beef & Processed Meat','Home Textile',
#  'Cleaning Supplies','Beef & Lamb Meat','Chocolates, Sweets & Desserts','Jams, Spreads & Syrups'
# ]

# # Reuse your string for TD_ClassificationEvaluator
# LABELS_CLAUSE = "Labels(" + ",".join(f"'{x}'" for x in LABELS_IN_ORDER) + ")\n"

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

#     # 1) Drop & create results (actual_class, predicted_class)
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

#     # Populate results via INSERT ... SELECT (note join on lkp.row_id = p.gpc_id per your schema)
#     insert_sql = """
#     INSERT INTO demo_user.results (actual_class, predicted_class)
#     SELECT
#         cf."class"      AS actual_class,
#         lkp."class"     AS predicted_class
#     FROM demo_user.original_label_predictions_fc p
#     JOIN demo_user.cleaned_final_with_id cf
#         ON cf.row_id = p.row_id
#     JOIN demo_user.original_labels_lookup lkp
#         ON lkp.row_id = p.gpc_id;
#     """
#     cur.execute(insert_sql)
#     print(pd.read_sql("SEL COUNT(*) AS n FROM demo_user.results;", con))

#     # 2) Build a canonical label catalog from LABELS_IN_ORDER (with normalization)
#     try:
#         cur.execute("DROP TABLE demo_user.label_catalog;")
#     except Exception as e:
#         if "3807" not in str(e):
#             raise

#     cur.execute("""
#     CREATE MULTISET TABLE demo_user.label_catalog
#     (
#       seqnum INTEGER,
#       label  VARCHAR(512),
#       norm   VARCHAR(512)
#     );
#     """)
#     # Insert rows (seqnum defines CLASS_1.. ordering)
#     for i, lab in enumerate(LABELS_IN_ORDER, start=1):
#         cur.execute(
#             "INSERT INTO demo_user.label_catalog (seqnum, label, norm) VALUES (?, ?, LOWER(TRIM(?)));",
#             (i, lab, lab)
#         )
#     print("✅ Built demo_user.label_catalog")

#     # 3) Map/normalize results to the catalog -> results_mapped
#     try:
#         cur.execute("DROP TABLE demo_user.results_mapped;")
#     except Exception as e:
#         if "3807" not in str(e):
#             raise

#     cur.execute("""
#     CREATE MULTISET TABLE demo_user.results_mapped AS
#     (
#       SELECT
#         ca.label AS actual_class,      -- canonical class name from the catalog
#         cp.label AS predicted_class    -- canonical class name from the catalog
#       FROM demo_user.results r
#       JOIN demo_user.label_catalog ca
#         ON ca.norm = LOWER(TRIM(r.actual_class))
#       JOIN demo_user.label_catalog cp
#         ON cp.norm = LOWER(TRIM(r.predicted_class))
#     ) WITH DATA;
#     """)
#     print(pd.read_sql("SEL COUNT(*) AS n FROM demo_user.results_mapped;", con))

#     # (Optional) show unmapped rows to debug mismatches (should be 0 ideally)
#     print("\n🔎 Unmapped actual_class (should be 0 rows):")
#     print(pd.read_sql("""
#       SEL actual_class, COUNT(*) n
#       FROM demo_user.results
#       WHERE LOWER(TRIM(actual_class)) NOT IN (SEL norm FROM demo_user.label_catalog)
#       GROUP BY 1 ORDER BY n DESC, 1;
#     """, con).head(10))

#     print("\n🔎 Unmapped predicted_class (should be 0 rows):")
#     print(pd.read_sql("""
#       SEL predicted_class, COUNT(*) n
#       FROM demo_user.results
#       WHERE LOWER(TRIM(predicted_class)) NOT IN (SEL norm FROM demo_user.label_catalog)
#       GROUP BY 1 ORDER BY n DESC, 1;
#     """, con).head(10))

#     # 4) Persist evaluator output via CTAS (run on results_mapped to ensure exact string match)
#     try:
#         cur.execute("DROP TABLE demo_user.classification_metrics;")
#     except Exception as e:
#         if "3807" not in str(e):
#             raise

#     eval_ctas = f"""
#     CREATE MULTISET TABLE demo_user.classification_metrics AS
#     (
#       SELECT *
#       FROM TD_ClassificationEvaluator (
#          ON demo_user.results_mapped AS InputTable
#          USING
#              ObservationColumn('actual_class')
#              PredictionColumn('predicted_class')
#              {LABELS_CLAUSE}
#       ) AS dt
#     ) WITH DATA;
#     """
#     cur.execute(eval_ctas)
#     print("✅ Created demo_user.classification_metrics")

#     # 5) Fetch metrics (persistent table)
#     df = pd.read_sql("SELECT * FROM demo_user.classification_metrics;", con)

# print("✅ Columns:", list(df.columns))

# # Optional: sort by best-guess column names if present
# metric_cols = ["metric_name", "Metric", "metric"]
# label_cols  = ["class_label", "ClassLabel", "label", "Label"]

# metric_col = next((c for c in metric_cols if c in df.columns), None)
# label_col  = next((c for c in label_cols  if c in df.columns), None)

# if metric_col and label_col:
#     df = df.sort_values([metric_col, label_col])
# elif metric_col:
#     df = df.sort_values([metric_col])

# print("\n🔎 Head:")
# print(df.head(20))


import pandas as pd
from sklearn.metrics import f1_score
import teradatasql

TD_HOST="iteration7-w9og53takluu3v27.env.clearscape.teradata.com"
TD_USER="demo_user"
TD_PASS="n8888888"

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    # Load all 3 tables
    gt    = pd.read_sql("SELECT row_id, class AS actual_class FROM demo_user.cleaned_final_with_id;", con)
    preds = pd.read_sql("SELECT row_id, gpc_id FROM demo_user.original_label_predictions_fc;", con)
    lkp   = pd.read_sql("SELECT row_id AS gpc_id, class AS predicted_class FROM demo_user.original_labels_lookup;", con)

# Join predictions with lookup to get predicted labels
preds = preds.merge(lkp, on="gpc_id", how="left")

# Join with ground truth
df = preds.merge(gt, on="row_id", how="inner")

print("✅ Joined shape:", df.shape)
print(df.head())

# Compute F1 (macro = treats all classes equally, weighted = weights by support)
y_true = df["actual_class"]
y_pred = df["predicted_class"]

print("F1 (macro):   ", f1_score(y_true, y_pred, average="macro"))
print("F1 (weighted):", f1_score(y_true, y_pred, average="weighted"))