import pandas as pd
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB
import teradatasql

with teradatasql.connect(host=TD_HOST, user=TD_USER, password=TD_PASS) as con:
    preds = pd.read_sql(f"SELECT row_id, gpc_id, score FROM {TD_DB}.train_predictions_fc", con)

gpc = pd.read_csv("outputs/gpc.csv").rename(columns={"gpc_code":"gpc_id"})
gpc["gpc_id"] = gpc["gpc_id"].astype(int)

matched = preds["gpc_id"].isin(gpc["gpc_id"]).mean()
print(f"GPC ID coverage: {matched:.1%}")    # expect ~100%
