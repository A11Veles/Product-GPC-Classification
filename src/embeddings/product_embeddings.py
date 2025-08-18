import pandas as pd, numpy as np
from sentence_transformers import SentenceTransformer

from src.utils import load_embedding_model
from src.constants import E5_LARGE_INSTRUCT_CONFIG_PATH

IN_CSV  = "data/train_val.csv"           
TEXTCOL = "Item_Name"               
OUT_PARQUET = "outputs/new_train_embeddings.parquet"

model = load_embedding_model(E5_LARGE_INSTRUCT_CONFIG_PATH)

df = pd.read_csv(IN_CSV)
texts = ("passage: " + df[TEXTCOL].fillna("").astype(str)).tolist()

emb = model.encode(texts, convert_to_numpy=True, show_progress_bar=True, normalize_embeddings=True)
emb = emb.astype(np.float32)

df_out = pd.DataFrame({
    "row_id": np.arange(1, len(df)+1, dtype=np.int64),
    "item_name": df[TEXTCOL].astype(str),
    "embedding_dim": emb.shape[1],
    "embedding": [v.tolist() for v in emb]
})
df_out.to_parquet(OUT_PARQUET, index=False)
print(f"✅ wrote {OUT_PARQUET} rows={len(df_out)} dim={emb.shape[1]}")