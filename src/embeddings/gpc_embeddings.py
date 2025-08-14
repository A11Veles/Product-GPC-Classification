import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

df = pd.read_csv("outputs/gpc.csv")
texts = ("passage: " + df["combined_text_for_embedding"].fillna("").astype(str)).tolist()

model = SentenceTransformer("intfloat/e5-large-v2")
emb = model.encode(texts, convert_to_numpy=True, show_progress_bar=True, normalize_embeddings=True)
emb = emb.astype(np.float32)

# Save vectors as lists for Parquet
df["embedding_dim"] = emb.shape[1]
df["embedding"] = [v.tolist() for v in emb]

df.to_parquet("outputs/gpc_bricks_embeddings.parquet", index=False)
print("Wrote gpc_bricks_embeddings.parquet with", len(df), "rows and dim", emb.shape[1])