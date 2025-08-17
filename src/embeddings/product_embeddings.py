# import pandas as pd
# import numpy as np
# from sentence_transformers import SentenceTransformer


# df = pd.read_csv("data/train_val.csv")   # or load from ClearScape
# texts = ("query: " + df["Item_Name"].fillna("").astype(str)).tolist()

# model = SentenceTransformer("intfloat/e5-large-v2")
# model.max_seq_length = 512

# emb = model.encode(
#     texts,
#     convert_to_numpy=True,
#     show_progress_bar=True,
#     normalize_embeddings=True
# ).astype(np.float32)

# df["embedding_dim"] = emb.shape[1]
# df["embedding"] = [v.tolist() for v in emb]

# df.to_parquet("outputs/train_embeddings.csv", index=False)
# print("Wrote product embeddings for", len(df), "rows (dim", emb.shape[1], ")")
