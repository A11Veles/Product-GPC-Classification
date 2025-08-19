from sentence_transformers import SentenceTransformer
import pandas as pd
import numpy as np
import torch

INPUT_CSV  = "data/cleaned_test.csv"
OUTPUT_CSV = "outputs/ccleaned_test_embeddings.csv"
MODEL_NAME = "intfloat/multilingual-e5-large-instruct"

# Load data
df = pd.read_csv(INPUT_CSV)
texts = df["cleaned_text"].fillna("").astype(str).tolist()

# Load model
device = "cuda" if torch.cuda.is_available() else "cpu"
model = SentenceTransformer(MODEL_NAME, device=device)

# 👉 prefix as passage
texts_for_model = [f"passage: {t}" for t in texts]

# Embed
emb = model.encode(
    texts_for_model,
    batch_size=64,
    convert_to_numpy=True,
    normalize_embeddings=True,
    show_progress_bar=True
)
dim = emb.shape[1]

# Save
emb_cols = [f"v{i}" for i in range(1, dim + 1)]
out = pd.concat(
    [df.reset_index(drop=True), pd.DataFrame(emb, columns=emb_cols)], axis=1
)

if "row_id" not in out.columns:
    out.insert(0, "row_id", np.arange(1, len(out) + 1, dtype=np.int64))

out.to_csv(OUTPUT_CSV, index=False)


print(f"✅ Saved {len(out)} rows with {dim}-dim passage embeddings → {OUTPUT_CSV}")