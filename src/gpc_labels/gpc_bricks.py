import pandas as pd

input_path = "data/GPC as of May 2025 v20250509 GB.xlsx"
output_path = "outputs/gpc.csv"

cols = [
    "SegmentTitle", "FamilyTitle", "ClassTitle", "BrickCode", "BrickTitle",
    "BrickDefinition_Includes", "BrickDefinition_Excludes"
]
df = pd.read_excel(input_path, sheet_name="Schema", usecols=cols)

df = df.fillna("").astype(str).apply(lambda col: col.str.strip())

df = df.drop_duplicates(subset=["BrickCode"])

df["gpc_path"] = (
    df["SegmentTitle"] + " > " +
    df["FamilyTitle"] + " > " +
    df["ClassTitle"] + " > " +
    df["BrickTitle"]
)

df["combined_text_for_embedding"] = df.apply(
    lambda r: f"{r['gpc_path']}:\n{r['BrickDefinition_Includes']}"
              + (f"\nExcludes: {r['BrickDefinition_Excludes']}" if r['BrickDefinition_Excludes'] else ""),
    axis=1
)

df.rename(columns={"BrickCode": "gpc_code", "BrickTitle": "gpc_name"}).to_csv(output_path, index=False)

print(f"Saved {len(df)} bricks to {output_path}")