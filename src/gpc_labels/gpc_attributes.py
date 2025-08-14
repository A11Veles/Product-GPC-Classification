import pandas as pd

input_path = "data/GPC as of May 2025 v20250509 GB.xlsx"
out_attributes = "outputs/gpc_attributes.csv"
out_values = "outputs/gpc_attribute_values.csv"

cols = [
    "BrickCode", "BrickTitle",
    "AttributeCode", "AttributeTitle", "AttributeDefinition",
    "AttributeValueCode", "AttributeValueTitle", "AttributeValueDefinition",
]
df = pd.read_excel(input_path, sheet_name="Schema", usecols=cols)

for c in cols:
    if c in df.columns:
        df[c] = df[c].fillna("").astype(str).str.strip()


attrs = df[df["AttributeCode"] != ""][
    ["BrickCode", "BrickTitle", "AttributeCode", "AttributeTitle", "AttributeDefinition"]
].drop_duplicates()
attrs = attrs.rename(columns={
    "BrickCode": "brick_code",
    "BrickTitle": "brick_title",
    "AttributeCode": "attribute_code",
    "AttributeTitle": "attribute_title",
    "AttributeDefinition": "attribute_definition",
})
attrs.to_csv(out_attributes, index=False)

vals = df[(df["AttributeCode"] != "") & (df["AttributeValueCode"] != "")][
    [
        "BrickCode", "BrickTitle",
        "AttributeCode", "AttributeTitle",
        "AttributeValueCode", "AttributeValueTitle", "AttributeValueDefinition",
    ]
].drop_duplicates()
vals = vals.rename(columns={
    "BrickCode": "brick_code",
    "BrickTitle": "brick_title",
    "AttributeCode": "attribute_code",
    "AttributeTitle": "attribute_title",
    "AttributeValueCode": "attribute_value_code",
    "AttributeValueTitle": "attribute_value_title",
    "AttributeValueDefinition": "attribute_value_definition",
})
vals.to_csv(out_values, index=False)

print(f"Saved {len(attrs)} attributes to {out_attributes}")
print(f"Saved {len(vals)} attribute values to {out_values}")
