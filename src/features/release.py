import pandas as pd
import pathlib
import os

out         = "features_v1.csv"
featuresP   = pathlib.Path("data/features")
componentsP = pathlib.Path("data/features/components")

timeline = pd.date_range(start="2013-01-02", end="2025-05-29", freq="D")
features = pd.DataFrame(index=timeline)

seen  = {"date"}
files = os.listdir(componentsP)
for f in files:
    if not f.endswith(".csv"):
        continue

    df = pd.read_csv(componentsP / f, parse_dates=["date"]).set_index("date")
    for col in df.columns:
        if col in seen:
            continue

        # if col not in custom_feature_list: continue
        seen.add(col)
        features[col] = df[col]


max_gap        = 60
init_rows      = features.shape[0]

features = features.dropna()

features.to_csv(featuresP / out, index_label="date")

print(f"""
Succesfully saved features
Deleted rows with NA values: {init_rows - features.shape[0]}
Final shape: {features.shape[0]} rows and {features.shape[1]} columns(features)
""")