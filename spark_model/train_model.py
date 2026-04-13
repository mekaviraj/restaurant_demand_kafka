# spark_model/train_model.py
import pandas as pd
import json
import os
from statsmodels.tsa.arima.model import ARIMA
import pickle

# first create a sample file like this:
# kafka-console-consumer --bootstrap-server localhost:9092 --topic orders --from-beginning --timeout-ms 5000 > orders_sample.json

# then run this script

os.makedirs("models", exist_ok=True)

df = []
with open("orders_sample.json") as f:
    for line in f:
        if line.strip():
            df.append(json.loads(line))

df = pd.DataFrame(df)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df["hour"] = df["timestamp"].dt.hour
df["date"] = df["timestamp"].dt.date

grouped = df.groupby(["dish_id", "date", "hour"])["quantity"].sum().reset_index()

for dish in grouped["dish_id"].unique():
    sub = grouped[grouped["dish_id"] == dish].sort_values(["date", "hour"])
    if len(sub) < 3:
        continue
    model = ARIMA(sub["quantity"], order=(1,0,1)).fit()
    with open(f"models/dish_{dish}_model.pkl", "wb") as f:
        pickle.dump(model, f)