import pandas as pd
from common.config import load_config, resolve

cfg = load_config()
df = pd.read_parquet(resolve(cfg["parquet_path"]))
print(df.shape)
print(df.dtypes)
print(df.head(3).T)
print("pickup range:", df["tpep_pickup_datetime"].min(), "->", df["tpep_pickup_datetime"].max())