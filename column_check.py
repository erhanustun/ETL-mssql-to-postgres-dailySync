import pandas as pd

df = pd.read_csv("orders.csv")
print(df.columns)
print(df["OrderCreatedAt"].isnull().sum())
print(df.shape)
