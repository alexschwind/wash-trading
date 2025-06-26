from preprocessing import preprocessing
from scc_algorithm import *
import os
if os.path.exists("data_preprocessed.csv"):
    print("Data already exists.")
    trades = pd.read_csv("data_preprocessed.csv", header=0)
    print("Data loaded.")
else:
    print("Preprocessing...")
    trades = preprocessing("data/IDEXTrades.csv")
    trades.to_csv("data_preprocessed.csv", index=False)
    print("Preprocessing done.")

print("Trades shape:", trades.shape)

print("Starting Parallel algorithm...")
scc_dt, relevant = scc_algo_parallel(trades.copy())
print("Parallel algorithm done.")

print(relevant)
print(relevant.shape)

print("Starting Sequential algorithm...")
scc_dt2, relevant2 = scc_algo_seq(trades.copy())
print("Sequential algorithm done.")

print(relevant2)

print("Starting Sequential algorithm...")
scc_dt3, relevant3 = scc_algo_seq_orig(trades.copy())
print("Sequential algorithm done.")

print(relevant3)