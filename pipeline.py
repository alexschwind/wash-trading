import pandas as pd
import numpy as np
import time

from preprocessing import preprocessing
from scc_algorithm import scc_algo_parallel
from volume_matching_algorithm import volume_matching_parallel_better, get_address_clusters, volume_matching_parallel_overlapping

start = time.time()

print("Preprocessing")
start_pre = time.time()
trades, global_trader_hashes = preprocessing("data/IDEXTrades.csv", filter_status=True)
end_pre = time.time()
print(f"Preprocessing Time: {(end_pre - start_pre)/60:.4f} minutes")

trades.to_csv("data_preprocessed.csv", index=False)
global_trader_hashes.to_csv("global_trader_hashes.csv", index=False)

# trades = pd.read_csv("data_preprocessed.csv", header=0)
# global_trader_hashes = pd.read_csv("global_trader_hashes.csv", header=0)




print("SCC algorithm")
start_scc = time.time()
scc_dt, relevant, global_scc_traders_map = scc_algo_parallel(trades.copy())
end_scc = time.time()
print(f"SCC Time: {end_scc - start_scc:.4f} seconds")

print("Relevant SCCs:", len(relevant))

print("Volume Matching algorithm")
start_vol = time.time()
trades, wash_trades_dict = volume_matching_parallel_better(trades, relevant, global_scc_traders_map)
end_vol = time.time()
print(f"Volume Matching Time: {(end_vol - start_vol)/60:.4f} minutes")

trades.to_csv("trades_wash_labeled.csv", index=False)

flagged = trades[trades['wash_label'] == True]
print("Wash trades detected:", flagged.shape[0])

print("Address Clusters")
start_cluster = time.time()
address_clusters = get_address_clusters(relevant, global_scc_traders_map, global_trader_hashes)
end_cluster = time.time()
print(f"Address Cluster Time: {end_cluster - start_cluster:.4f} seconds")

end = time.time()
print(f"Total Time: {(end - start)/60:.4f} minutes")
