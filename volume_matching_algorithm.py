from collections import defaultdict
import ctypes
import numpy as np
import pandas as pd
from tqdm import tqdm
from joblib import Parallel, delayed

def seqlast(start: float, stop: float, step: int) -> list:
    """Mimics seqlast in R (sequence from start to stop, step size in seconds)"""
    seq = list(np.arange(start, stop + step, step))
    if not np.isclose(seq[-1], stop):
        if seq[-1] < stop:
            seq.append(stop)
        else:
            seq[-1] = stop
    return seq

def detect_label_wash_trades(df: pd.DataFrame, margin: float = 0.01):

    if df.empty:
        return []
        
    # Remap buyer/seller IDs to dense indices
    all_ids = pd.concat([df['eth_seller'], df['eth_buyer']])
    id_map = {id_: i for i, id_ in enumerate(all_ids.unique())}

    # Before we did this, but we can also just use other names
    # temp_trades.rename(columns={
    #                 "eth_seller": "buyer",
    #                 "eth_buyer": "seller",
    #                 "trade_amount_token": "amount"
    #             }, inplace=True)

    buyers_remapped = df['eth_seller'].map(id_map).astype(np.int32).to_numpy(copy=True)
    sellers_remapped = df['eth_buyer'].map(id_map).astype(np.int32).to_numpy(copy=True)
    amounts = df['trade_amount_token'].astype(np.float64).to_numpy(copy=True)
    n = len(df)
    num_unique_ids = len(id_map)

    result_flags = (ctypes.c_int * n)()

    # Load the C library
    lib = ctypes.CDLL('./detect_wash_trades.dll')
    lib.detect_label_wash_trades.argtypes = [
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_double),
        ctypes.c_int,
        ctypes.c_double,
        ctypes.POINTER(ctypes.c_int),
        ctypes.c_int
    ]
    lib.detect_label_wash_trades.restype = ctypes.c_int

    lib.detect_label_wash_trades(
        buyers_remapped.ctypes.data_as(ctypes.POINTER(ctypes.c_int)),
        sellers_remapped.ctypes.data_as(ctypes.POINTER(ctypes.c_int)),
        amounts.ctypes.data_as(ctypes.POINTER(ctypes.c_double)),
        n,
        margin,
        result_flags,
        num_unique_ids
    )

    # Get transaction hashes where flag == 1
    wash_trade_hashes = df.loc[
        np.frombuffer(result_flags, dtype=np.int32).astype(bool),
        'transactionHash'
    ].tolist()
    return wash_trade_hashes

def volume_matching_parallel(trades: pd.DataFrame, relevant: pd.DataFrame, global_scc_traders_map):

    window_sizes_in_seconds = [3600, 86400, 604800]

    trades["wash_label"] = False

    window_start = trades["cut"].min()
    relevant_scc = relevant["scc_hash"].to_list()
    wash_trades = defaultdict(lambda: defaultdict(list))

    with tqdm(total=len(window_sizes_in_seconds) * len(relevant), desc="Processing SCCs") as pbar:
        for window_size in window_sizes_in_seconds:
            breaks = seqlast(window_start, trades["timestamp"].max(), window_size)

            for scc_id in relevant_scc:
                scc_traders = global_scc_traders_map[scc_id]

                scc_trades = trades[
                    (trades["eth_buyer_id"].isin(scc_traders)) &
                    (trades["eth_seller_id"].isin(scc_traders)) &
                    (trades["wash_label"] == False)
                ].sort_values("cut")

                if scc_trades.empty:
                    wash_trades[scc_id][str(window_size)] = []
                    pbar.update(1)
                    continue

                temp_trades = scc_trades[[
                    "transactionHash", "token", "date", "timestamp", 
                    "eth_seller", "eth_buyer", "trade_amount_token", "trade_amount_dollar", "wash_label"
                ]].copy()

                # Create window labels (right-exclusive, left-inclusive)
                temp_trades["window"] = pd.cut(
                    temp_trades["timestamp"],
                    bins=breaks,
                    right=False,
                    include_lowest=True,
                )

                # Group by token and time window
                grouped = temp_trades.groupby(["token", "window"], observed=True)

                scc_wash_trades_all = Parallel(n_jobs=16)(
                    delayed(detect_label_wash_trades)(group.reset_index(drop=True)) for _, group in grouped
                )

                all_hashes = [tx for sublist in scc_wash_trades_all for tx in sublist]

                # Store
                wash_trades[scc_id][str(window_size)] = all_hashes # all transaction hashes that are wash_trades

                trades.loc[trades['transactionHash'].isin(all_hashes), 'wash_label'] = True

                pbar.update(1)
    
    return trades, wash_trades

def volume_matching_parallel_overlapping(trades: pd.DataFrame, relevant: pd.DataFrame, global_scc_traders_map):

    window_sizes_in_seconds = [3600, 86400, 604800]

    trades["wash_label"] = False

    window_start = trades["cut"].min()
    window_end = trades["timestamp"].max()
    relevant_scc = relevant["scc_hash"].to_list()
    wash_trades = defaultdict(lambda: defaultdict(list))

    with tqdm(total=len(window_sizes_in_seconds) * len(relevant), desc="Processing SCCs") as pbar:
        for scc_id in relevant_scc:
            scc_traders = global_scc_traders_map[scc_id]

            scc_trades = trades[
                (trades["eth_buyer_id"].isin(scc_traders)) &
                (trades["eth_seller_id"].isin(scc_traders)) &
                (trades["wash_label"] == False)
            ].sort_values("cut")

            if scc_trades.empty:
                wash_trades[scc_id][str(window_size)] = []
                pbar.update(1)
                continue

            temp_trades = scc_trades[[
                "transactionHash", "token", "date", "timestamp", 
                "eth_seller", "eth_buyer", "trade_amount_token", "trade_amount_dollar", "wash_label"
            ]].copy()

            windowed_groups = []
            for window_size in window_sizes_in_seconds:
                stride = 3 * window_size // 4
                window_start_points = np.arange(window_start, window_end, stride)

                for start_time in window_start_points:
                    end_time = start_time + window_size
                    window_trades = temp_trades[
                        (temp_trades["timestamp"] >= start_time) &
                        (temp_trades["timestamp"] < end_time)
                    ].copy()

                    if not window_trades.empty:
                        windowed_groups.append((start_time, window_trades))

            scc_wash_trades_all = Parallel(n_jobs=16)(
                delayed(detect_label_wash_trades)(group.reset_index(drop=True)) for _, group in windowed_groups
            )

            all_hashes = [tx for sublist in scc_wash_trades_all for tx in sublist]

            # Store
            #wash_trades[scc_id][str(window_size)] = all_hashes # all transaction hashes that are wash_trades
            pbar.update(len(window_sizes_in_seconds))

            del windowed_groups

            trades.loc[trades['transactionHash'].isin(all_hashes), 'wash_label'] = True

    
    return trades, wash_trades

def get_address_clusters(relevant: pd.DataFrame, global_scc_traders_map, global_trader_hashes):

    relevant_scc = relevant["scc_hash"].to_list()

    address_clusters = {}
    for scc_id in relevant_scc:
        trader_ids = global_scc_traders_map.get(scc_id, [])
        addresses = global_trader_hashes[global_trader_hashes["trader_id"].isin(trader_ids)]["trader_address"].tolist()
        address_clusters[str(scc_id)] = addresses

    return address_clusters


def volume_matching_parallel_better(trades: pd.DataFrame, relevant: pd.DataFrame, global_scc_traders_map):

    window_sizes_in_seconds = [3600, 86400, 604800]

    trades["wash_label"] = False

    window_start = trades["cut"].min()
    relevant_scc = relevant["scc_hash"].to_list()
    wash_trades = defaultdict(lambda: defaultdict(list))

    with tqdm(total=len(window_sizes_in_seconds) * len(relevant), desc="Processing SCCs") as pbar:
        for scc_id in relevant_scc:
            scc_traders = global_scc_traders_map[scc_id]

            scc_trades = trades[
                (trades["eth_buyer_id"].isin(scc_traders)) &
                (trades["eth_seller_id"].isin(scc_traders)) &
                (trades["wash_label"] == False)
            ].sort_values("cut")

            if scc_trades.empty:
                wash_trades[scc_id][str(window_size)] = []
                pbar.update(1)
                continue

            temp_trades = scc_trades[[
                "transactionHash", "token", "date", "timestamp", 
                "eth_seller", "eth_buyer", "trade_amount_token", "trade_amount_dollar", "wash_label"
            ]].copy()

            for window_size in window_sizes_in_seconds:

                breaks = seqlast(window_start, trades["timestamp"].max(), window_size)

                # Create window labels (right-exclusive, left-inclusive)
                temp_trades["window"] = pd.cut(
                    temp_trades["timestamp"],
                    bins=breaks,
                    right=False,
                    include_lowest=True,
                )


                # Group by token and time window
                grouped = temp_trades.groupby(["token", "window"], observed=True)

                scc_wash_trades_all = Parallel(n_jobs=16)(
                    delayed(detect_label_wash_trades)(group.reset_index(drop=True)) for _, group in grouped
                )

                all_hashes = [tx for sublist in scc_wash_trades_all for tx in sublist]

                # Store
                wash_trades[scc_id][str(window_size)] = all_hashes # all transaction hashes that are wash_trades

                trades.loc[trades['transactionHash'].isin(all_hashes), 'wash_label'] = True

                pbar.update(1)
    
    return trades, wash_trades