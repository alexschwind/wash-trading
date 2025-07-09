import pandas as pd
import numpy as np
from joblib import Parallel, delayed
from tqdm import tqdm
import os
import argparse
import time

# Import your scc and volume matching functions
from scc_algorithm import scc_algo_parallel
from detect_label_wash_trades import detect_label_wash_trades

ETHER_ADDR = "0x0000000000000000000000000000000000000000"

### Utils

def format_time(seconds):
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def ensure_dir_exists(folder):
    print(folder)
    if not os.path.exists(folder):
        print(f"Creating output folder: {folder}")
        os.makedirs(folder)
    else:
        print(f"Output folder already exists: {folder}")


### 1. Data Loading and Preparation

def standardize_trade_amounts(trades):
    # Buy ETH trades (tokenBuy == ETHER_ADDR)
    trades_buy_eth = trades[trades['tokenBuy'] == ETHER_ADDR].copy()
    trades_buy_eth['token'] = trades_buy_eth['tokenSell']
    trades_buy_eth['trade_amount_eth'] = trades_buy_eth['amountBoughtReal']
    trades_buy_eth['trade_amount_token'] = trades_buy_eth['amountSoldReal']
    # Sell ETH trades (tokenSell == ETHER_ADDR)
    trades_sell_eth = trades[trades['tokenSell'] == ETHER_ADDR].copy()
    trades_sell_eth['token'] = trades_sell_eth['tokenBuy']
    trades_sell_eth['trade_amount_eth'] = trades_sell_eth['amountSoldReal']
    trades_sell_eth['trade_amount_token'] = trades_sell_eth['amountBoughtReal']
    # Combine
    trades_std = pd.concat([trades_buy_eth, trades_sell_eth], ignore_index=True)
    return trades_std

def load_trades(file_csv):
    trades = pd.read_csv(file_csv)
    # Standardize column names
    trades = trades.rename(columns={"maker": "eth_buyer", "taker": "eth_seller"})
    trades = standardize_trade_amounts(trades)
    return trades


def filter_successful_and_complete_trades(trades, status_column=None, status_success=None):
    n = len(trades)
    if status_column is not None and status_success is not None and status_column in trades.columns:
        trades = trades[trades[status_column] == status_success]
    trades = trades.dropna()
    dropped = n - len(trades)
    print(f"Info: dropped {dropped} rows, which had missing/unsuccessful status, or any missing values. {len(trades)} rows remaining.")
    return trades

def filter_ether_token_trades_with_log(trades, ether_address):
    n = len(trades)
    mask = ((trades['tokenBuy'] == ether_address) | (trades['tokenSell'] == ether_address)) & (trades['tokenBuy'] != trades['tokenSell'])
    trades = trades[mask].copy()
    dropped = n - len(trades)
    print(f"Info: dropped {dropped} rows, which are trades between two tokens or trades between the same currency. {len(trades)} rows remaining.")
    return trades


def merge_trades_with_usd_price(trades, price_file_csv, ether_address):
    price_df = pd.read_csv(price_file_csv)
    price_df.columns = ['date', 'timestamp', 'dollar']
    price_df['date'] = pd.to_datetime(price_df['date'], format='%m/%d/%Y')
    price_df['timestamp'] = pd.to_numeric(price_df['timestamp'])
    # Add "cut" column by timestamp intervals (see R code for "cut")
    intervals = price_df['timestamp'].sort_values().unique()
    trades['cut'] = pd.cut(trades['timestamp'], bins=intervals, labels=intervals[:-1], include_lowest=True, right=False)
    trades['cut'] = trades['cut'].astype(float)
    # Merge by "cut"
    trades = pd.merge(trades, price_df[['timestamp', 'dollar', 'date']], left_on='cut', right_on='timestamp',
                      how='left', suffixes=('', '_usd'))
    # You'll want to follow the trade structure and adjust as necessary
    return trades


### 2. Self-Trades Filtering

def filter_self_trades(trades, save_path=None):
    self_trades = trades[trades['eth_buyer'] == trades['eth_seller']]
    non_self_trades = trades[trades['eth_buyer'] != trades['eth_seller']]
    print(f"Filtered {len(self_trades)} self-trades, {len(non_self_trades)} non-self-trades remain.")
    if save_path:
        self_trades.to_csv(os.path.join(save_path, 'self_trades.csv'), index=False)
    return non_self_trades, self_trades


### 3. Trader Hashing

def assign_trader_ids(trades):
    # Map each unique address to an integer
    all_addresses = pd.unique(trades[['eth_buyer', 'eth_seller']].values.ravel('K'))
    addr2id = {addr: i for i, addr in enumerate(sorted(all_addresses))}
    trades['eth_buyer_id'] = trades['eth_buyer'].map(addr2id)
    trades['eth_seller_id'] = trades['eth_seller'].map(addr2id)
    return trades, addr2id


### 4. SCC Detection

def get_relevant_scc_by_threshold(scc_dt, threshold):
    relevant = scc_dt[scc_dt['occurrence'] >= threshold]
    print(f"Selected {len(relevant)} relevant SCCs at threshold {threshold}.")
    return set(relevant['scc_hash'])


### 5. Wash Trade Detection: per SCC, per window

def detect_and_label_wash_trades_parallel(trades, relevant_scc_hashes, scc_traders_map, window_sizes, ether, margin,
                                          n_jobs=8):
    # Preprocessing: Add wash_label column
    trades['wash_label'] = np.nan
    # Prepare tasks: one per (scc, window size)
    tasks = []
    for scc_hash in relevant_scc_hashes:
        trader_ids = scc_traders_map[scc_hash]
        for window_size in window_sizes:
            tasks.append((scc_hash, trader_ids, window_size))

    # Define worker
    def process_scc_window(scc_hash, trader_ids, window_size):
        scc_trades = trades[(trades['eth_buyer_id'].isin(trader_ids)) & (trades['eth_seller_id'].isin(trader_ids))]
        if ether:
            temp_trades = scc_trades[
                ['transactionHash', 'token', 'date', 'timestamp', 'eth_buyer', 'eth_seller', 'trade_amount_eth',
                 'trade_amount_dollar', 'wash_label']].copy()
            temp_trades.rename(columns={'eth_buyer': 'buyer', 'eth_seller': 'seller', 'trade_amount_eth': 'amount'},
                               inplace=True)
        else:
            temp_trades = scc_trades[
                ['transactionHash', 'token', 'date', 'timestamp', 'eth_buyer', 'eth_seller', 'trade_amount_token',
                 'trade_amount_dollar', 'wash_label']].copy()
            temp_trades.rename(columns={'eth_buyer': 'buyer', 'eth_seller': 'seller', 'trade_amount_token': 'amount'},
                               inplace=True)
        # Split into time windows
        min_ts, max_ts = temp_trades['timestamp'].min(), temp_trades['timestamp'].max()
        if pd.isnull(min_ts) or pd.isnull(max_ts):
            return pd.DataFrame()
        bins = np.arange(min_ts, max_ts + window_size, window_size)
        temp_trades['window'] = pd.cut(temp_trades['timestamp'], bins=bins, labels=bins[:-1], include_lowest=True,
                                       right=False)
        results = []
        for window_val, group in temp_trades.groupby('window', observed=True):
            # Run volume matching
            if group.empty:
                continue
            detected = detect_label_wash_trades(group, margin=margin)
            results.append(detected)
        if results:
            return pd.concat(results)
        else:
            return pd.DataFrame()

    print("Starting parallel wash trading detection...")
    results = Parallel(n_jobs=n_jobs)(
        delayed(process_scc_window)(scc_hash, trader_ids, window_size)
        for (scc_hash, trader_ids, window_size) in tqdm(tasks)
    )
    # Update main trades DataFrame
    wash_detected = pd.concat([df for df in results if not df.empty])
    trades.loc[trades['transactionHash'].isin(
        wash_detected[wash_detected['wash_label'] == True]['transactionHash']), 'wash_label'] = True
    return trades, wash_detected


### 6. Save Results and Summaries

def save_trades(trades, output_folder, filename):
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    trades.to_csv(os.path.join(output_folder, filename), index=False)


### 7. Main Pipeline Function

def run_pipeline(
        trades_file,
        price_file,
        output_folder,
        ether_address,
        scc_threshold_rank=100,
        wash_trade_detection_ether=True,
        wash_trade_detection_margin=0.1,
        wash_window_sizes_seconds=[60 * 60 * 24 * 7],  # Default: 1 week
        n_jobs=8
):
    start_time = time.time()
    #IO
    ensure_dir_exists(output_folder)
    # Load and preprocess
    trades = load_trades(trades_file)
    trades = filter_successful_and_complete_trades(trades)
    trades = filter_ether_token_trades_with_log(trades, ether_address)
    trades = merge_trades_with_usd_price(trades, price_file, ether_address)
    trades['trade_amount_dollar'] = trades['trade_amount_eth'] * trades['dollar']
    non_self_trades, self_trades = filter_self_trades(trades, output_folder)
    trades, addr2id = assign_trader_ids(non_self_trades)

    # SCC detection
    scc_dt, relevant, scc_traders_map = scc_algo_parallel(trades)
    # scc_traders_map = {row['scc_hash']: row['traders'] for _, row in
    #                    relevant.iterrows()}  # Build SCC -> traders mapping

    relevant_scc_hashes = get_relevant_scc_by_threshold(scc_dt, scc_threshold_rank)
    # Wash trade detection (parallel over (scc, window))
    trades_labeled, wash_detected = detect_and_label_wash_trades_parallel(
        trades,
        relevant_scc_hashes,
        scc_traders_map,
        wash_window_sizes_seconds,
        ether=wash_trade_detection_ether,
        margin=wash_trade_detection_margin,
        n_jobs=n_jobs
    )

    # Save
    save_trades(trades_labeled, output_folder, "trades_labeled.csv")
    save_trades(wash_detected, output_folder, "wash_trades_detected.csv")
    print("Pipeline complete. Results written to", output_folder)
    end_time = time.time()
    elapsed = end_time - start_time
    print(f"Pipeline complete. Total elapsed time: {format_time(elapsed)} (hh:mm:ss)")
### 8. CLI entrypoint

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--trades', required=True)
    parser.add_argument('--prices', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--ether', default="0x0000000000000000000000000000000000000000")
    parser.add_argument('--scc_threshold_rank', type=int, default=100)
    parser.add_argument('--wash_trade_detection_ether', action='store_true')
    parser.add_argument('--wash_trade_detection_margin', type=float, default=0.1)
    parser.add_argument('--wash_window_sizes_seconds', nargs='+', type=int, default=[60 * 60 * 24 * 7])
    parser.add_argument('--n_jobs', type=int, default=8)
    args = parser.parse_args()

    run_pipeline(
        trades_file=args.trades,
        price_file=args.prices,
        output_folder=args.output,
        ether_address=args.ether,
        scc_threshold_rank=args.scc_threshold_rank,
        wash_trade_detection_ether=args.wash_trade_detection_ether,
        wash_trade_detection_margin=args.wash_trade_detection_margin,
        wash_window_sizes_seconds=args.wash_window_sizes_seconds,
        n_jobs=args.n_jobs
    )
