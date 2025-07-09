import numpy as np
import pandas as pd

def detect_label_wash_trades(df, margin=0.1):
    buyers = df['buyer'].values
    sellers = df['seller'].values
    amounts = df['amount'].values

    n = len(df)
    indices = np.arange(n)

    # balanceMap = {account: balance}
    balance_map = {}
    trade_amounts = []
    running_sum = 0.0

    # 1st loop: initialize balances and running sum
    for idx in indices:
        amount = amounts[idx]
        trade_amounts.append(amount)
        running_sum += amount
        balance_map[buyers[idx]] = balance_map.get(buyers[idx], 0) + amount
        balance_map[sellers[idx]] = balance_map.get(sellers[idx], 0) - amount

    # Prepare wash_label if not present
    if 'wash_label' not in df.columns:
        df['wash_label'] = False

    # 2nd loop: backwards, efficiently maintain mean
    count = n
    for idx in range(n - 1, 0, -1):
        # Check if current set is wash
        balances = np.array(list(balance_map.values()))
        mean_trade_vol = running_sum / count if count > 0 else 0

        norm_balances = np.abs(balances / mean_trade_vol)
        if np.all(norm_balances <= margin):
            df.loc[:idx, 'wash_label'] = True
            return df

        # Remove this trade from running sum and balance map
        amount = amounts[idx]
        running_sum -= amount
        count -= 1

        balance_map[buyers[idx]] -= amount
        balance_map[sellers[idx]] += amount

    return df
