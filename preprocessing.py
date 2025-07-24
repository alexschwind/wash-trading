# take the original transaction data and convert it so that every transaction has a buyer_id, seller_id, amount

import pandas as pd

def preprocessing(filename, ether_dollar_path="data/EtherDollarPrice.csv", token_decimals_path="data/token_decimals.json", filter_status=True):
    trades = pd.read_csv(filename, header=0)

    

    # all token addresses
    IDEX_tokens = pd.DataFrame(pd.unique(pd.concat([trades['tokenBuy'], trades['tokenSell']], axis=0)), columns = ['address'])

    # load decimals for token conversion to float
    token_decimals = pd.read_json(token_decimals_path, orient="index")
    token_decimals.reset_index(inplace=True)
    token_decimals.drop(labels=["index", "name", "slug"], axis=1, inplace=True)
    token_decimals = pd.merge(token_decimals, IDEX_tokens, how = 'right') # keep only IDEX tokens
    token_decimals[['decimals']] = token_decimals[['decimals']].fillna(value = 18)

    # convert fields to float
    trades_real = trades.merge(token_decimals, how="left", left_on="tokenBuy", right_on="address")
    trades_real.drop(labels=["address"], axis=1, inplace=True)
    trades_real['amountBuyReal'] = trades_real['amountBuy']
    trades_real['amountBoughtReal'] = trades_real['amount']
    trades_real = trades_real.astype({'amountBuyReal': float, 'amountBoughtReal': float})
    trades_real['amountBuyReal'] = trades_real['amountBuyReal'].divide(10**trades_real['decimals'])
    trades_real['amountBoughtReal'] = trades_real['amountBoughtReal'].divide(10**trades_real['decimals'])
    trades_real.drop(labels=['decimals'], axis=1, inplace=True)

    trades_real = trades_real.merge(token_decimals, how="left", left_on="tokenSell", right_on="address")
    trades_real.drop(labels=["address"], axis=1, inplace=True)
    trades_real['amountSellReal'] = trades_real['amountSell']
    trades_real = trades_real.astype({'amountSellReal': float})
    trades_real['amountSellReal'] = trades_real['amountSellReal'].divide(10**trades_real['decimals'])
    trades_real.drop(labels=['decimals'], axis=1, inplace=True)
    trades_real['price'] = trades_real['amountSellReal'].divide(trades_real['amountBuyReal'])
    trades_real['amountSoldReal'] = trades_real['amountBoughtReal'].mul(trades_real['price'])

    trades_real = trades_real.rename(columns={'transaction_hash':'transactionHash'})
    trades = trades_real[['timestamp',
                        'transactionHash',
                        'status',
                        'maker',
                        'taker',
                        'tokenBuy',
                        'tokenSell',
                        'amountBoughtReal',
                        'amountSoldReal',]]
    trades["timestamp"] = pd.to_numeric(trades["timestamp"])

    # get successful and complete trades
    if filter_status:
        trades = trades[trades["status"] == 1]
        trades = trades.dropna()

    # get ether <-> token trades (not token <-> token trades)
    global_ether_id = "0x0000000000000000000000000000000000000000"
    condition = ((trades["tokenBuy"] == global_ether_id) | (trades["tokenSell"] == global_ether_id)) & (trades["tokenBuy"] != trades["tokenSell"])
    trades = trades[condition]

    # merge trades with USD price
    ether_dollar = pd.read_csv(ether_dollar_path, header=0, names=["date", "timestamp", "dollar"])
    ether_dollar["date"] = pd.to_datetime(ether_dollar["date"], format="%m/%d/%Y")
    ether_dollar["timestamp"] = pd.to_numeric(ether_dollar["timestamp"])

    # Ensure timestamps are numeric
    trades["timestamp"] = pd.to_numeric(trades["timestamp"])

    # Determine time range for binning
    min_trade_ts = trades["timestamp"].min()
    max_trade_ts = trades["timestamp"].max()

    min_dollar_ts = ether_dollar[ether_dollar["timestamp"] <= min_trade_ts]["timestamp"].max()
    max_dollar_ts = ether_dollar[ether_dollar["timestamp"] >= max_trade_ts]["timestamp"].min()

    # Binning intervals (left endpoints)
    intervals_left = ether_dollar[
        (ether_dollar["timestamp"] >= min_dollar_ts) & 
        (ether_dollar["timestamp"] <= max_dollar_ts)
    ]["timestamp"].sort_values().unique()

    # Use pd.cut to bin each trade into intervals
    trades["cut"] = pd.cut(
        trades["timestamp"],
        bins=intervals_left,
        right=False,
        include_lowest=True,
        labels=intervals_left[:-1]
    ).astype(float)

    # Prepare Ether price reference DataFrame
    ether_price = ether_dollar[["timestamp", "dollar", "date"]].rename(columns={
        "timestamp": "cut",
        "dollar": "eth_price"
    })

    # --- Buy ETH trades ---
    trades_buyeth = trades[trades["tokenBuy"] == global_ether_id].copy()
    trades_buyeth = trades_buyeth.merge(ether_price, on="cut", how="left")

    trades_buyeth = trades_buyeth.assign(
        eth_buyer=trades_buyeth["maker"],
        eth_seller=trades_buyeth["taker"],
        token=trades_buyeth["tokenSell"],
        trade_amount_eth=trades_buyeth["amountBoughtReal"],
        trade_amount_dollar=trades_buyeth["amountBoughtReal"] * trades_buyeth["eth_price"],
        trade_amount_token=trades_buyeth["amountSoldReal"],
    )

    trades_buyeth = trades_buyeth[[
        "date", "cut", "timestamp", "transactionHash",
        "eth_buyer", "eth_seller", "token",
        "trade_amount_eth", "trade_amount_dollar", "trade_amount_token"
    ]]

    # --- Sell ETH trades ---
    trades_selleth = trades[trades["tokenSell"] == global_ether_id].copy()
    trades_selleth = trades_selleth.merge(ether_price, on="cut", how="left")

    trades_selleth = trades_selleth.assign(
        eth_buyer=trades_selleth["taker"],
        eth_seller=trades_selleth["maker"],
        token=trades_selleth["tokenBuy"],
        trade_amount_eth=trades_selleth["amountSoldReal"],
        trade_amount_dollar=trades_selleth["amountSoldReal"] * trades_selleth["eth_price"],
        trade_amount_token=trades_selleth["amountBoughtReal"],
    )

    trades_selleth = trades_selleth[[
        "date", "cut", "timestamp", "transactionHash",
        "eth_buyer", "eth_seller", "token",
        "trade_amount_eth", "trade_amount_dollar", "trade_amount_token",
    ]]

    # Combine buy and sell ETH trades
    trades = pd.concat([trades_buyeth, trades_selleth], ignore_index=True)

    # filter self trades
    self_trades = trades[trades["eth_buyer"] == trades["eth_seller"]]
    non_self_trades = trades[trades["eth_buyer"] != trades["eth_seller"]]
    trades = non_self_trades

    # add trader hashes
    global_trader_hashes = pd.DataFrame(columns=["trader_address", "trader_id"])
    all_traders = pd.unique(trades["eth_buyer"].tolist() + trades["eth_seller"].tolist())
    all_traders = sorted(all_traders)
    if global_trader_hashes.empty:
        global_trader_hashes = pd.DataFrame({
            "trader_address": all_traders,
            "trader_id": [str(i + 1) for i in range(len(all_traders))]  # IDs as strings
        })
    else:
        # Identify new traders not yet in global_trader_hashes
        existing_traders = set(global_trader_hashes["trader_address"])
        additional_traders = [addr for addr in all_traders if addr not in existing_traders]

        if additional_traders:
            n_old = len(global_trader_hashes)
            new_entries = pd.DataFrame({
                "trader_address": additional_traders,
                "trader_id": [str(i + 1) for i in range(n_old, n_old + len(additional_traders))]
            })
            global_trader_hashes = pd.concat([global_trader_hashes, new_entries], ignore_index=True)


    trades = trades.merge(global_trader_hashes.rename(columns={
        "trader_address": "eth_buyer", "trader_id": "eth_buyer_id"
    }), on="eth_buyer", how="left")

    trades = trades.merge(global_trader_hashes.rename(columns={
        "trader_address": "eth_seller", "trader_id": "eth_seller_id"
    }), on="eth_seller", how="left")

    trades = trades.sort_values("timestamp")
    trades = trades[["eth_seller","eth_buyer","date","cut","timestamp","transactionHash","token","trade_amount_eth","trade_amount_dollar","trade_amount_token","eth_buyer_id","eth_seller_id"]].copy()
        
    return trades, global_trader_hashes
