import networkx as nx
import hashlib
from tqdm.auto import tqdm
import pandas as pd
from joblib import Parallel, delayed

def digest2int(sorted_list) -> str:
    # Mimics digest2int by converting hash digest to an integer string
    joined_str = ",".join(str(int(x)) for x in sorted_list)
    return str(int(hashlib.sha256(joined_str.encode()).hexdigest(), 16))

def digest2int_orig(s) -> str:
    # Mimics digest2int by converting hash digest to an integer string
    return str(int(hashlib.sha256(s.encode()).hexdigest(), 16))

def process_sub_trades(sub_trades):
    G = nx.MultiDiGraph()
    G.add_weighted_edges_from(sub_trades.values, weight="weight")

    G_simple = nx.DiGraph()
    for u, v in G.edges():
        if u == v:
            continue
        if G_simple.has_edge(u, v):
            G_simple[u][v]["weight"] += 1
        else:
            G_simple.add_edge(u, v, weight=1)

    result = []
    local_scc_traders_map = {}

    while G_simple.number_of_nodes() > 0:
        sccs = [list(c) for c in nx.strongly_connected_components(G_simple) if len(c) > 1]
        if not sccs:
            break

        for scc in sccs:
            sorted_members = sorted(scc)
            c_hash = digest2int(sorted_members)
            local_scc_traders_map[c_hash] = sorted_members
            result.append(c_hash)

        edges_to_remove = []
        for u, v, data in G_simple.edges(data=True):
            data["weight"] -= 1
            if data["weight"] <= 0:
                edges_to_remove.append((u, v))

        G_simple.remove_edges_from(edges_to_remove)
        isolated_nodes = [n for n in G_simple.nodes if G_simple.degree(n) == 0]
        G_simple.remove_nodes_from(isolated_nodes)

    return result, list(local_scc_traders_map.items())

def scc_algo_parallel(trades: pd.DataFrame):
    all_results = []
    print("Creating sub_trades_list...")
    trades["weight"] = 1
    trades_sorted = trades.sort_values("token")
    sub_trades_list = [
        group[["eth_buyer_id", "eth_seller_id", "weight"]].copy()
        for _, group in trades_sorted.groupby("token")
    ]
    print("Spawning parallel jobs.")
    parallel = Parallel(n_jobs=16)
    results = parallel(
        delayed(process_sub_trades)(sub_trades) for sub_trades in tqdm(sub_trades_list, desc="Processing tokens")
    )

    global_scc_traders_map = {}
    for result, local_scc_traders_map in results:
        all_results.extend(result)
        for key, val in local_scc_traders_map:
            global_scc_traders_map[key] = val
    
    print("All processes finished.")
    scc_df = pd.DataFrame(all_results, columns=["scc_hash"])
    scc_dt = scc_df.value_counts().reset_index(name="occurrence")
    scc_dt["num_traders"] = scc_dt["scc_hash"].apply(lambda h: len(global_scc_traders_map[h]))
    relevant = scc_dt[scc_dt["occurrence"] >= 100]
    return scc_dt, relevant

def scc_algo_seq_orig(trades):
    global_scc_traders_map = {}
    token_list = trades["token"].unique()
    result = []
    for token in tqdm(token_list, desc="Processing tokens"):
        sub_trades = trades[trades["token"] == token][["eth_buyer_id", "eth_seller_id"]].copy()
        sub_trades["weight"] = 1

        # Build initial multigraph
        G = nx.MultiDiGraph()
        G.add_weighted_edges_from(sub_trades.values, weight="weight")

        # Convert to simplified DiGraph with edge weight counts
        # simple graphs are graphs that dont contain loops or multiple edges
        G_simple = nx.DiGraph()
        for u, v in G.edges():
            if u == v:
                continue
            if G_simple.has_edge(u, v):
                G_simple[u][v]["weight"] += 1
            else:
                G_simple.add_edge(u, v, weight=1)

        # Layered detection
        while G_simple.number_of_nodes() > 0:
            sccs = [list(c) for c in nx.strongly_connected_components(G_simple) if len(c) > 1]
            if not sccs:
                break

            for scc in sccs:
                sorted_members = sorted(scc)
                c_hash = digest2int(sorted_members)
                global_scc_traders_map[c_hash] = sorted_members
                result.append(c_hash)

            # Decrement edge weights
            edges_to_remove = []
            for u, v, data in G_simple.edges(data=True):
                data["weight"] -= 1
                if data["weight"] <= 0:
                    edges_to_remove.append((u, v))

            G_simple.remove_edges_from(edges_to_remove)
            isolated_nodes = [n for n in G_simple.nodes if G_simple.degree(n) == 0]
            G_simple.remove_nodes_from(isolated_nodes)
            
    scc_df = pd.DataFrame(result, columns=["scc_hash"])
    scc_dt = scc_df.value_counts().reset_index(name="occurrence")#.rename(columns={0: "scc_hash"})

    scc_dt["num_traders"] = scc_dt["scc_hash"].apply(lambda h: len(global_scc_traders_map[h]))
    relevant = scc_dt[scc_dt["occurrence"] >= 100]
    return scc_dt, relevant

def scc_algo_seq(trades):
    global_scc_traders_map = {}
    trades["weight"] = 1
    trades_sorted = trades.sort_values("token")
    sub_trades_list = [
        group[["eth_buyer_id", "eth_seller_id", "weight"]].copy()
        for _, group in trades_sorted.groupby("token")
    ]
    result = []
    for sub_trades in tqdm(sub_trades_list, desc="Processing tokens"):
        # Build initial multigraph
        G = nx.MultiDiGraph()
        G.add_weighted_edges_from(sub_trades.values, weight="weight")

        # Convert to simplified DiGraph with edge weight counts
        # simple graphs are graphs that dont contain loops or multiple edges
        G_simple = nx.DiGraph()
        for u, v in G.edges():
            if u == v:
                continue
            if G_simple.has_edge(u, v):
                G_simple[u][v]["weight"] += 1
            else:
                G_simple.add_edge(u, v, weight=1)

        # Layered detection
        while G_simple.number_of_nodes() > 0:
            sccs = [list(c) for c in nx.strongly_connected_components(G_simple) if len(c) > 1]
            if not sccs:
                break
            
            for scc in sccs:
                sorted_members = sorted(scc)
                c_hash = digest2int(sorted_members)
                global_scc_traders_map[c_hash] = sorted_members
                result.append(c_hash)

            # Decrement edge weights
            edges_to_remove = []
            for u, v, data in G_simple.edges(data=True):
                data["weight"] -= 1
                if data["weight"] <= 0:
                    edges_to_remove.append((u, v))

            G_simple.remove_edges_from(edges_to_remove)
            isolated_nodes = [n for n in G_simple.nodes if G_simple.degree(n) == 0]
            G_simple.remove_nodes_from(isolated_nodes)
            
    scc_df = pd.DataFrame(result, columns=["scc_hash"])
    scc_dt = scc_df.value_counts().reset_index(name="occurrence")#.rename(columns={0: "scc_hash"})

    scc_dt["num_traders"] = scc_dt["scc_hash"].apply(lambda h: len(global_scc_traders_map[h]))
    relevant = scc_dt[scc_dt["occurrence"] >= 100]
    return scc_dt, relevant