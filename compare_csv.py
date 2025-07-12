import pandas as pd
import numpy as np

def is_equal(val1, val2, precision=6):
    if val1 == "nan" and val2 == "nan":
        return True
    if pd.isna(val1) and pd.isna(val2):
        return True
    try:
        f1, f2 = float(val1), float(val2)
        return np.isclose(f1, f2, rtol=10**-precision, atol=0)
    except:
        return str(val1).strip().lower() == str(val2).strip().lower()

def compare_scc_trades(file_r, file_py, key_col="transactionHash", precision=10):
    df_r = pd.read_csv(file_r, dtype=str)
    df_py = pd.read_csv(file_py, dtype=str)

    df_r.set_index(key_col, inplace=True)
    df_py.set_index(key_col, inplace=True)

    common_keys = df_r.index.intersection(df_py.index)
    print(f"Shapes R:{df_r.shape} PY:{df_py.shape}")
    print(f"🔍 Comparing {len(common_keys)} common transactions by '{key_col}'.")

    if len(common_keys) != len(df_r.index):
        print(f"❌ Not all entries are included. {len(df_r.index) - len(common_keys)} missing.")

    diffs = 0
    for tx in common_keys:
        row_r = df_r.loc[tx]
        row_py = df_py.loc[tx]

        # Ensure single row per transaction
        if isinstance(row_r, pd.Series): row_r = row_r.to_frame().T
        if isinstance(row_py, pd.Series): row_py = row_py.to_frame().T

        for col in df_r.columns.intersection(df_py.columns):
            val_r = row_r.iloc[0][col]
            val_py = row_py.iloc[0][col]
            if not is_equal(val_r, val_py, precision):
                print(f"transactionHash: {tx}, Column '{col}': R = '{val_r}', Python = '{val_py}'")
                diffs += 1

    if diffs == 0:
        print("✅ All transactions match between R and Python.")
    else:
        print(f"❌ Found {diffs} differing values.")

print("------------------------- CUT LABELS -------------------------")
compare_scc_trades("R_relevant_scc.csv", "PY_relevant_scc.csv", key_col="scc_hash")
compare_scc_trades("R_scc.csv", "PY_scc.csv", key_col="scc_hash")