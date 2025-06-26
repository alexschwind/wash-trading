import pandas as pd
import numpy as np
import os

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
compare_scc_trades("r_function_output11.csv", "py_function_output11.csv")
compare_scc_trades("r_function_output12.csv", "py_function_output12.csv")
compare_scc_trades("r_function_output13.csv", "py_function_output13.csv")
compare_scc_trades("r_function_output21.csv", "py_function_output21.csv")
compare_scc_trades("r_function_output22.csv", "py_function_output22.csv")
compare_scc_trades("r_function_output23.csv", "py_function_output23.csv")
# print("------------------------- TRADES AFTER -------------------------")
# compare_scc_trades("trades_after11.csv", "trades_after11_py.csv")
# compare_scc_trades("trades_after12.csv", "trades_after12_py.csv")
# compare_scc_trades("trades_after13.csv", "trades_after13_py.csv")
# compare_scc_trades("trades_after21.csv", "trades_after21_py.csv")
# compare_scc_trades("trades_after22.csv", "trades_after22_py.csv")
# compare_scc_trades("trades_after23.csv", "trades_after23_py.csv")
# print("------------------------- GROUPS -------------------------")
# for f in os.listdir("r_input_groups"):
#     if f.endswith(".csv"):
#         py_f = (f[:-16]+".0.csv").replace("__", "_")
#         compare_scc_trades("r_input_groups/"+f, "py_input_groups/"+py_f)
