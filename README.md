# Wash Trading Detection Pipeline

This repository contains a scalable pipeline for detecting **wash trades** in Ethereum-based token transactions using **graph analysis**, **parallel processing**, and a **volume-matching algorithm accelerated with C**.

The pipeline identifies **strongly connected components (SCCs)** in trader networks, and then analyzes transaction patterns in time-based windows to detect potential wash trading behavior.

---

## 🚀 How It Works

1. **Graph-Based Trader Grouping**:
   - Transactions are grouped by token.
   - A graph is built from buyer-seller pairs.
   - Strongly connected components (SCCs) are extracted to identify potential colluding trader groups.

2. **Volume-Based Wash Trade Detection**:
   - For each SCC, trades are analyzed in multiple **sliding time windows** (hour/day/week).
   - A C-based function checks if trade volumes balance out in a way consistent with wash trading.
   - Transactions are labeled as wash or non-wash trades accordingly.

---

## 📦 Setup Instructions

### 1. Download the Data

Download the dataset from the following link:

📥 **[Download Dataset](https://tubcloud.tu-berlin.de/s/6WGL9HnK4QwJKRK)**

Place the downloaded file(s) into the `data/` directory in the project root.

---

### 2. Set Up the Python Environment

Create and activate a virtual environment (recommended):

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Compile Shared Library
The volume matching logic is written in C for performance. You must compile the file into a shared library so it can be loaded from Python.

```bash
# Windows
gcc -shared -o detect_wash_trades.dll detect_wash_trades.c
```

### 4. Run the Pipeline
Run the main detection script:

```bash
python pipeline.py
```
The script will:

1. Process the trade data
2. Detect SCCs and wash trades
3. Generate an output CSV file containing all trades labeled as wash or non-wash

The resulting file will be saved in `trades_wash_labeled.csv`.

#### 📤 Output
The output CSV contains the original trade data plus an additional wash_label column:

True — trade is flagged as a potential wash trade

False — trade is considered legitimate

#### ⚡ Dependencies
Python 3.8+

pandas, numpy, networkx, joblib, tqdm

C compiler (for building the volume matching module)