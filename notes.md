python IDEXtrades_preprocessing.py -i data/IDEXTrades_small.csv -d data/token_decimals.json -o data/IDEXTrades-preprocessed.csv
Rscript pipeline_wash_trading_paper.R -d "IDEX" -t data/IDEXTrades-preprocessed.csv -p data/EtherDollarPrice.csv -o output --sccthresholdrank=100 --washdetectionether=FALSE -m 0.01 --washwindowsizesecondspass1=3600 --washwindowsizesecondspass2=86400 --washwindowsizesecondspass3=604800

python .\compare_csv.py merge_trader_id