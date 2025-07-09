# wash-trading
Improved Wash Trading Detection on Crypto Exchanges

## Run the script

```bash
Rscript pipeline_wash_trading_paper.R -d "EtherDelta" -t data/EtherDeltaTrades-preprocessed.csv -p data/EtherDollarPrice.csv -o output/etherdelta-t100-1h-1d-1w-1pmargin --sccthresholdrank=100 --washdetectionether=FALSE -m 0.01 --washwindowsizesecondspass1=3600 --washwindowsizesecondspass2=86400 --washwindowsizesecondspass3=604800
```

```bash
.\pipeline_start_EtherDelta.bat
```
