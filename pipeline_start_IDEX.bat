@REM @echo off
@REM python .\pipeline.py ^
@REM   --trades data\EtherDeltaTrades-preprocessed.csv ^
@REM   --prices data\EtherDollarPrice.csv ^
@REM   --out output\etherdelta-t100-1h-1d-1w-1pmargin ^
@REM   --ether_id 0x0000000000000000000000000000000000000000 ^
@REM   --scc_th 100 ^
@REM   --margin 0.01 ^
@REM   --windows 3600 86400 604800

@echo off
python pipeline.py ^
    --trades data\EtherDeltaTrades-preprocessed.csv ^
    --prices data\EtherDollarPrice.csv ^
    --output output\etherdelta-t100-1h-1d-1w-1pmargin-newidex ^
    --scc_threshold_rank 100 ^
    --wash_trade_detection_margin 0.01 ^
    --wash_window_sizes_seconds 3600 86400 604800
pause