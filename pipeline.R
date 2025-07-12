library("optparse")
library(data.table)
library(igraph)
library(rjson)
library(Rcpp)
library(hash)
library(digest)

sourceCpp(file = "volume_matching.cpp")



global_trader_hashes <- data.table(trader_address = character(), trader_id = character())
global_scc_traders_map <- hash()

# get sequences including last element, even if it does not match the window sizes
seqlast <- function (from, to, by) {
  vec <- do.call(what = seq, args = list(from, to, by))
  if ( tail(vec, 1) != to ) {
    return(c(vec, to))
  } else {
    return(vec)
  }
}

call_IDEX_pipeline <- function(scc_threshold_rank = 100,
                               wash_trade_detection_ether = FALSE,
                               wash_trade_detection_margin = 0.01,
                               wash_window_sizes_seconds = c(3600, 86400, 604800)) {
    

    # trades <- load_trades(file_csv = IDEXtrades_file)
    # trades <- get_successful_and_complete_trades(trades = trades, status_column = quote(status), status_success = 1)
    # trades <- get_ether_token_trades(trades = trades, token_column1 = quote(tokenBuy), token_column2 = quote(tokenSell))
    # trades <- merge_trades_with_daily_usd_price(trades = trades, price_file_csv = EtherDollarPrice_file)
    # l <- filter_self_trades(trades = trades, save = TRUE, folder = output_folder)
    # self_trades <- l[["self_trades"]]
    # self_trades_summary <- summarize_self_trades(self_trades = self_trades, save = TRUE, folder = output_folder)
    # trades <- l[["non_self_trades"]]
    # trades <- add_trader_hashes(trades = trades)

### LOAD PREPROCESSED DATA
    trades <- fread(file = "data_preprocessed.csv")
    print(paste0("Info: read data_preprocessed.csv as data.table with ", nrow(trades), " rows."))
    print(paste0("Columns are: ", paste(colnames(trades), collapse = ", ")))
  
    # scc_dt <- detect_scc_for_tokens_layered(trades = trades, save = TRUE, folder = output_folder)
### SCC Algorithm
    start_time <- Sys.time()
    tokenVector <- unique(trades$token)
    result <- c()
  
    pb <- txtProgressBar(min = 0, max = length(tokenVector), style = 3)
    for (token_index in 1:length(tokenVector)) {
        tokenName <- tokenVector[token_index]
        g <- graph_from_data_frame(trades[token == tokenName, list(eth_buyer_id, eth_seller_id, weight=1)])
        gs <- simplify(g, edge.attr.comb = length)
        
        while(vcount(gs) > 0) {
            comps <- components(gs, "strong")
            ids_larger_one <- which(comps$csize > 1)
            if(length(ids_larger_one) == 0) {
                gs <- delete_vertices(gs, V(gs))
                next
            }
            for(c_id in seq(1, length(ids_larger_one))) {
                c_v_ids <- which(comps$membership %in% ids_larger_one[c_id])
                c_v_names <- vertex_attr(gs, "name", c_v_ids)
                sorted_members <- sort(c_v_names)
                c_hash <- paste(digest(paste0(sorted_members, collapse=","), algo = "md5", serialize = FALSE))
                global_scc_traders_map[[c_hash]] <- sorted_members
                result <- c(result, c_hash)
            }
            edge_attr(gs, "weight") <- edge_attr(gs, "weight") - 1
            gs <- delete_edges(gs, which(edge_attr(gs, "weight") == 0))
            gs <- delete_vertices(gs, degree(gs)==0)
        }
        setTxtProgressBar(pb, token_index)
    }
    scc_dt <- data.table(scc_hash=result)[, list(occurrence = .N), by=scc_hash]
    scc_dt$num_traders <- sapply(scc_dt$scc_hash, function(x) {length(global_scc_traders_map[[x]])})
    
    # Extract relevant scc by threshold: relevant_scc_ids <- get_relevant_scc_by_threshold(scc_dt, scc_threshold_rank)

    relevant <- scc_dt[occurrence >= scc_threshold_rank]
    print(paste("Info: Determined", nrow(relevant), "unique SCCs to be relevant at threshold", scc_threshold_rank))
    print(paste("Info: Minimum occurrence is", min(relevant$occurrence)))
    relevant_scc <- relevant$scc_hash

    end_time <- Sys.time()
    elapsed <- end_time - start_time
    print("SCC execution time.")
    print(elapsed)


    # wash_trades_multiple_passes <- detect_and_label_wash_trades_for_scc_using_multiple_passes(trades = trades, relevant_scc = relevant_scc_ids, window_sizes_in_seconds = wash_window_sizes_seconds, ether = wash_trade_detection_ether, margin = wash_trade_detection_margin)
### VOLUME MATCHING
    # copy trades in order to label them
    print(paste("Starting wash trade labeling with", length(wash_window_sizes_seconds), "passes."))
    trades$wash_label <- NA
    
    # take start of first day of given trades
    window_start <- min(trades$cut)
    
    wash_trades <- list()
    
    # run for all given window sizes
    window_size_count <- length(wash_window_sizes_seconds)
    relevant_scc_count <- length(relevant_scc)
    pb <- txtProgressBar(min = 0, max = window_size_count*relevant_scc_count, style = 3)
    for (window_size_index in 1:length(wash_window_sizes_seconds)) {
        
        window_size <- wash_window_sizes_seconds[window_size_index]
        # breaks from start to last timestamp (incl.), by given steps in seconds
        breaks <- seqlast(window_start, max(trades$timestamp), window_size)
        
        # for each relevant SCC
        for (scc.id.index in 1:length(relevant_scc)) {
            scc.id <- relevant_scc[scc.id.index]
            scc.traders <- global_scc_traders_map[[scc.id]]

            # get trades within scc that have not been labeled as wash trades yet
            scc.trades <- trades[eth_seller_id %in% scc.traders & eth_buyer_id %in% scc.traders &
                                    (wash_label == FALSE | is.na(wash_label))][order(cut)]
            
            if(nrow(scc.trades) == 0) {
                wash_trades[[scc.id]][[as.character(window_size)]] <- list()
                next()
            }
            
            # label these trades as FALSE in original trade set to indicate they have been checked
            trades[transactionHash %in% scc.trades$transactionHash]$wash_label <- FALSE

            # prepare trades
            temp_trades <- scc.trades[, .(transactionHash, token, date, timestamp, buyer = eth_seller, seller = eth_buyer, amount = trade_amount_token, trade_amount_dollar, wash_label)]

            # split trades by token and given time window size and run wash trade detect function (time windows are defined as [break, next_break) using right=F and include.lowest=T)
            temp_trades_per_token_and_window <- split(temp_trades, list(temp_trades$token, cut(temp_trades$timestamp, breaks, right = FALSE, include.lowest = TRUE, dig.lab = 12)), drop = TRUE)
            
            scc.wash_trades_all <- lapply(temp_trades_per_token_and_window, FUN = detect_label_wash_trades, margin = wash_trade_detection_margin)
            
            # add to final result
            wash_trades[[scc.id]][[as.character(window_size)]] <- scc.wash_trades_all
            
            # label wash trades in original trade set
            checked_trades <- rbindlist(scc.wash_trades_all)
            trades[transactionHash %in% checked_trades[wash_label == TRUE]$transactionHash]$wash_label <- TRUE
            
            setTxtProgressBar(pb, (window_size_index - 1) * length(relevant_scc) + scc.id.index)
        }
    }

    # wash_trades_multiple_passes_summary <- get_summary_of_wash_trades_per_scc_and_timewindow(wash_trades = wash_trades_multiple_passes, window_size_name = "multiple_windows", multiple_passes = TRUE)
### SUMMARY
    print("Info: producing wash trading summary...")
    wash_trades_dt <- data.table(scc_hash = character(), token = character(), window_size = character(), time = character(), num_wash_trades = numeric(), num_trades = numeric(), total_amount_wash = numeric(), total_amount = numeric(), total_amount_dollar_wash = numeric(), total_amount_dollar = numeric())
    # for each SCC
    for (scc in names(wash_trades)) {
      # for each time window size
      for (window_size in names(wash_trades[[scc]])) {
        # for each time interval
        for (w in seq_len(length(wash_trades[[scc]][[window_size]]))) {
          # list names contain token and time window
          temp <- strsplit(names(wash_trades[[scc]][[window_size]])[w], split = "\\.")[[1]]
          token <- temp[1]
          window <- temp[2]
          num_wash <- nrow(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE])
          num_all <- nrow(wash_trades[[scc]][[window_size]][[w]])
          amount_wash <- sum(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE]$amount)
          amount_all <- sum(wash_trades[[scc]][[window_size]][[w]]$amount)
          amount_dollar_wash <- sum(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE]$trade_amount_dollar)
          amount_dollar_all <- sum(wash_trades[[scc]][[window_size]][[w]]$trade_amount_dollar)
          wash_trades_dt <- rbindlist(list(wash_trades_dt, list(scc, token, window_size, window, num_wash, num_all, amount_wash, amount_all, amount_dollar_wash, amount_dollar_all)))
        }
      }
    }
    fwrite(wash_trades_dt, file = paste0("R_volume_matching_summary.csv"))

    print(nrow(wash_trades[[scc]][[window_size]][[w]][wash_label == TRUE]))
  
    # get_address_clusters(trades = trades, scc_ids = relevant_scc)
### ADDRESS CLUSTERS
    address_clusters <- list()
    
    # for each SCC
    for (scc.id in relevant_scc) {
        scc.traders <- global_scc_traders_map[[scc.id]]
        # add to address clusters
        address_clusters[[as.character(scc.id)]] <- global_trader_hashes[trader_id %in% scc.traders]$trader_address
    }
    
    write(toJSON(address_clusters), file = paste0("R_address_clusters.json"))
  
    return()
}

#### MAIN CALL ####

call_IDEX_pipeline()

# Rscript pipeline.R 