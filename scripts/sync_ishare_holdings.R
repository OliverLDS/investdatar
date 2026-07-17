#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_ishare_holdings.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "ishare_holdings",
  path_source = "ishare",
  cadence = "daily",
  sync_call = quote({
    tickers <- parse_tickers_arg(arg_value("--tickers", ""))
    investdatar::sync_all_ishare_registry_holdings(tickers = tickers)
  }),
  description = "Sync configured iShares holdings snapshots into the local iShares cache.",
  extra_help = paste(
    "Provider flags:",
    "  --tickers CSV  Sync only this comma-separated holdings ticker list.",
    "",
    "Default:",
    "  Without --tickers, this node uses iShare.holdings_tickers from",
    "  investdatar_config.yaml, falling back to DYNF, THRO, BAI, BDYN, BDVL.",
    sep = "\n"
  )
)
