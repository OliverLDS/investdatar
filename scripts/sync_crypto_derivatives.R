#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_crypto_derivatives.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "crypto_derivatives",
  path_source = "crypto",
  path_subdir = "derivatives",
  cadence = "daily",
  sync_call = quote(investdatar::sync_all_crypto_derivatives_registry_data()),
  description = "Sync registered Binance and OKX historical derivatives series."
)
