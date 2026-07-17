#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_fred.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "fred",
  path_source = "fred",
  cadence = "weekly",
  sync_call = quote(investdatar::sync_all_fred_registry_data()),
  description = "Sync registered FRED series into the configured local FRED cache."
)
