#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_wbstats.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "wbstats",
  path_source = "wbstats",
  cadence = "monthly",
  sync_call = quote(investdatar::sync_all_wbstats_registry_data()),
  description = "Sync registered World Bank indicators into the configured local World Bank cache."
)
