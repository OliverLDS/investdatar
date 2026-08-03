#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_eia.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "eia",
  path_source = "eia",
  cadence = "weekly",
  sync_call = quote(investdatar::sync_all_eia_registry_data()),
  description = "Sync registered EIA physical energy fundamentals."
)
