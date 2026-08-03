#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_sdmx.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "sdmx",
  path_source = "sdmx",
  cadence = "daily",
  sync_call = quote(investdatar::sync_all_sdmx_registry_data()),
  description = "Sync registered ECB, OECD, and BIS SDMX series."
)
