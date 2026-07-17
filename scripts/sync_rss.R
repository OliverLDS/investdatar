#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_rss.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "rss",
  path_source = "rss",
  cadence = "daily",
  sync_call = quote(investdatar::sync_all_rss_registry_data()),
  description = "Sync registered RSS feeds into the configured investdatar RSS cache."
)
