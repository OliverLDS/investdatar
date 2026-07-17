#!/usr/bin/env Rscript

file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
this_file <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else "scripts/sync_ishare.R"
source(file.path(dirname(normalizePath(this_file, mustWork = FALSE)), "sync_node_utils.R"))

run_sync_node(
  source_id = "ishare",
  path_source = "ishare",
  cadence = "weekly",
  sync_call = quote({
    registry <- investdatar::get_ishare_registry()
    tickers <- if (has_flag("--all")) NULL else parse_tickers_arg(arg_value("--tickers", ""))
    if (is.null(tickers) && !has_flag("--all")) {
      tickers <- ishare_config_holdings_tickers()
    }
    registry <- filter_ishare_registry(registry, tickers)
    investdatar::sync_all_ishare_registry_data(registry = registry)
  }),
  description = "Sync iShares fund history into the configured local iShares cache.",
  extra_help = paste(
    "Provider flags:",
    "  --tickers CSV  Sync only this comma-separated ticker list.",
    "  --all          Sync the full iShares registry instead of the tracked list.",
    "",
    "Default:",
    "  Without --tickers or --all, this node syncs iShare.holdings_tickers from",
    "  investdatar_config.yaml, falling back to DYNF, THRO, BAI, BDYN, BDVL.",
    sep = "\n"
  )
)
