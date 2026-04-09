sync_safe <- function(expr) {
  expr_label <- deparse(substitute(expr), width.cutoff = 120L)
  tryCatch(
    expr,
    error = function(e) {
      message(sprintf("[sync error] %s", expr_label))
      message(conditionMessage(e))
      invisible(NULL)
    }
  )
}

ran_today <- function(source_id, local_path) {
  run <- tryCatch(investdatar::get_latest_sync_run(source_id, local_path = local_path), error = function(e) NULL)
  if (is.null(run) || is.null(run$run_finished_at)) {
    return(FALSE)
  }
  as.Date(run$run_finished_at, tz = "UTC") == Sys.Date()
}

sync_once_per_day <- function(source_id, local_path, expr) {
  expr_label <- deparse(substitute(expr), width.cutoff = 120L)
  if (ran_today(source_id, local_path)) {
    message(sprintf("[sync skipped] %s already ran today: %s", source_id, expr_label))
    return(invisible(NULL))
  }
  sync_safe(expr)
}

sync_once_per_day("ishare_holdings", investdatar::get_source_data_path("ishare", create = TRUE), investdatar::sync_all_ishare_registry_holdings())
sync_once_per_day("rss", investdatar::get_source_data_path("rss", create = TRUE), investdatar::sync_all_rss_registry_data())
sync_once_per_day("treasury", investdatar::get_source_data_path("treasury", create = TRUE), investdatar::sync_all_treasury_rates())
sync_once_per_day("yahoofinance", investdatar::get_source_data_path("yahoofinance", create = TRUE), investdatar::sync_all_yahoofinance_registry_data())

# Use numeric weekday to avoid locale-dependent weekday names.
if (format(Sys.Date(), "%u") == "1") {
  sync_once_per_day("fred", investdatar::get_source_data_path("fred", create = TRUE), investdatar::sync_all_fred_registry_data())
  sync_once_per_day("ishare", investdatar::get_source_data_path("ishare", create = TRUE), investdatar::sync_all_ishare_registry_data())
}

if (format(Sys.Date(), "%d") == "01") {
  sync_once_per_day("wbstats", investdatar::get_source_data_path("wbstats", create = TRUE), investdatar::sync_all_wbstats_registry_data())
}
