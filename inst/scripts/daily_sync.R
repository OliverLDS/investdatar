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

same_period <- function(run_time, cadence, now = Sys.time()) {
  if (is.null(run_time) || is.na(run_time)) {
    return(FALSE)
  }

  run_time <- as.POSIXct(run_time, tz = "UTC")
  now <- as.POSIXct(now, tz = "UTC")

  switch(
    cadence,
    daily = identical(as.Date(run_time), as.Date(now)),
    weekly = identical(format(run_time, "%G-%V"), format(now, "%G-%V")),
    monthly = identical(format(run_time, "%Y-%m"), format(now, "%Y-%m")),
    FALSE
  )
}

already_synced_in_period <- function(source_id, local_path, cadence = "daily") {
  run <- tryCatch(investdatar::get_latest_sync_run(source_id, local_path = local_path), error = function(e) NULL)
  if (is.null(run) || is.null(run$run_finished_at)) {
    return(FALSE)
  }
  same_period(run$run_finished_at, cadence = cadence)
}

sync_if_stale <- function(source_id, local_path, cadence = "daily", expr) {
  expr_label <- deparse(substitute(expr), width.cutoff = 120L)
  if (already_synced_in_period(source_id, local_path, cadence = cadence)) {
    message(sprintf("[sync skipped] %s already ran in the current %s period: %s", source_id, cadence, expr_label))
    return(invisible(NULL))
  }
  sync_safe(expr)
}

sync_if_stale("ishare_holdings", investdatar::get_source_data_path("ishare", create = TRUE), "daily", investdatar::sync_all_ishare_registry_holdings())
sync_if_stale("rss", investdatar::get_source_data_path("rss", create = TRUE), "daily", investdatar::sync_all_rss_registry_data())
sync_if_stale("treasury", investdatar::get_source_data_path("treasury", create = TRUE), "daily", investdatar::sync_all_treasury_rates())
sync_if_stale("yahoofinance", investdatar::get_source_data_path("yahoofinance", create = TRUE), "daily", investdatar::sync_all_yahoofinance_registry_data())
sync_if_stale("fred", investdatar::get_source_data_path("fred", create = TRUE), "weekly", investdatar::sync_all_fred_registry_data())
sync_if_stale("ishare", investdatar::get_source_data_path("ishare", create = TRUE), "weekly", investdatar::sync_all_ishare_registry_data())
sync_if_stale("wbstats", investdatar::get_source_data_path("wbstats", create = TRUE), "monthly", investdatar::sync_all_wbstats_registry_data())
