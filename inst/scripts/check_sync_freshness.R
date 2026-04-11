source_specs <- list(
  list(source_id = "ishare_holdings", path_source = "ishare", cadence = "daily"),
  list(source_id = "rss", path_source = "rss", cadence = "daily"),
  list(source_id = "treasury", path_source = "treasury", cadence = "daily"),
  list(source_id = "yahoofinance", path_source = "yahoofinance", cadence = "daily"),
  list(source_id = "fred", path_source = "fred", cadence = "weekly"),
  list(source_id = "ishare", path_source = "ishare", cadence = "weekly"),
  list(source_id = "wbstats", path_source = "wbstats", cadence = "monthly")
)

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

stale_msgs <- character()

for (spec in source_specs) {
  local_path <- tryCatch(
    investdatar::get_source_data_path(spec$path_source, create = TRUE),
    error = function(e) NULL
  )

  run <- tryCatch(
    investdatar::get_latest_sync_run(spec$source_id, local_path = local_path),
    error = function(e) NULL
  )

  if (is.null(run) || is.null(run$run_finished_at)) {
    stale_msgs <- c(
      stale_msgs,
      sprintf("STALE: %s has no recorded batch sync run.", spec$source_id)
    )
    next
  }

  if (!same_period(run$run_finished_at, spec$cadence)) {
    stale_msgs <- c(
      stale_msgs,
      sprintf(
        "STALE: %s latest sync was %s, behind expected %s cadence.",
        spec$source_id,
        format(as.POSIXct(run$run_finished_at, tz = "UTC"), "%Y-%m-%d %H:%M:%S %Z"),
        spec$cadence
      )
    )
  }
}

if (length(stale_msgs) == 0L) {
  cat("All tracked providers have fresh sync logs for their expected cadence.\n")
  quit(save = "no", status = 0L)
}

cat(paste(stale_msgs, collapse = "\n"), "\n", sep = "")
quit(save = "no", status = 1L)
