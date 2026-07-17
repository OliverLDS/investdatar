args <- commandArgs(trailingOnly = TRUE)

arg_value <- function(flag, default = NULL) {
  idx <- match(flag, args)
  if (is.na(idx) || idx == length(args)) {
    return(default)
  }
  args[[idx + 1L]]
}

has_flag <- function(...) {
  any(unlist(list(...)) %in% args)
}

script_path <- function() {
  file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(file_arg) == 0L) {
    return(normalizePath(getwd(), mustWork = FALSE))
  }
  normalizePath(sub("^--file=", "", file_arg[[1L]]), mustWork = FALSE)
}

load_local_investdatar <- function(config_path = Sys.getenv("INVESTDATAR_CONFIG", unset = "")) {
  path <- script_path()
  repo_root <- normalizePath(file.path(dirname(path), ".."), mustWork = FALSE)

  if (file.exists(file.path(repo_root, "DESCRIPTION")) && requireNamespace("pkgload", quietly = TRUE)) {
    suppressPackageStartupMessages(pkgload::load_all(repo_root, quiet = TRUE))
  } else {
    suppressPackageStartupMessages(requireNamespace("investdatar"))
  }

  if (nzchar(config_path)) {
    invisible(investdatar::load_investdatar_config(config_path))
  }
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

iso_time <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) {
    return(NULL)
  }
  format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
}

json_safe <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }
  if (inherits(x, "POSIXt") || inherits(x, "Date")) {
    return(iso_time(x))
  }
  if (data.table::is.data.table(x) || is.data.frame(x)) {
    return(lapply(as.data.frame(x), json_safe))
  }
  if (is.list(x)) {
    return(lapply(x, json_safe))
  }
  if (is.factor(x)) {
    return(as.character(x))
  }
  if (is.atomic(x)) {
    return(x)
  }
  as.character(x)
}

summarize_sync_result <- function(result) {
  if (is.null(result)) {
    return(NULL)
  }
  dt <- tryCatch(data.table::as.data.table(result), error = function(e) NULL)
  if (is.null(dt)) {
    return(json_safe(result))
  }

  out <- list(n_rows = nrow(dt))
  if ("status" %in% names(dt)) {
    out$status_counts <- as.list(table(dt$status, useNA = "ifany"))
  }
  if ("updated" %in% names(dt)) {
    out$updated_count <- sum(suppressWarnings(as.logical(dt$updated)) %in% TRUE, na.rm = TRUE)
  }
  if ("error" %in% names(dt)) {
    out$error_count <- sum(!is.na(dt$error) & nzchar(as.character(dt$error)))
  }
  out
}

latest_sync_log_path <- function(source_id, local_path) {
  run_dir <- file.path(local_path, "_sync_runs")
  if (!dir.exists(run_dir)) {
    return(NULL)
  }
  safe_id <- gsub("[^A-Za-z0-9._-]+", "_", source_id)
  paths <- list.files(run_dir, pattern = sprintf("^%s__.*\\.rds$", safe_id), full.names = TRUE)
  if (length(paths) == 0L) {
    return(NULL)
  }
  normalizePath(sort(paths)[[length(paths)]], winslash = "/", mustWork = FALSE)
}

emit_json <- function(x, pretty = !has_flag("--compact")) {
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required for node JSON output.", call. = FALSE)
  }
  cat(jsonlite::toJSON(json_safe(x), auto_unbox = TRUE, pretty = pretty, null = "null", na = "null"))
  cat("\n")
}

print_sync_help <- function(source_id, description, extra_help = NULL) {
  cat(sprintf("%s\n\n", description))
  cat(sprintf("Usage: Rscript scripts/sync_%s.R [--config PATH] [--force] [--compact]\n\n", source_id))
  cat("Flags:\n")
  cat("  -h, --help     Show this help text.\n")
  cat("  --config PATH  Load this investdatar YAML config before syncing.\n")
  cat("  --force        Sync even if the latest run log is fresh for this cadence.\n")
  cat("  --compact      Emit compact JSON instead of pretty JSON.\n\n")
  if (!is.null(extra_help) && nzchar(extra_help)) {
    cat(extra_help)
    if (!grepl("\n$", extra_help)) {
      cat("\n")
    }
    cat("\n")
  }
  cat("Side effects:\n")
  cat("  Updates the configured local cache for this provider and writes a batch\n")
  cat("  sync log under <provider_data_path>/_sync_runs when the provider sync runs.\n")
}

run_sync_node <- function(source_id, path_source, cadence, sync_call, description,
                          extra_help = NULL) {
  if (has_flag("-h", "--help")) {
    print_sync_help(source_id, description, extra_help = extra_help)
    quit(save = "no", status = 0L)
  }

  started_at <- Sys.time()
  force <- has_flag("--force")
  config_path <- arg_value("--config", Sys.getenv("INVESTDATAR_CONFIG", unset = ""))
  output <- tryCatch(
    {
      load_local_investdatar(config_path = config_path)
      local_path <- investdatar::get_source_data_path(path_source, create = TRUE)
      previous_run <- tryCatch(investdatar::get_latest_sync_run(source_id, local_path = local_path), error = function(e) NULL)
      fresh <- !is.null(previous_run) && same_period(previous_run$run_finished_at, cadence = cadence)

      if (fresh && !force) {
        list(
          success = TRUE,
          source_id = source_id,
          skipped = TRUE,
          cadence = cadence,
          local_path = normalizePath(local_path, winslash = "/", mustWork = FALSE),
          previous_run_finished_at = iso_time(previous_run$run_finished_at),
          run_log_path = latest_sync_log_path(source_id, local_path),
          result_summary = NULL,
          error = NULL,
          started_at = iso_time(started_at),
          finished_at = iso_time(Sys.time())
        )
      } else {
        result <- eval(sync_call)
        latest_run <- tryCatch(investdatar::get_latest_sync_run(source_id, local_path = local_path), error = function(e) NULL)
        list(
          success = TRUE,
          source_id = source_id,
          skipped = FALSE,
          cadence = cadence,
          local_path = normalizePath(local_path, winslash = "/", mustWork = FALSE),
          previous_run_finished_at = if (is.null(previous_run)) NULL else iso_time(previous_run$run_finished_at),
          run_finished_at = if (is.null(latest_run)) NULL else iso_time(latest_run$run_finished_at),
          run_log_path = latest_sync_log_path(source_id, local_path),
          result_summary = summarize_sync_result(result),
          error = NULL,
          started_at = iso_time(started_at),
          finished_at = iso_time(Sys.time())
        )
      }
    },
    error = function(e) {
      list(
        success = FALSE,
        source_id = source_id,
        skipped = FALSE,
        cadence = cadence,
        local_path = NULL,
        run_log_path = NULL,
        result_summary = NULL,
        error = conditionMessage(e),
        started_at = iso_time(started_at),
        finished_at = iso_time(Sys.time())
      )
    }
  )

  emit_json(output)
  quit(save = "no", status = if (isTRUE(output$success)) 0L else 1L)
}

parse_tickers_arg <- function(value) {
  if (is.null(value) || !nzchar(value)) {
    return(NULL)
  }
  tickers <- unlist(strsplit(value, ",", fixed = TRUE), use.names = FALSE)
  tickers <- unique(trimws(tickers))
  tickers[nzchar(tickers)]
}

ishare_config_holdings_tickers <- function(default = c("DYNF", "THRO", "BAI", "BDYN", "BDVL")) {
  cfg <- tryCatch(investdatar::get_source_config("ishare"), error = function(e) list())
  tickers <- cfg$holdings_tickers
  if (is.null(tickers)) {
    return(default)
  }
  tickers <- unique(trimws(as.character(tickers)))
  tickers[nzchar(tickers)]
}

filter_ishare_registry <- function(registry, tickers) {
  if (is.null(tickers)) {
    return(registry)
  }
  ticker_value <- tickers
  registry[ticker %in% ticker_value]
}
