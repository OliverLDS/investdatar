#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

arg_value <- function(flag, default = NULL) {
  idx <- match(flag, args)
  if (is.na(idx) || idx == length(args)) {
    return(default)
  }
  args[[idx + 1L]]
}

flag_present <- function(flag) {
  flag %in% args
}

config_path <- arg_value("--config", Sys.getenv("INVESTDATAR_CONFIG", unset = ""))
max_files <- suppressWarnings(as.integer(arg_value("--max-files", "200")))
if (is.na(max_files) || max_files < 1L) {
  max_files <- 200L
}
pretty_json <- !flag_present("--compact")

load_investdatar <- function() {
  file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  script_path <- if (length(file_arg) > 0L) sub("^--file=", "", file_arg[[1L]]) else getwd()
  script_path <- normalizePath(script_path, mustWork = FALSE)
  repo_root <- normalizePath(file.path(dirname(script_path), ".."), mustWork = FALSE)

  if (file.exists(file.path(repo_root, "DESCRIPTION")) && requireNamespace("pkgload", quietly = TRUE)) {
    suppressPackageStartupMessages(pkgload::load_all(repo_root, quiet = TRUE))
  } else {
    suppressPackageStartupMessages(requireNamespace("investdatar"))
  }
  invisible(TRUE)
}

safe_call <- function(expr, default = NULL) {
  tryCatch(expr, error = function(e) default)
}

iso_time <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) {
    return(NULL)
  }
  if (inherits(x, "Date")) {
    return(as.character(x))
  }
  if (inherits(x, "POSIXt")) {
    return(format(as.POSIXct(x, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"))
  }
  as.character(x)
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

capture_str <- function(x) {
  paste(utils::capture.output(str(x, max.level = 2L, vec.len = 4L, list.len = 16L)), collapse = "\n")
}

candidate_time_columns <- function(dt) {
  preferred <- c("datetime", "date", "published_at", "updated_date", "time", "timestamp")
  preferred <- preferred[preferred %in% names(dt)]
  typed <- names(dt)[vapply(dt, function(x) inherits(x, c("POSIXt", "Date")), logical(1))]
  unique(c(preferred, typed))
}

coerce_time_vector <- function(x) {
  if (inherits(x, "POSIXt") || inherits(x, "Date")) {
    return(x)
  }
  if (is.character(x)) {
    parsed_date <- suppressWarnings(as.Date(x))
    if (sum(!is.na(parsed_date)) > 0L) {
      return(parsed_date)
    }
  }
  NULL
}

infer_frequency <- function(times) {
  if (is.null(times) || length(times) < 2L) {
    return(list(label = "unknown", median_interval_seconds = NULL))
  }

  times <- sort(unique(times[!is.na(times)]))
  if (length(times) < 2L) {
    return(list(label = "unknown", median_interval_seconds = NULL))
  }

  if (inherits(times, "Date")) {
    diffs <- as.numeric(diff(times), units = "days") * 86400
  } else {
    diffs <- as.numeric(diff(as.POSIXct(times, tz = "UTC")), units = "secs")
  }
  diffs <- diffs[is.finite(diffs) & diffs > 0]
  if (length(diffs) == 0L) {
    return(list(label = "unknown", median_interval_seconds = NULL))
  }

  median_seconds <- stats::median(diffs)
  days <- median_seconds / 86400
  label <- if (median_seconds < 60) {
    sprintf("%ss", round(median_seconds, 3))
  } else if (median_seconds < 3600) {
    sprintf("%sm", round(median_seconds / 60, 3))
  } else if (median_seconds < 86400) {
    sprintf("%sh", round(median_seconds / 3600, 3))
  } else if (abs(days - 1) < 0.25) {
    "daily"
  } else if (abs(days - 7) < 1) {
    "weekly"
  } else if (days >= 27 && days <= 32) {
    "monthly"
  } else if (days >= 88 && days <= 93) {
    "quarterly"
  } else if (days >= 360 && days <= 370) {
    "yearly"
  } else {
    sprintf("irregular_%s_days_median", round(days, 3))
  }

  list(label = label, median_interval_seconds = as.numeric(median_seconds))
}

data_stats <- function(x) {
  if (data.table::is.data.table(x) || is.data.frame(x)) {
    dt <- data.table::as.data.table(x)
    time_candidates <- candidate_time_columns(dt)
    time_col <- if (length(time_candidates) > 0L) time_candidates[[1L]] else NULL
    times <- if (!is.null(time_col)) coerce_time_vector(dt[[time_col]]) else NULL
    time_range <- if (!is.null(times) && any(!is.na(times))) {
      list(start = iso_time(min(times, na.rm = TRUE)), end = iso_time(max(times, na.rm = TRUE)))
    } else {
      list(start = NULL, end = NULL)
    }

    return(list(
      nrow = nrow(dt),
      ncol = ncol(dt),
      columns = names(dt),
      frequency = infer_frequency(times),
      time_column = if (is.null(time_col)) NULL else time_col,
      time_range = time_range
    ))
  }

  list(
    nrow = if (!is.null(dim(x))) dim(x)[[1]] else length(x),
    ncol = if (!is.null(dim(x)) && length(dim(x)) >= 2L) dim(x)[[2]] else NULL,
    columns = names(x),
    frequency = list(label = "unknown", median_interval_seconds = NULL),
    time_column = NULL,
    time_range = list(start = NULL, end = NULL)
  )
}

list_data_files <- function(local_path, pattern = "\\.rds$") {
  if (is.null(local_path) || !dir.exists(local_path)) {
    return(character())
  }
  paths <- list.files(local_path, pattern = pattern, recursive = TRUE, full.names = TRUE)
  paths <- paths[!grepl("\\.meta\\.rds$", paths, ignore.case = TRUE)]
  paths <- paths[!grepl("(^|/)_sync_runs/", paths)]
  paths <- paths[!grepl("(^|/)_cache/", paths)]
  info <- file.info(paths)
  paths[order(info$mtime, decreasing = TRUE, na.last = TRUE)]
}

quote_chr <- function(x) {
  sprintf("%s", deparse(as.character(x)))
}

file_stem <- function(path) {
  sub("\\.rds$", "", basename(path), ignore.case = TRUE)
}

sample_code_for_file <- function(source_id, file_path) {
  stem <- file_stem(file_path)
  if (identical(source_id, "fred")) {
    return(sprintf("investdatar::get_local_FRED_data(%s)", quote_chr(stem)))
  }
  if (identical(source_id, "rss")) {
    return(sprintf("investdatar::get_local_rss_data(%s)", quote_chr(stem)))
  }
  if (identical(source_id, "treasury")) {
    return(sprintf("investdatar::get_local_treasury_rates(%s)", quote_chr(stem)))
  }
  if (identical(source_id, "wbstats")) {
    parts <- strsplit(stem, "__", fixed = TRUE)[[1]]
    if (length(parts) >= 3L) {
      return(sprintf(
        "investdatar::get_local_wbstats_data(%s, %s, freq = %s)",
        quote_chr(parts[[1]]), quote_chr(parts[[2]]), quote_chr(parts[[3]])
      ))
    }
  }
  if (identical(source_id, "yahoofinance")) {
    parts <- strsplit(stem, "__", fixed = TRUE)[[1]]
    if (length(parts) >= 3L) {
      return(sprintf(
        "investdatar::get_local_quantmod_OHLC(%s, src = %s, interval = %s)",
        quote_chr(parts[[1]]), quote_chr(parts[[2]]), quote_chr(parts[[3]])
      ))
    }
  }
  if (identical(source_id, "ishare")) {
    if (identical(stem, "mega_data")) {
      return("investdatar::get_local_ishare_mega_data()")
    }
    if (grepl("_historical$", stem)) {
      return(sprintf("investdatar::get_local_ishare_data(%s)", quote_chr(sub("_historical$", "", stem))))
    }
  }
  if (identical(source_id, "ishare_holdings") && grepl("_holdings$", stem)) {
    return(sprintf("investdatar::get_local_ishare_holdings(%s)", quote_chr(sub("_holdings$", "", stem))))
  }
  if (identical(source_id, "okx")) {
    m <- regexec("^(.+)_([^_]+)$", stem)
    parts <- regmatches(stem, m)[[1]]
    if (length(parts) == 3L) {
      return(sprintf("investdatar::get_local_okx_candle(%s, %s)", quote_chr(parts[[2]]), quote_chr(parts[[3]])))
    }
  }
  if (identical(source_id, "binance")) {
    parts <- strsplit(stem, "__", fixed = TRUE)[[1]]
    if (length(parts) >= 2L) {
      return(sprintf("investdatar::get_local_binance_klines(%s, %s)", quote_chr(parts[[1]]), quote_chr(parts[[2]])))
    }
  }
  sprintf("readRDS(%s)", quote_chr(normalizePath(file_path, winslash = "/", mustWork = FALSE)))
}

source_defs <- function() {
  list(
    fred = list(path_expr = function() investdatar::get_source_data_path("fred"), sync_source_id = "fred", spec_source = "fred"),
    wbstats = list(path_expr = function() investdatar::get_source_data_path("wbstats"), sync_source_id = "wbstats", spec_source = "wbstats"),
    treasury = list(path_expr = function() investdatar::get_source_data_path("treasury"), sync_source_id = "treasury", spec_source = "treasury"),
    rss = list(path_expr = function() investdatar::get_source_data_path("rss"), sync_source_id = "rss", spec_source = "rss"),
    ishare = list(path_expr = function() investdatar::get_source_data_path("ishare"), sync_source_id = "ishare", spec_source = "ishare", exclude = "_holdings\\.rds$"),
    ishare_holdings = list(path_expr = function() investdatar::get_source_data_path("ishare"), sync_source_id = "ishare_holdings", spec_source = NULL, pattern = "_holdings\\.rds$"),
    yahoofinance = list(path_expr = function() investdatar::get_source_data_path("yahoofinance"), sync_source_id = "yahoofinance", spec_source = "quantmod"),
    okx = list(path_expr = function() investdatar::get_source_data_path("crypto", subdir = "okx"), sync_source_id = "okx", spec_source = "okx"),
    binance = list(path_expr = function() investdatar::get_source_data_path("crypto", subdir = "binance"), sync_source_id = "binance", spec_source = "binance")
  )
}

summarize_file <- function(source_id, file_path, local_path) {
  obj <- safe_call(readRDS(file_path), default = structure(list(), class = "read_error"))
  read_error <- inherits(obj, "read_error")
  meta <- safe_call(investdatar::get_local_data_meta(file_path), default = NULL)

  if (read_error) {
    return(list(
      file_path = normalizePath(file_path, winslash = "/", mustWork = FALSE),
      relative_path = sub(paste0("^", normalizePath(local_path, winslash = "/", mustWork = FALSE), "/?"), "", normalizePath(file_path, winslash = "/", mustWork = FALSE)),
      load_sample_code = sample_code_for_file(source_id, file_path),
      current_data_structure = NULL,
      basic_statistics = NULL,
      meta_info = list(meta = json_safe(meta), error = "failed_to_read_rds")
    ))
  }

  list(
    file_path = normalizePath(file_path, winslash = "/", mustWork = FALSE),
    relative_path = sub(paste0("^", normalizePath(local_path, winslash = "/", mustWork = FALSE), "/?"), "", normalizePath(file_path, winslash = "/", mustWork = FALSE)),
    load_sample_code = sample_code_for_file(source_id, file_path),
    current_data_structure = capture_str(obj),
    basic_statistics = data_stats(obj),
    meta_info = list(meta = json_safe(meta))
  )
}

aggregate_stats <- function(files) {
  stats <- lapply(files, `[[`, "basic_statistics")
  stats <- Filter(Negate(is.null), stats)
  total_rows <- sum(vapply(stats, function(x) x$nrow %||% 0L, numeric(1)), na.rm = TRUE)
  starts <- unlist(lapply(stats, function(x) x$time_range$start), use.names = FALSE)
  ends <- unlist(lapply(stats, function(x) x$time_range$end), use.names = FALSE)
  frequencies <- unique(unlist(lapply(stats, function(x) x$frequency$label), use.names = FALSE))

  list(
    files_read = length(stats),
    nrow = as.integer(total_rows),
    total_rows = as.integer(total_rows),
    frequency = if (length(frequencies) == 0L) NULL else frequencies,
    time_range = list(
      start = if (length(starts) == 0L) NULL else min(starts, na.rm = TRUE),
      end = if (length(ends) == 0L) NULL else max(ends, na.rm = TRUE)
    )
  )
}

aggregate_file_meta <- function(files) {
  metas <- lapply(files, function(x) x$meta_info$meta)
  metas <- Filter(Negate(is.null), metas)
  local_times <- unlist(lapply(metas, `[[`, "local_updated_at"), use.names = FALSE)
  source_times <- unlist(lapply(metas, `[[`, "source_updated_at"), use.names = FALSE)
  latest_value <- function(x) {
    x <- x[!is.na(x) & nzchar(x)]
    if (length(x) == 0L) {
      return(NULL)
    }
    max(x)
  }

  list(
    files_with_meta = length(metas),
    latest_file_local_updated_at = latest_value(local_times),
    latest_file_source_updated_at = latest_value(source_times)
  )
}

summarize_latest_sync_run <- function(run) {
  if (is.null(run)) {
    return(NULL)
  }

  summary_dt <- safe_call(data.table::as.data.table(run$summary), default = NULL)
  status_counts <- NULL
  updated_count <- NULL
  error_count <- NULL
  summary_rows <- NULL

  if (!is.null(summary_dt)) {
    summary_rows <- nrow(summary_dt)
    if ("status" %in% names(summary_dt)) {
      status_counts <- as.list(table(summary_dt$status, useNA = "ifany"))
    }
    if ("updated" %in% names(summary_dt)) {
      updated_values <- suppressWarnings(as.logical(summary_dt$updated))
      updated_count <- sum(updated_values %in% TRUE, na.rm = TRUE)
    }
    if ("error" %in% names(summary_dt)) {
      error_count <- sum(!is.na(summary_dt$error) & nzchar(as.character(summary_dt$error)))
    }
  }

  list(
    source_id = run$source_id,
    run_started_at = iso_time(run$run_started_at),
    run_finished_at = iso_time(run$run_finished_at),
    params = json_safe(run$params),
    summary_rows = summary_rows,
    status_counts = status_counts,
    updated_count = updated_count,
    error_count = error_count
  )
}

`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

summarize_source <- function(source_id, def) {
  local_path <- safe_call(def$path_expr(), default = NULL)
  path_error <- NULL
  if (is.null(local_path)) {
    path_error <- "local_path_not_configured"
  }

  paths <- list_data_files(local_path, pattern = def$pattern %||% "\\.rds$")
  if (!is.null(def$exclude)) {
    paths <- paths[!grepl(def$exclude, basename(paths))]
  }
  file_count <- length(paths)
  paths <- utils::head(paths, max_files)
  files <- lapply(paths, summarize_file, source_id = source_id, local_path = local_path)
  latest_sync <- safe_call(investdatar::get_latest_sync_run(def$sync_source_id, local_path = local_path), default = NULL)
  spec <- if (!is.null(def$spec_source)) safe_call(investdatar::get_source_spec(def$spec_source), default = NULL) else NULL

  list(
    data_source = source_id,
    local_cache_path = if (is.null(local_path)) NULL else normalizePath(local_path, winslash = "/", mustWork = FALSE),
    file_count = file_count,
    reported_file_count = length(files),
    load_sample_code = if (length(files) > 0L) files[[1L]]$load_sample_code else NULL,
    current_data_structure = if (length(files) > 0L) files[[1L]]$current_data_structure else NULL,
    basic_statistics = aggregate_stats(files),
    meta_info = list(
      source_spec = json_safe(spec),
      file_meta_summary = aggregate_file_meta(files),
      latest_sync_run = summarize_latest_sync_run(latest_sync),
      error = path_error
    ),
    files = files
  )
}

suppressWarnings(load_investdatar())
if (nzchar(config_path)) {
  invisible(safe_call(investdatar::load_investdatar_config(config_path), default = NULL))
}
if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required to emit JSON.", call. = FALSE)
}
if (!requireNamespace("data.table", quietly = TRUE)) {
  stop("Package 'data.table' is required to inspect cached data.", call. = FALSE)
}

out <- lapply(names(source_defs()), function(source_id) {
  summarize_source(source_id, source_defs()[[source_id]])
})
names(out) <- names(source_defs())

cat(jsonlite::toJSON(json_safe(out), auto_unbox = TRUE, pretty = pretty_json, null = "null", na = "null"))
cat("\n")
