.quantmod_get_symbols <- function(ticker, src, from, to) {
  suppressWarnings(quantmod::getSymbols(ticker, src = src, auto.assign = FALSE, from = from, to = to))
}

.quantmod_retry_delay <- function(attempt, retry_delay_seconds, max_delay_seconds = 30) {
  min(as.numeric(retry_delay_seconds) * 2^(attempt - 1L), max_delay_seconds)
}

.new_quantmod_error <- function(ticker, src, attempts, parent) {
  structure(
    list(
      message = sprintf(
        "quantmod failed to fetch '%s' from source '%s' after %s attempt(s): %s",
        ticker, src, attempts, conditionMessage(parent)
      ),
      call = NULL,
      ticker = ticker,
      src = src,
      attempts = attempts,
      parent = parent
    ),
    class = c("investdatar_quantmod_error", "error", "condition")
  )
}

.quantmod_complete_ohlc_rows <- function(dt) {
  dt <- .as_data_table(dt)
  required <- c("open", "high", "low", "close")
  if (is.null(dt) || !all(required %in% names(dt))) return(logical(0))
  Reduce(`&`, lapply(required, function(nm) is.finite(dt[[nm]])))
}

.new_quantmod_incomplete_window_error <- function(ticker, src, from, to, dt, reason) {
  valid <- .quantmod_complete_ohlc_rows(dt)
  observed_dates <- if (length(valid) > 0L) as.Date(dt$date[valid]) else as.Date(character())
  structure(
    list(
      message = sprintf(
        paste0(
          "incomplete OHLC window for '%s' from source '%s': %s ",
          "(requested %s through %s; observed %s through %s; valid rows %s of %s)"
        ),
        ticker, src, reason, as.Date(from), as.Date(to),
        if (length(observed_dates)) min(observed_dates) else NA_character_,
        if (length(observed_dates)) max(observed_dates) else NA_character_,
        sum(valid), nrow(dt)
      ),
      call = NULL,
      ticker = ticker,
      src = src,
      from = as.Date(from),
      to = as.Date(to),
      reason = reason
    ),
    class = c("investdatar_incomplete_window_error", "error", "condition")
  )
}

.validate_quantmod_ohlc_window <- function(dt, ticker, src, from, to,
                                            require_start_coverage = FALSE,
                                            max_edge_gap_days = 7L) {
  dt <- .as_data_table(dt)
  if (is.null(dt) || nrow(dt) == 0L) {
    stop(.new_quantmod_incomplete_window_error(ticker, src, from, to, data.table::data.table(), "no rows returned"))
  }
  valid <- .quantmod_complete_ohlc_rows(dt)
  if (length(valid) != nrow(dt) || !any(valid)) {
    stop(.new_quantmod_incomplete_window_error(ticker, src, from, to, dt, "no finite OHLC observations"))
  }
  invalid_ohlc_rows <- sum(!valid)
  dt <- dt[valid]

  requested_from <- as.Date(from)
  requested_to <- min(as.Date(to), Sys.Date())
  observed_from <- min(dt$date)
  observed_to <- max(dt$date)
  max_edge_gap_days <- max(0L, as.integer(max_edge_gap_days))
  expected_weekdays <- sum(!(weekdays(seq(requested_from, requested_to, by = "day")) %in% c("Saturday", "Sunday")))
  minimum_rows <- max(1L, ceiling(expected_weekdays / 2))

  incomplete_start <- isTRUE(require_start_coverage) &&
    (observed_from > requested_from + max_edge_gap_days || nrow(dt) < minimum_rows)
  incomplete_end <- observed_to < requested_to - max_edge_gap_days
  if (incomplete_start || incomplete_end) {
    stop(.new_quantmod_incomplete_window_error(ticker, src, from, to, dt, "returned coverage is materially shorter than requested"))
  }
  attr(dt, "investdatar_invalid_ohlc_rows") <- invalid_ohlc_rows
  dt
}

.quantmod_xts_to_ohlc <- function(x, label, src) {
  cn <- colnames(x)
  open_col <- grep("\\.Open$", cn, value = TRUE)
  high_col <- grep("\\.High$", cn, value = TRUE)
  low_col <- grep("\\.Low$", cn, value = TRUE)
  close_col <- grep("\\.Close$", cn, value = TRUE)
  adj_col <- grep("\\.Adjusted$", cn, value = TRUE)
  volume_col <- grep("\\.Volume$", cn, value = TRUE)

  x_dt <- data.table::data.table(
    date = as.Date(zoo::index(x)), open = NA_real_, high = NA_real_, low = NA_real_,
    close = NA_real_, volume = NA_real_, adj_close = NA_real_, symbol = label
  )
  if (length(open_col) == 1L) x_dt[, open := as.numeric(x[, open_col])]
  if (length(high_col) == 1L) x_dt[, high := as.numeric(x[, high_col])]
  if (length(low_col) == 1L) x_dt[, low := as.numeric(x[, low_col])]
  if (length(close_col) == 1L) x_dt[, close := as.numeric(x[, close_col])]
  if (length(volume_col) == 1L) x_dt[, volume := as.numeric(x[, volume_col])]
  if (length(adj_col) == 1L) x_dt[, adj_close := as.numeric(x[, adj_col])]

  .standardize_market_ohlcv(
    x_dt, source = paste0("quantmod_", src), symbol = label,
    interval = "1d", time_col = "date"
  )
}

.fetch_eastmoney_ohlc <- function(ticker, label, from, to) {
  payload <- .http_get_json(
    "https://push2his.eastmoney.com/api/qt/stock/kline/get",
    query = list(
      secid = ticker, klt = "101", fqt = "0",
      beg = format(as.Date(from), "%Y%m%d"), end = format(as.Date(to), "%Y%m%d"),
      fields1 = "f1,f2,f3,f4,f5,f6",
      fields2 = "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61"
    )
  )
  klines <- payload$data$klines
  if (is.null(klines) || length(klines) == 0L) {
    stop("Eastmoney fallback returned no daily data for ", ticker, call. = FALSE)
  }
  fields <- strsplit(as.character(klines), ",", fixed = TRUE)
  if (any(lengths(fields) < 6L)) {
    stop("Eastmoney fallback returned malformed daily data for ", ticker, call. = FALSE)
  }
  dt <- data.table::rbindlist(lapply(fields, function(x) {
    data.table::data.table(
      date = as.Date(x[[1L]]), open = as.numeric(x[[2L]]), close = as.numeric(x[[3L]]),
      high = as.numeric(x[[4L]]), low = as.numeric(x[[5L]]), volume = as.numeric(x[[6L]]),
      adj_close = NA_real_, symbol = label
    )
  }))
  .standardize_market_ohlcv(dt, source = "eastmoney", symbol = label, interval = "1d", time_col = "date")
}

.yahoo_chart_range_for_dates <- function(from) {
  age_days <- as.integer(Sys.Date() - as.Date(from))
  ranges <- c(`1mo` = 31L, `3mo` = 92L, `6mo` = 184L, `1y` = 366L,
              `2y` = 732L, `5y` = 1830L, `10y` = 3660L)
  matching <- names(ranges)[age_days <= ranges]
  if (length(matching) == 0L) "max" else matching[[1L]]
}

.yahoo_chart_numeric <- function(x, n) {
  if (is.null(x) || length(x) != n) return(rep(NA_real_, n))
  vapply(x, function(value) {
    if (is.null(value) || length(value) == 0L) NA_real_ else as.numeric(value[[1L]])
  }, numeric(1))
}

.fetch_yahoo_chart_range_ohlc <- function(ticker, label, from, to) {
  url <- paste0("https://query1.finance.yahoo.com/v8/finance/chart/", utils::URLencode(ticker, reserved = TRUE))
  response <- .http_request(
    "GET", url,
    query = list(
      range = .yahoo_chart_range_for_dates(from), interval = "1d",
      events = "history", includeAdjustedClose = "true"
    )
  )
  payload <- jsonlite::fromJSON(.http_response_text(response), simplifyVector = FALSE)
  result <- payload$chart$result[[1L]]
  if (is.null(result) || is.null(result$timestamp) || is.null(result$indicators$quote[[1L]])) {
    stop("Yahoo chart range fallback returned no usable chart result for ", ticker, call. = FALSE)
  }
  timestamps <- unlist(result$timestamp, use.names = FALSE)
  quote <- result$indicators$quote[[1L]]
  n <- length(timestamps)
  adjclose <- result$indicators$adjclose[[1L]]$adjclose
  dt <- data.table::data.table(
    date = as.Date(as.POSIXct(timestamps, origin = "1970-01-01", tz = "UTC")),
    open = .yahoo_chart_numeric(quote$open, n), high = .yahoo_chart_numeric(quote$high, n),
    low = .yahoo_chart_numeric(quote$low, n), close = .yahoo_chart_numeric(quote$close, n),
    volume = .yahoo_chart_numeric(quote$volume, n), adj_close = .yahoo_chart_numeric(adjclose, n),
    symbol = label
  )
  dt <- dt[date >= as.Date(from) & date <= as.Date(to)]
  .standardize_market_ohlcv(dt, source = "quantmod_yahoo", symbol = label, interval = "1d", time_col = "date")
}

#' Fetch Market OHLCV Through quantmod
#'
#' Returns a standardized OHLCV `data.table` with common market-schema columns:
#' `source`, `symbol`, `interval`, `datetime`, `date`, `open`, `high`, `low`,
#' `close`, and `volume`.
#'
#' @param ticker Market symbol passed to `quantmod::getSymbols()`.
#' @param label Optional label to store in the standardized `symbol` column.
#' @param from Start date.
#' @param to End date.
#' @param src quantmod source, default `"yahoo"`.
#' @param raw_data Logical. If `TRUE`, return the raw xts object.
#' @param max_attempts Maximum bounded attempts for a transient source failure.
#' @param retry_delay_seconds Initial retry delay in seconds; delays use
#'   exponential backoff.
#' @param fallback_source Optional explicitly configured fallback provider.
#'   Currently supports `"eastmoney"` for daily OHLC data. For Yahoo sources,
#'   a failed dated quantmod request first retries Yahoo's chart endpoint with a
#'   bounded range before this external fallback is considered.
#' @param fallback_ticker Optional provider-specific fallback identifier.
#' @param require_start_coverage Logical. Require material coverage from `from`.
#'   `sync_local_quantmod_OHLC()` enables this only when valid local bars already
#'   establish the instrument's history.
#'
#' @return `data.table` or raw xts object when `raw_data = TRUE`.
#'
#' @details A row is usable only if open, high, low, and close are finite.
#' Isolated invalid rows are discarded. A window is materially incomplete when
#' its end is more than seven calendar days behind the requested end, or, for
#' an instrument with valid local history, when its start is more than seven
#' calendar days late or fewer than half of the requested weekdays are present.
#' The start rule is not applied to a newly listed instrument, and the
#' calendar-day grace prevents weekend and market-holiday false positives.
#' @export
fetch_quantmod_OHLC <- function(ticker, label = ticker, from, to, src = "yahoo", raw_data = FALSE,
                                max_attempts = 3L, retry_delay_seconds = 1,
                                fallback_source = NULL, fallback_ticker = ticker,
                                require_start_coverage = FALSE) {
  .require_suggested_package("quantmod", "to fetch OHLC data.")
  .require_suggested_package("zoo", "to fetch OHLC data.")
  max_attempts <- max(1L, as.integer(max_attempts))
  last_error <- NULL
  for (attempt in seq_len(max_attempts)) {
    fetched <- tryCatch(
      .quantmod_get_symbols(ticker = ticker, src = src, from = from, to = to),
      error = function(e) e
    )
    if (!inherits(fetched, "error")) {
      if (raw_data) return(fetched)
      dt <- .quantmod_xts_to_ohlc(fetched, label = label, src = src)
      validation <- tryCatch(
        .validate_quantmod_ohlc_window(
          dt, ticker = ticker, src = src, from = from, to = to,
          require_start_coverage = require_start_coverage
        ),
        error = function(e) e
      )
      if (!inherits(validation, "error")) {
        dt <- validation
        attr(dt, "investdatar_fetch_method") <- "quantmod"
        attr(dt, "investdatar_fetch_attempts") <- attempt
        return(dt)
      }
      last_error <- validation
    } else {
      last_error <- fetched
    }
    if (attempt < max_attempts) Sys.sleep(.quantmod_retry_delay(attempt, retry_delay_seconds))
  }

  primary_error <- last_error
  primary_error_message <- if (is.null(primary_error)) NA_character_ else conditionMessage(primary_error)
  primary_error_class <- if (is.null(primary_error)) NA_character_ else class(primary_error)[[1L]]

  # Yahoo's chart endpoint occasionally succeeds where quantmod's dated request
  # fails. This remains Yahoo data and is attempted before any external source.
  if (identical(src, "yahoo") && !isTRUE(raw_data)) {
    yahoo_range <- tryCatch(
      .fetch_yahoo_chart_range_ohlc(ticker, label = label, from = from, to = to),
      error = function(e) e
    )
    if (!inherits(yahoo_range, "error")) {
      yahoo_range <- tryCatch(
        .validate_quantmod_ohlc_window(
          yahoo_range, ticker = ticker, src = src, from = from, to = to,
          require_start_coverage = require_start_coverage
        ),
        error = function(e) e
      )
    }
    if (!inherits(yahoo_range, "error")) {
      attr(yahoo_range, "investdatar_fetch_method") <- "yahoo_chart_range_fallback"
      attr(yahoo_range, "investdatar_fetch_attempts") <- max_attempts
      attr(yahoo_range, "investdatar_primary_error") <- primary_error_message
      attr(yahoo_range, "investdatar_primary_error_class") <- primary_error_class
      return(yahoo_range)
    }
    last_error <- yahoo_range
  }

  if (!is.null(fallback_source) && !isTRUE(raw_data)) {
    fallback_source <- tolower(as.character(fallback_source))
    if (is.na(fallback_source) || !nzchar(fallback_source)) fallback_source <- NULL
  }
  if (!is.null(fallback_source) && !isTRUE(raw_data)) {
    fallback <- tryCatch(
      switch(
        fallback_source,
        eastmoney = .fetch_eastmoney_ohlc(fallback_ticker, label = label, from = from, to = to),
        stop("Unsupported market-data fallback source: ", fallback_source, call. = FALSE)
      ),
      error = function(e) e
    )
    if (!inherits(fallback, "error")) {
      fallback <- .validate_quantmod_ohlc_window(
        fallback, ticker = ticker, src = fallback_source, from = from, to = to,
        require_start_coverage = require_start_coverage
      )
      attr(fallback, "investdatar_fetch_method") <- paste0(fallback_source, "_fallback")
      attr(fallback, "investdatar_fetch_attempts") <- max_attempts
      attr(fallback, "investdatar_primary_error") <- primary_error_message
      attr(fallback, "investdatar_primary_error_class") <- primary_error_class
      return(fallback)
    }
    last_error <- fallback
  }
  stop(.new_quantmod_error(ticker, src, max_attempts, last_error))
}

.quantmod_local_filename <- function(label, src = "yahoo", interval = "1d") {
  label <- gsub("[^A-Za-z0-9._-]+", "_", label)
  src <- gsub("[^A-Za-z0-9._-]+", "_", src)
  interval <- gsub("[^A-Za-z0-9._-]+", "_", interval)
  sprintf("%s__%s__%s.rds", label, src, interval)
}

.quantmod_default_local_path <- function(src = "yahoo", create = FALSE) {
  if (identical(src, "yahoo")) {
    return(get_source_data_path("yahoofinance", create = create))
  }

  stop("A local_path must be supplied for quantmod sources other than 'yahoo'.")
}

.quantmod_latest_local_date <- function(label, src = "yahoo", interval = "1d", local_path = NULL) {
  dt <- tryCatch(
    get_local_quantmod_OHLC(label = label, src = src, interval = interval, local_path = local_path),
    error = function(e) NULL
  )
  if (is.null(dt) || nrow(dt) == 0L || !"date" %in% names(dt)) {
    return(as.Date(NA))
  }
  valid <- .quantmod_complete_ohlc_rows(dt)
  if (length(valid) != nrow(dt) || !any(valid)) return(as.Date(NA))
  max(dt$date[valid], na.rm = TRUE)
}

#' Get Yahoo Finance Registry File Path
#'
#' Resolve the JSON registry path for Yahoo Finance ticker metadata. If no
#' explicit `registry_file` is configured, the function falls back to a default
#' filename in the package config directory.
#'
#' @param config_dir Optional configuration directory used for the fallback
#'   registry path.
#'
#' @return Character scalar path.
#' @export
get_yahoofinance_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("yahoofinance"), error = function(e) list())
  registry_file <- cfg$registry_file

  if (is.null(registry_file) || !nzchar(registry_file)) {
    if (is.null(config_dir)) {
      config_dir <- getOption("investdatar.config_dir")
    }
    if (is.null(config_dir) || !nzchar(config_dir)) {
      stop(
        "No YahooFinance registry path is configured. Set YahooFinance.registry_file in your ",
        "config or load a config file rooted at the desired directory."
      )
    }
    return(file.path(config_dir, "YahooFinance_ticker_registry.json"))
  }

  .normalize_scalar_path(registry_file, config_dir = getOption("investdatar.config_dir"))
}

#' Get Yahoo Finance Registry
#'
#' @param registry_path Optional registry JSON path.
#'
#' @return `data.table`.
#' @export
get_yahoofinance_registry <- function(registry_path = get_yahoofinance_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("yahoo_finance_ticker", "definition", "main_asset_type", "second_asset_type", "geography")
  )
}

#' Get Yahoo Finance Seed Registry Path
#'
#' @return Path to the package-managed Yahoo Finance registry seed.
#' @export
get_yahoofinance_seed_registry_path <- function() {
  path <- system.file("extdata", "config", "YahooFinance_ticker_registry.json", package = "investdatar")
  if (!nzchar(path) || !file.exists(path)) {
    stop("The packaged Yahoo Finance registry seed is unavailable.", call. = FALSE)
  }
  path
}

#' Validate Yahoo Finance Runtime Registry
#'
#' Compare fallback declarations in a runtime registry with the package-managed
#' seed. Additional runtime-only ticker metadata is permitted; every seed
#' fallback declaration must be present unchanged.
#'
#' @param registry_path Runtime registry JSON path.
#' @param seed_path Package seed registry JSON path.
#'
#' @return A list with `valid`, `missing`, and `mismatched` data tables.
#' @export
validate_yahoofinance_registry <- function(registry_path = get_yahoofinance_registry_file_path(),
                                            seed_path = get_yahoofinance_seed_registry_path()) {
  required <- c("yahoo_finance_ticker", "fallback_source", "fallback_ticker")
  seed <- .read_json_registry(seed_path, empty_cols = required)
  seed <- seed[!is.na(fallback_source) & nzchar(fallback_source)]
  runtime <- if (file.exists(registry_path)) {
    .read_json_registry(registry_path, empty_cols = required)
  } else {
    data.table::data.table(yahoo_finance_ticker = character(), fallback_source = character(), fallback_ticker = character())
  }
  runtime <- runtime[, ..required]
  missing <- seed[!runtime, on = "yahoo_finance_ticker"]
  matched <- seed[runtime, on = "yahoo_finance_ticker", nomatch = 0L, allow.cartesian = FALSE]
  mismatched <- matched[
    fallback_source != i.fallback_source | fallback_ticker != i.fallback_ticker,
    .(yahoo_finance_ticker, fallback_source, fallback_ticker,
      runtime_fallback_source = i.fallback_source, runtime_fallback_ticker = i.fallback_ticker)
  ]
  list(valid = nrow(missing) == 0L && nrow(mismatched) == 0L, missing = missing, mismatched = mismatched)
}

#' Bootstrap Yahoo Finance Runtime Registry
#'
#' Create an absent runtime registry from the tracked package seed. Existing
#' registries are never overwritten; they are validated and an actionable error
#' is raised if required fallback declarations have drifted.
#'
#' @param registry_path Runtime registry JSON path.
#' @param seed_path Package seed registry JSON path.
#'
#' @return Invisibly returns the runtime registry path.
#' @export
bootstrap_yahoofinance_registry <- function(registry_path = get_yahoofinance_registry_file_path(),
                                             seed_path = get_yahoofinance_seed_registry_path()) {
  if (!file.exists(registry_path)) {
    dir.create(dirname(registry_path), recursive = TRUE, showWarnings = FALSE)
    if (!file.copy(seed_path, registry_path, overwrite = FALSE)) {
      stop("Could not create Yahoo Finance runtime registry: ", registry_path, call. = FALSE)
    }
    return(invisible(registry_path))
  }
  validation <- validate_yahoofinance_registry(registry_path, seed_path)
  if (!validation$valid) {
    stop(
      "Yahoo Finance runtime registry is missing or has changed required fallback declarations. ",
      "Reinitialize it from the package seed or update the declarations manually.",
      call. = FALSE
    )
  }
  invisible(registry_path)
}

#' Get Local quantmod OHLC Data
#'
#' @param label Local symbol label used in the stored data.
#' @param src quantmod source, default `"yahoo"`.
#' @param interval Interval label, default `"1d"`.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_quantmod_OHLC <- function(label, src = "yahoo", interval = "1d", local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- .quantmod_default_local_path(src = src, create = FALSE)
  }

  .read_local_data_table(file.path(local_path, .quantmod_local_filename(label, src = src, interval = interval)), sort_cols = "datetime")
}

#' Get Completed Local Daily quantmod OHLC Data
#'
#' Return only daily OHLC rows whose UTC date is strictly before an explicit
#' cutoff. A finite row dated on the current UTC date is retained in the raw
#' cache but is provisional until the next UTC date.
#'
#' @param label Local symbol label used in the stored data.
#' @param src quantmod source, default `"yahoo"`.
#' @param interval Interval label. Only `"1d"` is supported.
#' @param local_path Optional local storage path.
#' @param as_of UTC timestamp used to determine the current UTC date.
#'
#' @return `data.table` or `NULL`.
#' @export
get_completed_local_quantmod_OHLC <- function(label, src = "yahoo", interval = "1d", local_path = NULL,
                                              as_of = as.Date(Sys.time(), tz = "UTC")) {
  if (!identical(interval, "1d")) {
    stop("Completed-bar filtering is only defined for daily ('1d') OHLC data.", call. = FALSE)
  }
  dt <- get_local_quantmod_OHLC(label = label, src = src, interval = interval, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) return(dt)
  cutoff_date <- as.Date(as_of, tz = "UTC")
  dt[date < cutoff_date]
}

#' Synchronize Local quantmod OHLC Data
#'
#' @param ticker Market symbol passed to `quantmod::getSymbols()`.
#' @param label Optional label to store in the standardized `symbol` column.
#' @param from Start date.
#' @param to End date.
#' @param src quantmod source, default `"yahoo"`.
#' @param local_path Optional local storage path.
#' @inheritParams fetch_quantmod_OHLC
#'
#' @return A sync result list.
#'
#' @details An external fallback never replaces a finite existing primary-source
#' OHLC bar. It only fills keys whose local bars are missing or invalid.
#' @export
sync_local_quantmod_OHLC <- function(ticker, label = ticker, from, to, src = "yahoo", local_path = NULL,
                                     max_attempts = 3L, retry_delay_seconds = 1,
                                     fallback_source = NULL, fallback_ticker = ticker) {
  if (is.null(local_path)) {
    local_path <- .quantmod_default_local_path(src = src, create = TRUE)
  }

  local_file_path <- file.path(local_path, .quantmod_local_filename(label, src = src, interval = "1d"))
  existing_dt <- .as_data_table(.safe_read_rds(local_file_path, default = NULL))
  has_valid_local_history <- !is.null(existing_dt) && nrow(existing_dt) > 0L &&
    any(.quantmod_complete_ohlc_rows(existing_dt))
  new_dt <- fetch_quantmod_OHLC(
    ticker = ticker, label = label, from = from, to = to, src = src, raw_data = FALSE,
    max_attempts = max_attempts, retry_delay_seconds = retry_delay_seconds,
    fallback_source = fallback_source, fallback_ticker = fallback_ticker,
    require_start_coverage = has_valid_local_history
  )
  fetch_method <- attr(new_dt, "investdatar_fetch_method") %||% "quantmod"
  fetch_attempts <- attr(new_dt, "investdatar_fetch_attempts") %||% 1L
  primary_error <- attr(new_dt, "investdatar_primary_error") %||% NA_character_
  primary_error_class <- attr(new_dt, "investdatar_primary_error_class") %||% NA_character_
  new_dt <- .validate_quantmod_ohlc_window(
    new_dt, ticker = ticker, src = src, from = from, to = to,
    require_start_coverage = has_valid_local_history
  )
  invalid_ohlc_rows <- attr(new_dt, "investdatar_invalid_ohlc_rows") %||% 0L
  source_utime <- infer_source_utime_from_frequency("1d", reference_time = Sys.time(), tz = "UTC")

  # A declared fallback fills absent or invalid local bars but preserves valid primary-source rows.
  if (grepl("_fallback$", fetch_method)) {
    old_valid <- if (is.null(existing_dt)) existing_dt else existing_dt[.quantmod_complete_ohlc_rows(existing_dt)]
    if (!is.null(old_valid) && nrow(old_valid) > 0L) {
      new_dt <- new_dt[!old_valid[, .(symbol, interval, datetime)], on = c("symbol", "interval", "datetime")]
    }
  }

  result <- sync_local_data(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = c("symbol", "interval", "datetime"),
    order_cols = "datetime",
    source_utime = source_utime
  )
  result$fetch_method <- fetch_method
  result$fetch_attempts <- fetch_attempts
  result$primary_error <- primary_error
  result$primary_error_class <- primary_error_class
  result$invalid_ohlc_rows <- invalid_ohlc_rows
  result
}

#' Synchronize All Yahoo Finance Tickers In The Registry
#'
#' @param from Optional start date passed to `quantmod::getSymbols()`. When
#'   omitted, the function derives a per-ticker start date from the latest local
#'   record minus `overlap_days`, or falls back to `to - initial_lookback_days`
#'   for tickers without local data.
#' @param to End date passed to `quantmod::getSymbols()`.
#' @param registry Optional Yahoo Finance registry table.
#'   When omitted, the configured runtime registry is validated against the
#'   package seed before any provider request.
#' @param local_path Optional local storage path.
#' @param src quantmod source, default `"yahoo"`.
#' @param overlap_days Integer safety overlap used when deriving per-ticker
#'   incremental start dates from local data.
#' @param initial_lookback_days Integer fallback lookback for tickers without
#'   local data when `from` is omitted.
#' @inheritParams fetch_quantmod_OHLC
#'
#' @return Summary `data.table`.
#' @export
sync_all_yahoofinance_registry_data <- function(from = NULL,
                                                to = Sys.Date(),
                                                registry = get_yahoofinance_registry(),
                                                local_path = NULL,
                                                src = "yahoo",
                                                overlap_days = 10L,
                                                initial_lookback_days = 400L,
                                                max_attempts = 3L,
                                                retry_delay_seconds = 1,
                                                fallback_source = NULL,
                                                fallback_ticker = NULL) {
  if (missing(registry)) {
    registry_path <- get_yahoofinance_registry_file_path()
    if (!file.exists(registry_path)) {
      stop(
        "Yahoo Finance runtime registry is missing: ", registry_path,
        ". Run bootstrap_yahoofinance_registry() to create it from the package seed.",
        call. = FALSE
      )
    }
    validation <- validate_yahoofinance_registry(registry_path)
    if (!validation$valid) {
      stop(
        "Yahoo Finance runtime registry is missing or has changed required fallback declarations. ",
        "Restore the required entries from the package seed, or back up the existing file and ",
        "recreate it with bootstrap_yahoofinance_registry().",
        call. = FALSE
      )
    }
  }
  stopifnot("yahoo_finance_ticker" %in% names(registry))

  if (is.null(local_path)) {
    local_path <- .quantmod_default_local_path(src = src, create = TRUE)
  }
  run_started_at <- Sys.time()
  to <- as.Date(to)
  overlap_days <- as.integer(overlap_days)
  initial_lookback_days <- as.integer(initial_lookback_days)

  summary_list <- lapply(seq_len(nrow(registry)), function(i) {
    ticker <- registry$yahoo_finance_ticker[[i]]
    ticker_fallback_source <- fallback_source
    if (is.null(ticker_fallback_source) && "fallback_source" %in% names(registry)) {
      ticker_fallback_source <- registry$fallback_source[[i]]
    }
    if (length(ticker_fallback_source) == 0L || is.na(ticker_fallback_source) || !nzchar(ticker_fallback_source)) {
      ticker_fallback_source <- NULL
    }
    ticker_fallback_ticker <- fallback_ticker
    if (is.null(ticker_fallback_ticker) && "fallback_ticker" %in% names(registry)) {
      ticker_fallback_ticker <- registry$fallback_ticker[[i]]
    }
    if (is.null(ticker_fallback_ticker) || is.na(ticker_fallback_ticker) || !nzchar(ticker_fallback_ticker)) {
      ticker_fallback_ticker <- ticker
    }
    latest_local_date <- .quantmod_latest_local_date(ticker, src = src, interval = "1d", local_path = local_path)
    ticker_from <- if (!is.null(from)) {
      as.Date(from)
    } else if (!is.na(latest_local_date)) {
      latest_local_date - overlap_days
    } else {
      to - initial_lookback_days
    }

    tryCatch(
      {
        res <- sync_local_quantmod_OHLC(
          ticker = ticker,
          label = ticker,
          from = ticker_from,
          to = to,
          src = src,
          local_path = local_path,
          max_attempts = max_attempts,
          retry_delay_seconds = retry_delay_seconds,
          fallback_source = ticker_fallback_source,
          fallback_ticker = ticker_fallback_ticker
        )
        data.table::data.table(
          yahoo_finance_ticker = ticker,
          from = ticker_from,
          to = to,
          latest_local_date = latest_local_date,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          fetch_method = res$fetch_method %||% "quantmod",
          fetch_attempts = res$fetch_attempts %||% 1L,
          primary_error = res$primary_error %||% NA_character_,
          primary_error_class = res$primary_error_class %||% NA_character_,
          invalid_ohlc_rows = res$invalid_ohlc_rows %||% 0L,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          yahoo_finance_ticker = ticker,
          from = ticker_from,
          to = to,
          latest_local_date = latest_local_date,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          fetch_method = NA_character_,
          fetch_attempts = if (!is.null(e$attempts)) e$attempts else NA_integer_,
          primary_error = NA_character_,
          primary_error_class = NA_character_,
          invalid_ohlc_rows = NA_integer_,
          error = conditionMessage(e),
          error_class = class(e)[[1L]]
        )
      }
    )
  })

  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE),
    source_id = "yahoofinance",
    run_started_at = run_started_at,
    run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "yahoofinance",
    summary = summary_dt,
    local_path = local_path,
    params = list(
      from = if (is.null(from)) NULL else as.character(as.Date(from)),
      to = as.character(to),
      src = src,
      overlap_days = overlap_days,
      initial_lookback_days = initial_lookback_days,
      max_attempts = max_attempts,
      retry_delay_seconds = retry_delay_seconds,
      fallback_source = fallback_source,
      fallback_ticker = fallback_ticker
    ),
    run_started_at = run_started_at,
    run_finished_at = run_finished_at
  )
  summary_dt
}
