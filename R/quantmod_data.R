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
#'
#' @return `data.table` or raw xts object when `raw_data = TRUE`.
#' @export
fetch_quantmod_OHLC <- function(ticker, label = ticker, from, to, src = "yahoo", raw_data = FALSE) {
  .require_suggested_package("quantmod", "to fetch OHLC data.")
  .require_suggested_package("zoo", "to fetch OHLC data.")
  x <- tryCatch(
    quantmod::getSymbols(ticker, src = src, auto.assign = FALSE, from = from, to = to),
    error = function(e) {
      stop(
        sprintf("quantmod failed to fetch '%s' from source '%s': %s", ticker, src, conditionMessage(e)),
        call. = FALSE
      )
    }
  )
  if (raw_data) return(x)
  
  cn <- colnames(x)
  open_col <- grep("\\.Open$", cn, value = TRUE)
  high_col <- grep("\\.High$", cn, value = TRUE)
  low_col <- grep("\\.Low$", cn, value = TRUE)
  close_col <- grep("\\.Close$", cn, value = TRUE)
  adj_col <- grep("\\.Adjusted$", cn, value = TRUE)
  volume_col <- grep("\\.Volume$", cn, value = TRUE)
  
  x_dt <- data.table::data.table(
    date = as.Date(zoo::index(x)),
    open = NA_real_,
    high = NA_real_,
    low = NA_real_,
    close = NA_real_,
    volume = NA_real_,
    adj_close = NA_real_,
    symbol = label
  )
  
  if (length(open_col) == 1) x_dt[, open := as.numeric(x[, open_col])]
  if (length(high_col) == 1) x_dt[, high := as.numeric(x[, high_col])]
  if (length(low_col) == 1) x_dt[, low := as.numeric(x[, low_col])]
  if (length(close_col) == 1) x_dt[, close := as.numeric(x[, close_col])]
  if (length(volume_col) == 1) x_dt[, volume := as.numeric(x[, volume_col])]
  if (length(adj_col) == 1) x_dt[, adj_close := as.numeric(x[, adj_col])]
  
  .standardize_market_ohlcv(
    x_dt,
    source = paste0("quantmod_", src),
    symbol = label,
    interval = "1d",
    time_col = "date"
  )
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
  max(dt$date, na.rm = TRUE)
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

#' Synchronize Local quantmod OHLC Data
#'
#' @param ticker Market symbol passed to `quantmod::getSymbols()`.
#' @param label Optional label to store in the standardized `symbol` column.
#' @param from Start date.
#' @param to End date.
#' @param src quantmod source, default `"yahoo"`.
#' @param local_path Optional local storage path.
#'
#' @return A sync result list.
#' @export
sync_local_quantmod_OHLC <- function(ticker, label = ticker, from, to, src = "yahoo", local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- .quantmod_default_local_path(src = src, create = TRUE)
  }

  local_file_path <- file.path(local_path, .quantmod_local_filename(label, src = src, interval = "1d"))
  new_dt <- fetch_quantmod_OHLC(ticker = ticker, label = label, from = from, to = to, src = src, raw_data = FALSE)
  source_utime <- infer_source_utime_from_frequency("1d", reference_time = Sys.time(), tz = "UTC")

  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = c("symbol", "interval", "datetime"),
    order_cols = "datetime",
    source_utime = source_utime
  )
}

#' Synchronize All Yahoo Finance Tickers In The Registry
#'
#' @param from Optional start date passed to `quantmod::getSymbols()`. When
#'   omitted, the function derives a per-ticker start date from the latest local
#'   record minus `overlap_days`, or falls back to `to - initial_lookback_days`
#'   for tickers without local data.
#' @param to End date passed to `quantmod::getSymbols()`.
#' @param registry Optional Yahoo Finance registry table.
#' @param local_path Optional local storage path.
#' @param src quantmod source, default `"yahoo"`.
#' @param overlap_days Integer safety overlap used when deriving per-ticker
#'   incremental start dates from local data.
#' @param initial_lookback_days Integer fallback lookback for tickers without
#'   local data when `from` is omitted.
#'
#' @return Summary `data.table`.
#' @export
sync_all_yahoofinance_registry_data <- function(from = NULL,
                                                to = Sys.Date(),
                                                registry = get_yahoofinance_registry(),
                                                local_path = NULL,
                                                src = "yahoo",
                                                overlap_days = 10L,
                                                initial_lookback_days = 400L) {
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
          local_path = local_path
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
          error = conditionMessage(e)
        )
      }
    )
  })

  summary_dt <- data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE)
  .write_sync_run_log(
    source_id = "yahoofinance",
    summary = summary_dt,
    local_path = local_path,
    params = list(
      from = if (is.null(from)) NULL else as.character(as.Date(from)),
      to = as.character(to),
      src = src,
      overlap_days = overlap_days,
      initial_lookback_days = initial_lookback_days
    ),
    run_started_at = run_started_at,
    run_finished_at = Sys.time()
  )
  summary_dt
}
