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
    error = function(e) NULL
  )
  if (raw_data) return(x)
  if (is.null(x)) return(NULL)
  
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
