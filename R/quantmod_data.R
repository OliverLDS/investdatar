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
