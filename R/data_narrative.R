.describe_value_summary <- function(x) {
  x <- as.numeric(x)
  x_non_na <- x[!is.na(x)]
  if (length(x_non_na) == 0L) {
    return("All values are missing.")
  }

  sprintf(
    "Non-missing values range from %s to %s.",
    format(min(x_non_na), scientific = FALSE, trim = TRUE),
    format(max(x_non_na), scientific = FALSE, trim = TRUE)
  )
}

.describe_time_coverage <- function(x) {
  if (length(x) == 0L || all(is.na(x))) {
    return("No time coverage is available.")
  }

  sprintf(
    "Observed time coverage runs from %s to %s.",
    as.character(min(x, na.rm = TRUE)),
    as.character(max(x, na.rm = TRUE))
  )
}

.describe_market_ohlcv_core <- function(dt, source_label = NULL) {
  dt <- .as_data_table(dt)
  if (is.null(dt) || nrow(dt) == 0L) {
    stop("data must contain at least one row.")
  }

  if (is.null(source_label) || !nzchar(source_label)) {
    source_label <- unique(stats::na.omit(dt$source))
    source_label <- if (length(source_label) >= 1L) source_label[[1]] else "unknown_source"
  }

  symbol <- unique(stats::na.omit(dt$symbol))
  interval <- unique(stats::na.omit(dt$interval))
  symbol_txt <- if (length(symbol) >= 1L) symbol[[1]] else "unknown_symbol"
  interval_txt <- if (length(interval) >= 1L) interval[[1]] else "unknown_interval"
  n_na_close <- if ("close" %in% names(dt)) sum(is.na(dt$close)) else NA_integer_

  paste(
    sprintf("This object is a data.table of standardized market OHLCV data from %s.", source_label),
    sprintf("The primary instrument label is %s and the interval label is %s.", symbol_txt, interval_txt),
    "Core columns are source, symbol, interval, datetime, date, open, high, low, close, and volume.",
    sprintf("The table currently contains %s rows.", nrow(dt)),
    .describe_time_coverage(dt$datetime),
    if ("close" %in% names(dt)) .describe_value_summary(dt$close) else "Close values are not available.",
    if (!is.na(n_na_close)) sprintf("The close column has %s missing values.", n_na_close) else "",
    "This summary is intended to help another analyst agent understand the dataset before writing R analysis or visualization code."
  )
}

#' Describe Market OHLCV Data
#'
#' Creates a compact narrative for standardized market OHLCV data.
#'
#' @param data A standardized OHLCV `data.table`.
#' @param source_label Optional source label override.
#'
#' @return Character scalar narrative.
#' @export
describe_market_ohlcv_data <- function(data, source_label = NULL) {
  .describe_market_ohlcv_core(data, source_label = source_label)
}

#' Describe FRED Data
#'
#' Creates a compact narrative describing the structure and meaning of a locally
#' stored FRED series for downstream analyst agents.
#'
#' @param series_id FRED series identifier.
#' @param local_path Optional local storage path used to read the local series.
#' @param registry_path Optional registry path used to resolve series metadata.
#'
#' @return Character scalar narrative.
#' @export
describe_fred_data <- function(series_id, local_path = NULL, registry_path = get_fred_registry_file_path()) {
  local_dt <- get_local_FRED_data(series_id, local_path = local_path)
  if (is.null(local_dt)) {
    stop("Local FRED data not found for series_id: ", series_id)
  }

  registry <- get_fred_registry(registry_path = registry_path)
  series_id_value <- series_id
  row <- registry[series_id == series_id_value]
  if (nrow(row) == 0L) {
    stop("Series metadata not found in FRED registry for series_id: ", series_id)
  }

  paste(
    sprintf("This object is a data.table for FRED series %s.", row$series_id[[1]]),
    "The primary fields are date, stored as Date and potentially containing calendar gaps, and value, stored as numeric.",
    sprintf("Series title: %s.", row$title[[1]]),
    sprintf("Units: %s.", row$units[[1]]),
    sprintf("Registry frequency: %s.", row$freq[[1]]),
    sprintf("Registry coverage: %s to %s.", row$start[[1]], row$end[[1]]),
    sprintf("Seasonal adjustment: %s.", row$season[[1]]),
    sprintf("The local data currently contains %s rows and %s missing values in the value column.", nrow(local_dt), sum(is.na(local_dt$value))),
    .describe_time_coverage(local_dt$date),
    .describe_value_summary(local_dt$value),
    "This summary is intended to help another analyst agent understand the dataset before writing R analysis or visualization code."
  )
}

#' Describe World Bank Data
#'
#' Creates a compact narrative for locally stored World Bank indicator data.
#'
#' @param indicator World Bank indicator code.
#' @param country Country or aggregate code.
#' @param freq Frequency code.
#' @param local_path Local storage path.
#'
#' @return Character scalar narrative.
#' @export
describe_wbstats_data <- function(indicator, country, freq = "Y", local_path) {
  dt <- get_local_wbstats_data(indicator = indicator, country = country, freq = freq, local_path = local_path)
  if (is.null(dt)) {
    stop("Local World Bank data not found for indicator/country/freq combination.")
  }

  paste(
    sprintf("This object is a data.table for World Bank indicator %s for %s.", indicator, country),
    "The primary columns are source, indicator_id, country, frequency, date, and value, with optional ISO country codes and extra metadata columns.",
    sprintf("The declared frequency code is %s.", toupper(freq)),
    sprintf("The table currently contains %s rows and %s missing values in the value column.", nrow(dt), sum(is.na(dt$value))),
    .describe_time_coverage(dt$date),
    .describe_value_summary(dt$value),
    "This summary is intended to help another analyst agent understand the dataset before writing R analysis or visualization code."
  )
}

#' Describe iShares Data
#'
#' Creates a compact narrative for locally stored iShares historical fund data.
#'
#' @param ticker ETF ticker.
#' @param local_path Optional local storage path.
#'
#' @return Character scalar narrative.
#' @export
describe_ishare_data <- function(ticker, local_path = NULL) {
  dt <- get_local_ishare_data(ticker = ticker, local_path = local_path)
  if (is.null(dt)) {
    stop("Local iShares data not found for ticker: ", ticker)
  }

  paste(
    sprintf("This object is a data.table for iShares fund history for ticker %s.", ticker),
    "The key columns are date, nav, ex_div, N_shares, and derived columns such as log_ret and log_N_shares when available.",
    sprintf("The table currently contains %s rows.", nrow(dt)),
    .describe_time_coverage(dt$date),
    if ("nav" %in% names(dt)) .describe_value_summary(dt$nav) else "NAV values are not available.",
    sprintf("The nav column has %s missing values.", if ("nav" %in% names(dt)) sum(is.na(dt$nav)) else NA_integer_),
    "This summary is intended to help another analyst agent understand the dataset before writing R analysis or visualization code."
  )
}

#' Describe OKX Candle Data
#'
#' Creates a compact narrative for locally stored OKX candle data.
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param local_path Optional local storage path.
#'
#' @return Character scalar narrative.
#' @export
describe_okx_candle_data <- function(inst_id, bar, local_path = NULL) {
  dt <- get_local_okx_candle(inst_id = inst_id, bar = bar, local_path = local_path)
  if (is.null(dt)) {
    stop("Local OKX candle data not found for inst_id/bar combination.")
  }

  .describe_market_ohlcv_core(dt, source_label = "okx")
}

#' Describe Binance Data
#'
#' Fetches Binance OHLCV data and creates a compact narrative for the resulting
#' standardized market table.
#'
#' @inheritParams get_source_data_binance_klines
#'
#' @return Character scalar narrative.
#' @export
describe_binance_data <- function(symbol = "ETHUSDT", interval = "1m",
                                  start_time = NULL, end_time = NULL,
                                  limit = 1500L, tz = "UTC",
                                  paginate = TRUE) {
  dt <- get_source_data_binance_klines(
    symbol = symbol,
    interval = interval,
    start_time = start_time,
    end_time = end_time,
    limit = limit,
    tz = tz,
    paginate = paginate
  )
  .describe_market_ohlcv_core(dt, source_label = "binance")
}

#' Describe AlphaVantage Data
#'
#' Fetches AlphaVantage daily OHLCV data and creates a compact narrative for
#' the resulting standardized market table.
#'
#' @param symbol Market symbol.
#' @param mode Output size passed to AlphaVantage.
#' @param config Optional AlphaVantage API configuration.
#'
#' @return Character scalar narrative.
#' @export
describe_alphavantage_data <- function(symbol, mode = c("compact", "full"), config = NULL) {
  dt <- get_source_data_alphavantage_ts_daily(symbol = symbol, mode = mode, config = config)
  .describe_market_ohlcv_core(dt, source_label = "alphavantage")
}

#' Describe quantmod Data
#'
#' Fetches market OHLCV data through `quantmod` and creates a compact narrative
#' for the resulting standardized market table.
#'
#' @param ticker Market symbol passed to `quantmod::getSymbols()`.
#' @param label Optional label to store in the standardized `symbol` column.
#' @param from Start date.
#' @param to End date.
#' @param src quantmod source, default `"yahoo"`.
#'
#' @return Character scalar narrative.
#' @export
describe_quantmod_data <- function(ticker, label = ticker, from, to, src = "yahoo") {
  dt <- fetch_quantmod_OHLC(ticker = ticker, label = label, from = from, to = to, src = src, raw_data = FALSE)
  .describe_market_ohlcv_core(dt, source_label = paste0("quantmod_", src))
}
