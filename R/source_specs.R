.new_source_spec <- function(source_id, config_key, local_path_source,
                             resource_type, schema, capabilities, functions) {
  structure(
    list(
      source_id = source_id,
      config_key = config_key,
      local_path_source = local_path_source,
      resource_type = resource_type,
      schema = schema,
      capabilities = capabilities,
      functions = functions
    ),
    class = "investdatar_source_spec"
  )
}

#' Get Source Specification
#'
#' Returns a formal source-spec object describing one provider module. The spec
#' is intentionally lightweight so modules can later move into separate packages
#' without changing the high-level contract.
#'
#' @param source Character provider key.
#'
#' @return An object of class `investdatar_source_spec`.
#' @export
get_source_spec <- function(source) {
  key <- .resolve_source_key(source)

  specs <- list(
    fred = .new_source_spec(
      source_id = "fred",
      config_key = "FRED",
      local_path_source = "FRED",
      resource_type = "single_series",
      schema = list(time_col = "date", key_cols = "date", value_cols = "value"),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = FALSE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_fred", fetch_utime = "get_source_utime_fred", sync = "sync_local_fred_data", read_local = "get_local_FRED_data")
    ),
    wbstats = .new_source_spec(
      source_id = "wbstats",
      config_key = "WorldBank",
      local_path_source = "WorldBank",
      resource_type = "single_series",
      schema = list(time_col = "date", key_cols = c("indicator_id", "country", "date"), value_cols = "value"),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = FALSE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_wbstats", fetch_utime = "get_source_utime_wbstats", sync = "sync_local_wbstats_data", read_local = "get_local_wbstats_data")
    ),
    treasury = .new_source_spec(
      source_id = "treasury",
      config_key = "Treasury",
      local_path_source = "Treasury",
      resource_type = "rate_panel",
      schema = list(time_col = "date", key_cols = c("dataset", "date", "series_id"), value_cols = "value"),
      capabilities = list(source_utime = TRUE, inferred_utime = FALSE, pagination = FALSE, gap_detection = TRUE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_treasury_rates",
        fetch_utime = "get_source_utime_treasury_rates",
        sync = "sync_local_treasury_rates",
        sync_all = "sync_all_treasury_rates",
        read_local = "get_local_treasury_rates"
      )
    ),
    rss = .new_source_spec(
      source_id = "rss",
      config_key = "RSS",
      local_path_source = "RSS",
      resource_type = "narrative_feed",
      schema = list(time_col = "published_at", key_cols = c("feed_id", "guid"), value_cols = c("title", "summary", "link")),
      capabilities = list(source_utime = TRUE, inferred_utime = FALSE, pagination = FALSE, gap_detection = FALSE, sync = TRUE),
      functions = list(fetch = "get_source_data_rss", fetch_utime = "get_source_utime_rss", sync = "sync_local_rss_data", sync_registry = "sync_all_rss_registry_data", read_local = "get_local_rss_data")
    ),
    ishare = .new_source_spec(
      source_id = "ishare",
      config_key = "iShare",
      local_path_source = "iShare",
      resource_type = "fund_history",
      schema = list(time_col = "date", key_cols = "date", value_cols = c("nav", "ex_div", "N_shares")),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = FALSE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_ishare",
        fetch_holdings = "get_source_data_ishare_holdings",
        fetch_utime = "get_source_utime_ishare",
        sync = "sync_local_ishare_data",
        sync_holdings = "sync_local_ishare_holdings",
        read_local = "get_local_ishare_data",
        read_local_holdings = "get_local_ishare_holdings"
      )
    ),
    alphavantage = .new_source_spec(
      source_id = "alphavantage",
      config_key = "AlphaVantage",
      local_path_source = NULL,
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = FALSE, gap_detection = TRUE, sync = FALSE),
      functions = list(fetch = "get_source_data_alphavantage_ts_daily")
    ),
    quantmod = .new_source_spec(
      source_id = "quantmod",
      config_key = "YahooFinance",
      local_path_source = "YahooFinance",
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = FALSE, gap_detection = TRUE, sync = TRUE),
      functions = list(
        fetch = "fetch_quantmod_OHLC",
        sync = "sync_local_quantmod_OHLC",
        sync_registry = "sync_all_yahoofinance_registry_data",
        read_local = "get_local_quantmod_OHLC"
      )
    ),
    okx = .new_source_spec(
      source_id = "okx",
      config_key = "Crypto",
      local_path_source = "Crypto/okx",
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = TRUE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_okx_candle", fetch_history = "get_source_hist_data_okx_candle", sync = "sync_local_okx_candle", read_local = "get_local_okx_candle")
    ),
    binance = .new_source_spec(
      source_id = "binance",
      config_key = "Crypto",
      local_path_source = "Crypto/binance",
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = TRUE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_binance_klines", sync = "sync_local_binance_klines", read_local = "get_local_binance_klines")
    )
  )

  if (!key %in% names(specs)) {
    stop("Unknown source spec: ", source)
  }

  specs[[key]]
}

#' List Source Specifications
#'
#' @return Named list of `investdatar_source_spec` objects.
#' @export
list_source_specs <- function() {
  stats::setNames(
    lapply(c("fred", "wbstats", "treasury", "rss", "ishare", "alphavantage", "quantmod", "okx", "binance"), get_source_spec),
    c("fred", "wbstats", "treasury", "rss", "ishare", "alphavantage", "quantmod", "okx", "binance")
  )
}
