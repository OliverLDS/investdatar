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
      functions = list(fetch = "get_source_data_fred", fetch_utime = "get_source_utime_fred", sync = "sync_local_fred_data", sync_registry = "sync_all_fred_registry_data", read_local = "get_local_FRED_data", describe = "describe_fred_data", detect_gaps = "detect_time_gaps_fred")
    ),
    wbstats = .new_source_spec(
      source_id = "wbstats",
      config_key = "WorldBank",
      local_path_source = "WorldBank",
      resource_type = "single_series",
      schema = list(time_col = "date", key_cols = c("indicator_id", "country", "date"), value_cols = "value"),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = FALSE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_wbstats", fetch_utime = "get_source_utime_wbstats", sync = "sync_local_wbstats_data", sync_registry = "sync_all_wbstats_registry_data", read_local = "get_local_wbstats_data", describe = "describe_wbstats_data", detect_gaps = "detect_time_gaps_wbstats")
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
        read_local = "get_local_treasury_rates",
        describe = "describe_treasury_rates",
        detect_gaps = "detect_time_gaps"
      )
    ),
    cftc = .new_source_spec(
      source_id = "cftc",
      config_key = "CFTC",
      local_path_source = "CFTC",
      resource_type = "position_panel",
      schema = list(
        time_col = "report_date",
        key_cols = c("report_id", "id"),
        value_cols = c(
          "open_interest_all", "dealer_positions_long_all",
          "dealer_positions_short_all", "asset_mgr_positions_long",
          "asset_mgr_positions_short", "lev_money_positions_long",
          "lev_money_positions_short"
        )
      ),
      capabilities = list(source_utime = TRUE, inferred_utime = FALSE, pagination = TRUE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_cftc_cot",
        fetch_utime = "get_source_utime_cftc_cot",
        sync = "sync_local_cftc_cot",
        sync_registry = "sync_all_cftc_cot_registry_data",
        read_local = "get_local_cftc_cot",
        describe = "describe_cftc_cot_data"
      )
    ),
    fiscaldata = .new_source_spec(
      source_id = "fiscaldata",
      config_key = "FiscalData",
      local_path_source = "FiscalData",
      resource_type = "dated_table",
      schema = list(time_col = "record_date", key_cols = c("dataset_id", "record_date"), value_cols = "provider_specific"),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = TRUE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_fiscaldata",
        fetch_utime = "get_source_utime_fiscaldata",
        sync = "sync_local_fiscaldata",
        sync_registry = "sync_all_fiscaldata_registry_data",
        read_local = "get_local_fiscaldata",
        describe = "describe_fiscaldata"
      )
    ),
    eia = .new_source_spec(
      source_id = "eia",
      config_key = "EIA",
      local_path_source = "EIA",
      resource_type = "single_series",
      schema = list(time_col = "date", key_cols = c("series_id", "period"), value_cols = "value"),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = TRUE, gap_detection = TRUE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_eia",
        fetch_utime = "get_source_utime_eia",
        sync = "sync_local_eia_data",
        sync_registry = "sync_all_eia_registry_data",
        read_local = "get_local_eia_data",
        describe = "describe_eia_data",
        detect_gaps = "detect_time_gaps"
      )
    ),
    sec_submissions = .new_source_spec(
      source_id = "sec_submissions",
      config_key = "SEC",
      local_path_source = "SEC/submissions",
      resource_type = "filing_event",
      schema = list(time_col = "filing_date", key_cols = c("cik", "accession_number"), value_cols = c("form", "primary_document")),
      capabilities = list(source_utime = TRUE, inferred_utime = FALSE, pagination = TRUE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_sec_submissions",
        fetch_utime = "get_source_utime_sec_submissions",
        sync = "sync_local_sec_submissions",
        sync_registry = "sync_all_sec_submissions_registry_data",
        read_local = "get_local_sec_submissions",
        describe = "describe_sec_submissions"
      )
    ),
    sec_companyfacts = .new_source_spec(
      source_id = "sec_companyfacts",
      config_key = "SEC",
      local_path_source = "SEC/companyfacts",
      resource_type = "fundamental_fact",
      schema = list(time_col = "filed", key_cols = c("cik", "fact_key"), value_cols = "value"),
      capabilities = list(source_utime = TRUE, inferred_utime = FALSE, pagination = FALSE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_sec_companyfacts",
        fetch_utime = "get_source_utime_sec_companyfacts",
        sync = "sync_local_sec_companyfacts",
        sync_registry = "sync_all_sec_companyfacts_registry_data",
        read_local = "get_local_sec_companyfacts",
        describe = "describe_sec_companyfacts"
      )
    ),
    sdmx = .new_source_spec(
      source_id = "sdmx",
      config_key = "SDMX",
      local_path_source = "SDMX",
      resource_type = "multidimensional_series",
      schema = list(time_col = "date", key_cols = c("series_id", "dimension_key", "period"), value_cols = "value"),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = FALSE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_sdmx", fetch_utime = "get_source_utime_sdmx",
        sync = "sync_local_sdmx_data", sync_registry = "sync_all_sdmx_registry_data",
        read_local = "get_local_sdmx_data", describe = "describe_sdmx_data"
      )
    ),
    rss = .new_source_spec(
      source_id = "rss",
      config_key = "RSS",
      local_path_source = "RSS",
      resource_type = "narrative_feed",
      schema = list(time_col = "published_at", key_cols = c("feed_id", "guid"), value_cols = c("title", "summary", "link")),
      capabilities = list(source_utime = TRUE, inferred_utime = FALSE, pagination = FALSE, gap_detection = FALSE, sync = TRUE),
      functions = list(fetch = "get_source_data_rss", fetch_utime = "get_source_utime_rss", sync = "sync_local_rss_data", sync_registry = "sync_all_rss_registry_data", read_local = "get_local_rss_data", describe = "describe_rss_data")
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
        sync_registry = "sync_all_ishare_registry_data",
        sync_holdings = "sync_local_ishare_holdings",
        sync_holdings_registry = "sync_all_ishare_registry_holdings",
        read_local = "get_local_ishare_data",
        read_local_holdings = "get_local_ishare_holdings",
        describe = "describe_ishare_data"
      )
    ),
    alphavantage = .new_source_spec(
      source_id = "alphavantage",
      config_key = "AlphaVantage",
      local_path_source = NULL,
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = FALSE, gap_detection = TRUE, sync = FALSE),
      functions = list(fetch = "get_source_data_alphavantage_ts_daily", describe = "describe_alphavantage_data", detect_gaps = "detect_time_gaps")
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
        read_local = "get_local_quantmod_OHLC",
        describe = "describe_quantmod_data",
        detect_gaps = "detect_time_gaps"
      )
    ),
    okx = .new_source_spec(
      source_id = "okx",
      config_key = "Crypto",
      local_path_source = "Crypto/okx",
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = TRUE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_okx_candle", fetch_history = "get_source_hist_data_okx_candle", fetch_utime = "get_source_utime_okx_candle", sync = "sync_local_okx_candle", repair = "repair_local_okx_candle_gaps", read_local = "get_local_okx_candle", describe = "describe_okx_candle_data", detect_gaps = "detect_time_gaps_okx_candle")
    ),
    binance = .new_source_spec(
      source_id = "binance",
      config_key = "Crypto",
      local_path_source = "Crypto/binance",
      resource_type = "market_ohlcv",
      schema = list(time_col = "datetime", key_cols = c("symbol", "interval", "datetime"), value_cols = c("open", "high", "low", "close", "volume")),
      capabilities = list(source_utime = FALSE, inferred_utime = TRUE, pagination = TRUE, gap_detection = TRUE, sync = TRUE),
      functions = list(fetch = "get_source_data_binance_klines", sync = "sync_local_binance_klines", repair = "repair_local_binance_klines_gaps", read_local = "get_local_binance_klines", describe = "describe_binance_data", detect_gaps = "detect_time_gaps")
    ),
    crypto_derivatives = .new_source_spec(
      source_id = "crypto_derivatives",
      config_key = "Crypto",
      local_path_source = "Crypto/derivatives",
      resource_type = "derivatives_series",
      schema = list(time_col = "datetime", key_cols = c("provider", "dataset_type", "symbol", "interval", "datetime"), value_cols = "value"),
      capabilities = list(source_utime = TRUE, inferred_utime = TRUE, pagination = TRUE, gap_detection = FALSE, sync = TRUE),
      functions = list(
        fetch = "get_source_data_crypto_derivatives",
        fetch_utime = "get_source_utime_crypto_derivatives",
        sync = "sync_local_crypto_derivatives",
        sync_registry = "sync_all_crypto_derivatives_registry_data",
        read_local = "get_local_crypto_derivatives",
        describe = "describe_crypto_derivatives"
      )
    )
  )

  if (!key %in% names(specs)) {
    stop("Unknown source spec: ", source)
  }

  .validate_source_spec(specs[[key]])
}

.validate_source_spec <- function(spec) {
  required_fields <- c(
    "source_id", "config_key", "local_path_source", "resource_type",
    "schema", "capabilities", "functions"
  )
  missing_fields <- setdiff(required_fields, names(spec))
  if (length(missing_fields) > 0L) {
    stop("Source spec is missing field(s): ", paste(missing_fields, collapse = ", "), call. = FALSE)
  }

  required_schema <- c("time_col", "key_cols", "value_cols")
  missing_schema <- setdiff(required_schema, names(spec$schema))
  if (length(missing_schema) > 0L) {
    stop("Source spec schema is missing field(s): ", paste(missing_schema, collapse = ", "), call. = FALSE)
  }
  if (!is.character(spec$schema$key_cols) || length(spec$schema$key_cols) == 0L) {
    stop("Source spec schema$key_cols must be a non-empty character vector.", call. = FALSE)
  }
  if (isTRUE(spec$capabilities$sync) && is.null(spec$functions$sync)) {
    stop("A sync-capable source spec must declare functions$sync.", call. = FALSE)
  }
  if (isTRUE(spec$capabilities$source_utime) && is.null(spec$functions$fetch_utime)) {
    stop("A source-update-capable spec must declare functions$fetch_utime.", call. = FALSE)
  }
  if (isTRUE(spec$capabilities$gap_detection) && is.null(spec$functions$detect_gaps)) {
    stop("A gap-detection-capable spec must declare functions$detect_gaps.", call. = FALSE)
  }
  if (is.null(spec$functions$describe)) {
    stop("A source spec must declare functions$describe.", call. = FALSE)
  }
  if (!isTRUE(spec$capabilities$sync) && !is.null(spec$local_path_source)) {
    stop("A fetch-only source spec must not declare local_path_source.", call. = FALSE)
  }

  function_names <- unlist(spec$functions, use.names = FALSE)
  if (!is.character(function_names) || any(!nzchar(function_names))) {
    stop("Source spec function declarations must be non-empty names.", call. = FALSE)
  }

  spec
}

#' List Source Specifications
#'
#' @return Named list of `investdatar_source_spec` objects.
#' @export
list_source_specs <- function() {
  stats::setNames(
    lapply(c("fred", "wbstats", "treasury", "cftc", "fiscaldata", "eia", "sec_submissions", "sec_companyfacts", "sdmx", "rss", "ishare", "alphavantage", "quantmod", "okx", "binance", "crypto_derivatives"), get_source_spec),
    c("fred", "wbstats", "treasury", "cftc", "fiscaldata", "eia", "sec_submissions", "sec_companyfacts", "sdmx", "rss", "ishare", "alphavantage", "quantmod", "okx", "binance", "crypto_derivatives")
  )
}
