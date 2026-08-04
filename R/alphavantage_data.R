#' Fetch Daily Time Series from AlphaVantage
#'
#' Retrieves daily historical stock data (open, high, low, close, volume) for a given symbol
#' using the AlphaVantage API. You can specify whether to retrieve only recent data
#' (\code{"compact"}) or the full history (\code{"full"}).
#' The returned object follows the package's standardized market OHLCV schema.
#'
#' @param symbol A character string for the stock ticker symbol (e.g., \code{"AAPL"}, \code{"TSLA"}).
#' @param mode Character string; either \code{"compact"} (latest 100 days) or \code{"full"} (full history). Defaults to \code{"compact"}.
#' @param config A list of AlphaVantage API settings, typically retrieved via \code{tool_set_config("alphavantage")}.
#'
#' @return A `data.table` with standardized columns including `source`,
#'   `symbol`, `interval`, `datetime`, `date`, `open`, `high`, `low`, `close`,
#'   and `volume`.
#' \describe{
#'   \item{\code{datetime}}{Midnight timestamp derived from the trading date.}
#'   \item{\code{date}}{Date of the observation.}
#'   \item{\code{open}}{Opening price.}
#'   \item{\code{high}}{Highest price of the day.}
#'   \item{\code{low}}{Lowest price of the day.}
#'   \item{\code{close}}{Closing price.}
#'   \item{\code{volume}}{Volume of trades.}
#' }
#'
#' @examples
#' \dontrun{
#' config <- list(api_key = Sys.getenv("ALPHAVANTAGE_API_KEY"))
#' df <- get_source_data_alphavantage_ts_daily("MSFT", mode = "compact", config = config)
#' head(df)
#' }
#'
#' @export
get_source_data_alphavantage_ts_daily <- function(symbol, mode = c('compact', 'full'), config = NULL) {
  config <- .get_api_config("alphavantage", config = config)

  api_key <- config$api_key
  if (is.null(api_key) || !nzchar(api_key)) {
    stop("Alpha Vantage API key is missing. Set ALPHAVANTAGE_API_KEY or AlphaVantage.api_key.", call. = FALSE)
  }
  mode <- match.arg(mode)
  data_raw <- .http_get_json(config$url, query = list(
    `function` = "TIME_SERIES_DAILY", outputsize = mode, symbol = symbol, apikey = api_key
  ))
  api_error <- data_raw[["Error Message"]] %||% data_raw[["Note"]] %||% data_raw[["Information"]]
  if (!is.null(api_error)) stop("Alpha Vantage API error: ", api_error, call. = FALSE)
  
  ts_list <- data_raw[["Time Series (Daily)"]]
  if (is.null(ts_list) || length(ts_list) == 0L) stop("Alpha Vantage response contains no daily time series.", call. = FALSE)
  
  ts_df <- do.call(rbind, lapply(ts_list, function(x) as.data.frame(t(x), stringsAsFactors = FALSE)))
  
  # Add date column (rownames are dates)
  ts_df$date <- rownames(ts_df)
  rownames(ts_df) <- NULL
  
  # Reorder columns
  ts_df <- ts_df[, c("date", names(ts_df)[1:5])]
  
  # Rename columns
  colnames(ts_df) <- c("date", "open", "high", "low", "close", "volume")
  
  # Convert types
  ts_df$open <- as.numeric(ts_df$open)
  ts_df$high <- as.numeric(ts_df$high)
  ts_df$low <- as.numeric(ts_df$low)
  ts_df$close <- as.numeric(ts_df$close)
  ts_df$volume <- as.numeric(ts_df$volume)
  ts_df$date <- as.Date(ts_df$date)
  ts_df$symbol <- symbol
  
  ts_dt <- data.table::as.data.table(ts_df)
  .standardize_market_ohlcv(
    ts_dt,
    source = "alphavantage",
    symbol = symbol,
    interval = "1d",
    time_col = "date"
  )
}

.alphavantage_local_file <- function(symbol, local_path) {
  file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]", "_", toupper(symbol)), "__daily.rds"))
}

#' Read Local Alpha Vantage Daily Data
#' @param symbol Market symbol.
#' @param local_path Optional Alpha Vantage storage directory.
#' @return A standardized market `data.table`, or `NULL`.
#' @export
get_local_alphavantage_data <- function(symbol, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("alphavantage")
  .read_local_data_table(.alphavantage_local_file(symbol, local_path), sort_cols = "datetime")
}

#' Synchronize One Alpha Vantage Daily Series
#' @inheritParams get_source_data_alphavantage_ts_daily
#' @param local_path Optional Alpha Vantage storage directory.
#' @return A standard synchronization result.
#' @export
sync_local_alphavantage_data <- function(symbol, mode = NULL, config = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("alphavantage", create = TRUE)
  local_file <- .alphavantage_local_file(symbol, local_path)
  existing <- .safe_read_rds(local_file, default = NULL)
  if (is.null(mode)) mode <- if (is.null(existing) || nrow(existing) == 0L) "full" else "compact"
  new_data <- get_source_data_alphavantage_ts_daily(symbol, mode = mode, config = config)
  source_utime <- if (nrow(new_data)) max(new_data$datetime, na.rm = TRUE) else NULL
  sync_local_data(
    new_data, local_file, key_cols = c("symbol", "interval", "datetime"),
    order_cols = "datetime", source_utime = source_utime
  )
}

#' Get Alpha Vantage Registry File Path
#' @param config_dir Optional configuration directory used for fallback.
#' @return Character scalar path.
#' @export
get_alphavantage_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("alphavantage"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) {
    return(.normalize_scalar_path(cfg$registry_file, config_dir = getOption("investdatar.config_dir")))
  }
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) stop("No Alpha Vantage registry path is configured.", call. = FALSE)
  file.path(config_dir, "alphavantage_series_registry.json")
}

#' Get Alpha Vantage Series Registry
#' @param registry_path Optional registry JSON path.
#' @return A registry `data.table`.
#' @export
get_alphavantage_registry <- function(registry_path = get_alphavantage_registry_file_path()) {
  .read_json_registry(registry_path, empty_cols = c("symbol", "label", "active"))
}

#' Synchronize Registered Alpha Vantage Series
#' @param registry Optional Alpha Vantage registry.
#' @param config Optional API configuration.
#' @param local_path Optional storage directory.
#' @param ... Passed to `sync_local_alphavantage_data()`.
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_alphavantage_registry_data <- function(registry = get_alphavantage_registry(), config = NULL, local_path = NULL, ...) {
  stopifnot("symbol" %in% names(registry))
  if (is.null(local_path)) local_path <- get_source_data_path("alphavantage", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) registry <- registry[tolower(as.character(registry$active)) %in% c("true", "1", "yes", "y")]
  rows <- lapply(registry$symbol, function(symbol) tryCatch({
    res <- sync_local_alphavantage_data(symbol, config = config, local_path = local_path, ...)
    data.table::data.table(symbol = symbol, status = "success", updated = isTRUE(res$updated),
                           n_rows = res$n_rows %||% NA_integer_, n_new_rows = res$n_new_rows %||% NA_integer_, error = NA_character_)
  }, error = function(e) data.table::data.table(
    symbol = symbol, status = "error", updated = FALSE, n_rows = NA_integer_, n_new_rows = NA_integer_,
    error = conditionMessage(e), error_class = class(e)[[1L]],
    http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
  )))
  run_finished_at <- Sys.time()
  summary <- .normalize_sync_summary(data.table::rbindlist(rows, use.names = TRUE, fill = TRUE),
                                     "alphavantage", run_started_at, run_finished_at)
  .write_sync_run_log("alphavantage", summary, local_path, list(), run_started_at, run_finished_at)
  summary
}
