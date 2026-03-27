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
  base_url <- config$url
  data_type <- "TIME_SERIES_DAILY"
  mode <- match.arg(mode)
  
  url <- sprintf("%s?function=%s&outputsize=%s&symbol=%s&apikey=%s", base_url, data_type, mode, symbol, api_key)
  
  response <- httr::GET(url)
  
  data_raw <- jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"))
  
  ts_list <- data_raw[["Time Series (Daily)"]]
  
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

