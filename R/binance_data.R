#' Fetch Kline (Candlestick) Data from Binance Futures API
#'
#' Retrieves historical candlestick data from Binance USDT-margined Futures.
#' Supports custom time intervals and optional start/end time filtering.
#'
#' @param symbol Trading pair symbol (e.g., \code{"ETHUSDT"}).
#' @param interval Candlestick interval (e.g., \code{"1m"}, \code{"5m"}, \code{"1h"}, \code{"1d"}).
#' @param start_time Optional. Start time in \code{"YYYY-MM-DD HH:MM:SS"} format. Defaults to \code{NULL}.
#' @param end_time Optional. End time in \code{"YYYY-MM-DD HH:MM:SS"} format. Defaults to \code{NULL}.
#' @param limit Optional. Number of candles to return (default is 500 if unspecified).
#' @param tz Timezone to apply to returned timestamps (default is \code{"Asia/Hong_Kong"}).
#'
#' @return A data frame with columns:
#' \describe{
#'   \item{\code{open_time}}{Time when the candle opened (POSIXct).}
#'   \item{\code{open}}{Opening price.}
#'   \item{\code{high}}{Highest price.}
#'   \item{\code{low}}{Lowest price.}
#'   \item{\code{close}}{Closing price.}
#'   \item{\code{volume}}{Base asset volume.}
#'   \item{\code{close_time}}{Time when the candle closed (POSIXct).}
#'   \item{\code{quote_asset_volume}}{Quote asset volume.}
#'   \item{\code{num_trades}}{Number of trades.}
#'   \item{\code{taker_buy_base_vol}}{Taker buy base asset volume.}
#'   \item{\code{taker_buy_quote_vol}}{Taker buy quote asset volume.}
#'   \item{\code{ignore}}{Unused placeholder field.}
#' }
#'
#' @details See the official Binance API documentation: \url{https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data}
#'
#' @examples
#' \dontrun{
#' df <- fetch_binance_klines(
#'   symbol = "BTCUSDT",
#'   interval = "5m",
#'   start_time = "2023-01-01 00:00:00",
#'   end_time = "2023-01-01 12:00:00"
#' )
#' head(df)
#' }
#'
#' @export
get_source_data_binance_klines <- function(
  symbol = "ETHUSDT",
  interval = "1m",
  start_time = NULL,
  end_time = NULL,
  limit = NULL,
  tz = "Asia/Hong_Kong"
) {
  start_posix <- as.POSIXct(start_time, tz = tz)
  end_posix   <- as.POSIXct(end_time, tz = tz)
  start_ms <- as.numeric(start_posix) * 1000
  end_ms   <- as.numeric(end_posix) * 1000

  res <- httr::GET(
    "https://fapi.binance.com/fapi/v1/klines",
    query = list(
      symbol = symbol,
      interval = interval,
      startTime = start_ms,
      endTime = end_ms,
      limit = limit
    )
  )

  data <- jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  
  df <- as.data.frame(data, stringsAsFactors = FALSE)

  colnames(df) <- c(
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_asset_volume", "num_trades",
    "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
  )

  df[] <- lapply(df, type.convert, as.is = TRUE)
  df$open_time  <- as.POSIXct(df$open_time / 1000, origin = "1970-01-01", tz = tz)
  df$close_time <- as.POSIXct(df$close_time / 1000, origin = "1970-01-01", tz = tz)

  return(df)
}

# document url: https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data

# agent <- NewsReaderAgent$new()
# df <- agent$fetch_binance_klines(symbol = "ETHUSDT", interval = "15m")
# View(df)

# df <- fetch_binance_klines(
#   symbol = "ETHUSDT",
#   interval = "1m",
#   start_time = "2025-07-05 08:00:00",
#   end_time = "2025-07-05 09:00:00",
#   limit = 1000,
#   tz = "Asia/Hong_Kong"
# )
# 
# tail(df$open_time)
# attr(df$open_time, "tzone")
# View(df)