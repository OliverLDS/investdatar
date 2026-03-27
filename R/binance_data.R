#' Fetch Kline Data from Binance Futures API
#'
#' Retrieves candlestick data from Binance USDT-margined futures. If
#' `paginate = TRUE`, the function keeps requesting pages until the requested
#' time range is exhausted or the API returns fewer rows than `limit`.
#'
#' @param symbol Trading pair symbol.
#' @param interval Candlestick interval.
#' @param start_time Optional start time.
#' @param end_time Optional end time.
#' @param limit Page size. Binance futures allows up to 1500.
#' @param tz Time zone applied to returned timestamps.
#' @param paginate Logical. Request multiple pages when needed.
#'
#' @return Standardized OHLCV `data.table` with `source`, `symbol`, `interval`,
#'   `datetime`, `date`, `open`, `high`, `low`, `close`, and `volume`, plus
#'   Binance-specific columns such as `close_time` and trade counts.
#' @export
get_source_data_binance_klines <- function(symbol = "ETHUSDT", interval = "1m",
                                           start_time = NULL, end_time = NULL,
                                           limit = 1500L, tz = "UTC",
                                           paginate = TRUE) {
  limit <- as.integer(limit)
  if (is.na(limit) || limit <= 0L) {
    stop("limit must be a positive integer.")
  }

  start_ms <- if (is.null(start_time)) NULL else as.numeric(as.POSIXct(start_time, tz = tz)) * 1000
  end_ms <- if (is.null(end_time)) NULL else as.numeric(as.POSIXct(end_time, tz = tz)) * 1000
  interval_seconds <- .parse_frequency(interval)$seconds

  fetch_page <- function(page_start_ms = NULL) {
    query <- list(
      symbol = symbol,
      interval = interval,
      startTime = page_start_ms,
      endTime = end_ms,
      limit = limit
    )
    query <- query[!vapply(query, is.null, logical(1))]

    res <- httr::GET("https://fapi.binance.com/fapi/v1/klines", query = query)
    httr::stop_for_status(res)
    jsonlite::fromJSON(httr::content(res, "text", encoding = "UTF-8"))
  }

  pages <- list()
  next_start_ms <- start_ms
  repeat {
    page <- fetch_page(next_start_ms)
    if (length(page) == 0L) {
      break
    }

    page_dt <- data.table::as.data.table(page)
    if (nrow(page_dt) == 0L) {
      break
    }

    data.table::setnames(page_dt, c(
      "open_time", "open", "high", "low", "close", "volume",
      "close_time", "quote_asset_volume", "num_trades",
      "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
    ))

    numeric_cols <- setdiff(names(page_dt), c("open_time", "close_time"))
    page_dt[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]
    page_dt[, datetime := as.POSIXct(open_time / 1000, origin = "1970-01-01", tz = tz)]
    page_dt[, close_time := as.POSIXct(close_time / 1000, origin = "1970-01-01", tz = tz)]
    page_dt[, open_time := NULL]
    page_dt <- .standardize_market_ohlcv(
      page_dt,
      source = "binance",
      symbol = symbol,
      interval = interval,
      time_col = "datetime",
      tz = tz
    )

    pages[[length(pages) + 1L]] <- page_dt

    if (!paginate || nrow(page_dt) < limit || is.null(end_ms)) {
      if (!paginate || nrow(page_dt) < limit) {
        break
      }
    }

    next_start_ms <- as.numeric(max(page_dt$datetime)) * 1000 + interval_seconds * 1000
    if (!is.null(end_ms) && next_start_ms > end_ms) {
      break
    }
    if (!paginate) {
      break
    }
  }

  out <- data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
  if (nrow(out) == 0L) {
    return(out)
  }

  data.table::setorderv(out, "datetime")
  out <- unique(out, by = c("symbol", "interval", "datetime"))
  out[]
}
