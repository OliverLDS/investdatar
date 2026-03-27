.standardize_market_ohlcv <- function(dt, source, symbol = NULL, interval = NULL,
                                      time_col = "datetime", tz = "UTC") {
  dt <- .as_data_table(dt)
  if (is.null(dt)) {
    return(NULL)
  }
  if (!time_col %in% names(dt)) {
    stop("Column not found in dt: ", time_col)
  }

  if (inherits(dt[[time_col]], "Date")) {
    dt[, datetime := as.POSIXct(get(time_col), tz = tz)]
  } else {
    dt[, datetime := as.POSIXct(get(time_col), tz = tz)]
  }
  dt[, date := as.Date(datetime, tz = tz)]

  if (!is.null(source)) {
    dt[, source := source]
  } else if (!"source" %in% names(dt)) {
    dt[, source := NA_character_]
  }

  if (!is.null(symbol)) {
    dt[, symbol := symbol]
  } else if (!"symbol" %in% names(dt)) {
    dt[, symbol := NA_character_]
  }

  if (!is.null(interval)) {
    dt[, interval := interval]
  } else if (!"interval" %in% names(dt)) {
    dt[, interval := NA_character_]
  }

  required_numeric <- c("open", "high", "low", "close", "volume")
  for (nm in required_numeric) {
    if (!nm %in% names(dt)) {
      dt[, (nm) := NA_real_]
    }
  }

  numeric_cols <- intersect(
    c("open", "high", "low", "close", "volume", "adj_close",
      "quote_asset_volume", "num_trades", "taker_buy_base_vol",
      "taker_buy_quote_vol", "volCcy", "volCcyQuote"),
    names(dt)
  )
  if (length(numeric_cols) > 0L) {
    dt[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]
  }

  first_cols <- c("source", "symbol", "interval", "datetime", "date",
                  "open", "high", "low", "close", "volume")
  data.table::setcolorder(dt, c(first_cols, setdiff(names(dt), first_cols)))
  data.table::setorderv(dt, "datetime")
  dt[]
}
