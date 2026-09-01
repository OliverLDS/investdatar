#' @import data.table

.okx_candle_timeframe_to <- function(tag, unit = c("seconds", "minutes", "hours")) {
  unit <- match.arg(unit)
  parsed <- .parse_frequency(tag)
  minutes <- parsed$seconds / 60

  switch(unit,
    seconds = minutes * 60,
    minutes = minutes,
    hours = minutes / 60
  )
}

#' Get Last Completed Candle Time
#'
#' Floor current time to the last completed candle start for the given bar.
#'
#' @param bar Character. OKX timeframe tag (e.g., `"1m"`, `"15m"`, `"4H"`, `"1D"`).
#' @param tz Character. IANA timezone.
#'
#' @return POSIXct.
#' @export
get_source_utime_okx_candle <- function(bar, tz = "UTC") {
  infer_source_utime_from_frequency(bar, reference_time = Sys.time(), tz = tz)
}

.normalize_okx_candles <- function(dt, inst_id = NULL, bar = NULL) {
  if (is.null(dt)) {
    return(NULL)
  }

  dt <- data.table::as.data.table(dt)
  if ("timestamp" %in% names(dt) && !"datetime" %in% names(dt)) {
    data.table::setnames(dt, "timestamp", "datetime")
  }

  keep <- intersect(
    c("datetime", "open", "high", "low", "close", "volume", "volCcy", "volCcyQuote", "confirm"),
    names(dt)
  )
  dt <- dt[, keep, with = FALSE]

  if ("confirm" %in% names(dt)) {
    dt <- dt[confirm == 1L]
  }

  numeric_cols <- intersect(c("open", "high", "low", "close", "volume", "volCcy", "volCcyQuote"), names(dt))
  dt[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]
  dt[, datetime := as.POSIXct(datetime, tz = attr(datetime, "tzone") %||% "UTC")]

  if (!is.null(inst_id) && !"inst_id" %in% names(dt)) {
    dt[, inst_id := inst_id]
  }
  if (!is.null(bar) && !"bar" %in% names(dt)) {
    dt[, bar := bar]
  }

  .standardize_market_ohlcv(
    dt,
    source = "okx",
    symbol = inst_id,
    interval = bar,
    time_col = "datetime"
  )
}

`%||%` <- function(x, y) if (is.null(x)) y else x

#' Get Latest OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param limit Integer page size.
#' @param config Optional OKX API config. If omitted, defaults from the
#'   package config and `OKX_API_KEY` / `OKX_SECRET_KEY` /
#'   `OKX_PASSPHRASE` environment variables are used.
#' @param tz Output time zone.
#'
#' @return `data.table` or `NULL`.
#' @export
get_source_data_okx_candle <- function(inst_id, bar, limit = 100L, config = NULL, tz = "UTC") {
  config <- .get_api_config("okx", config = config)
  .require_suggested_package("okxr", "to retrieve OKX candles.")
  dt <- okxr::get_market_candles(inst_id, bar, limit = limit, config = config, tz = tz)
  .normalize_okx_candles(dt, inst_id = inst_id, bar = bar)
}

#' Get Historical OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param before Optional pagination cursor. Date-time cursors are interpreted
#'   in `tz` and sent to OKX as exact Unix-millisecond values.
#' @param limit Integer page size.
#' @param config Optional OKX API config. If omitted, defaults from the
#'   package config and `OKX_API_KEY` / `OKX_SECRET_KEY` /
#'   `OKX_PASSPHRASE` environment variables are used.
#' @param tz Output time zone.
#'
#' @return `data.table` or `NULL`.
#' @export
get_source_hist_data_okx_candle <- function(inst_id, bar, before = NULL, limit = 100L, config = NULL, tz = "UTC") {
  before_ms <- .okx_history_cursor_to_ms(before, tz = tz)
  response <- .http_get_json(
    "https://www.okx.com/api/v5/market/history-candles",
    query = list(instId = inst_id, bar = bar, after = before_ms, limit = as.integer(limit))
  )
  dt <- .okx_history_candle_response_to_table(response, tz = tz)
  .normalize_okx_candles(dt, inst_id = inst_id, bar = bar)
}

.okx_history_cursor_to_ms <- function(before, tz = "UTC") {
  if (is.null(before)) return(NULL)
  if (length(before) != 1L) stop("before must be a single OKX history cursor.", call. = FALSE)

  numeric_cursor <- suppressWarnings(as.numeric(before))
  seconds <- if (!is.na(numeric_cursor) && is.character(before) && grepl("^[0-9]+(?:[.][0-9]+)?$", before)) {
    if (numeric_cursor >= 1e11) numeric_cursor / 1000 else numeric_cursor
  } else if (!is.na(numeric_cursor) && is.numeric(before)) {
    if (numeric_cursor >= 1e11) numeric_cursor / 1000 else numeric_cursor
  } else {
    timestamp <- as.POSIXct(before, tz = tz)
    if (is.na(timestamp)) stop("before must be a parseable date-time or Unix timestamp.", call. = FALSE)
    as.numeric(timestamp)
  }
  formatC(seconds * 1000, format = "f", digits = 0)
}

.okx_history_candle_response_to_table <- function(response, tz = "UTC") {
  if (!identical(as.character(response$code), "0")) {
    stop(
      "OKX history candles request failed",
      if (!is.null(response$code)) paste0(" (code ", response$code, ")"),
      if (!is.null(response$msg) && nzchar(response$msg)) paste0(": ", response$msg),
      call. = FALSE
    )
  }
  raw <- response$data
  if (is.null(raw) || length(raw) == 0L) return(NULL)
  dt <- data.table::as.data.table(raw)
  if (ncol(dt) < 9L) stop("OKX history candles response has an invalid data shape.", call. = FALSE)
  data.table::setnames(
    dt,
    old = names(dt)[seq_len(9L)],
    new = c("timestamp", "open", "high", "low", "close", "volume", "volCcy", "volCcyQuote", "confirm")
  )
  dt[, timestamp := as.POSIXct(as.numeric(timestamp) / 1000, origin = "1970-01-01", tz = tz)]
  dt
}

#' Get Local OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param local_path Optional OKX storage path.
#' @param storage Local storage mode: monolithic `"single"` or monthly
#'   partitioned `"monthly"`.
#' @param from,to Optional bounds used to prune monthly partitions before read.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_okx_candle <- function(inst_id, bar, local_path = NULL,
                                 storage = c("single", "monthly"),
                                 from = NULL, to = NULL) {
  storage <- match.arg(storage)
  if (is.null(local_path)) {
    local_path <- get_source_data_path("crypto", subdir = "okx")
  }
  local_file <- file.path(local_path, sprintf("%s_%s.rds", inst_id, bar))
  if (storage == "monthly") {
    return(get_local_data_partitioned(local_file, "datetime", from = from, to = to, order_cols = "datetime"))
  }
  .read_local_data_table(local_file, sort_cols = "datetime")
}

#' Synchronize Local OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param config Optional OKX API config. If omitted, defaults from the
#'   package config and `OKX_API_KEY` / `OKX_SECRET_KEY` /
#'   `OKX_PASSPHRASE` environment variables are used.
#' @param local_path Optional OKX storage path.
#' @param mode Either `"latest"` or `"history"`.
#' @param before Optional history cursor.
#' @param limit Integer page size.
#' @param tz Output time zone.
#' @param storage Local storage mode: monolithic `"single"` or monthly
#'   partitioned `"monthly"`.
#'
#' @return A sync result list.
#' @export
sync_local_okx_candle <- function(inst_id, bar, config = NULL, local_path = NULL,
                                  mode = c("latest", "history"), before = NULL,
                                  limit = 100L, tz = "UTC",
                                  storage = c("single", "monthly")) {
  mode <- match.arg(mode)
  storage <- match.arg(storage)
  config <- .get_api_config("okx", config = config)
  if (is.null(local_path)) {
    local_path <- get_source_data_path("crypto", subdir = "okx", create = TRUE)
  }

  local_file_path <- file.path(local_path, sprintf("%s_%s.rds", inst_id, bar))
  if (identical(mode, "history")) {
    new_dt <- get_source_hist_data_okx_candle(inst_id, bar, before = before, limit = limit, config = config, tz = tz)
    source_utime <- NULL
  } else {
    new_dt <- get_source_data_okx_candle(inst_id, bar, limit = limit, config = config, tz = tz)
    source_utime <- get_source_utime_okx_candle(bar = bar, tz = tz)
  }

  sync_fun <- if (storage == "monthly") sync_local_data_partitioned else sync_local_data
  args <- list(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = "datetime",
    order_cols = "datetime",
    source_utime = source_utime
  )
  if (storage == "monthly") args$time_col <- "datetime"
  do.call(sync_fun, args)
}

#' Repair Local OKX Candle Data From Multiple History Pages
#'
#' Fetches multiple OKX historical candle pages in memory and writes the merged
#' repair result to local storage with one `sync_local_data()` call.
#'
#' @param before Character, numeric, or date-time vector of OKX history
#'   pagination cursors. Date-time cursors are interpreted in `tz`.
#' @inheritParams sync_local_okx_candle
#'
#' @return A sync result list.
#' @export
repair_local_okx_candle_gaps <- function(inst_id, bar, before, config = NULL, local_path = NULL,
                                         limit = 100L, tz = "UTC",
                                         storage = c("single", "monthly")) {
  if (missing(before) || is.null(before) || length(before) == 0L) {
    stop("before must contain at least one OKX history pagination cursor.")
  }
  config <- .get_api_config("okx", config = config)
  storage <- match.arg(storage)
  if (is.null(local_path)) {
    local_path <- get_source_data_path("crypto", subdir = "okx", create = TRUE)
  }

  batches <- lapply(before, function(cursor) {
    get_source_hist_data_okx_candle(
      inst_id = inst_id,
      bar = bar,
      before = cursor,
      limit = limit,
      config = config,
      tz = tz
    )
  })

  combined <- data.table::rbindlist(batches, use.names = TRUE, fill = TRUE)
  sync_fun <- if (storage == "monthly") sync_local_data_partitioned else sync_local_data
  args <- list(
    new_data = combined,
    local_file_path = file.path(local_path, sprintf("%s_%s.rds", inst_id, bar)),
    key_cols = "datetime",
    order_cols = "datetime",
    source_utime = NULL
  )
  if (storage == "monthly") args$time_col <- "datetime"
  do.call(sync_fun, args)
}

#' Detect Time Gaps In OKX Candle Data
#'
#' @param dt A candle `data.table`.
#' @param bar Candle interval.
#' @param tolerance Numeric tolerance for fixed-width gap detection.
#'
#' @return A `data.table`.
#' @export
detect_time_gaps_okx_candle <- function(dt, bar = "4H", tolerance = 1e-04) {
  detect_time_gaps(dt, time_col = "datetime", frequency = bar, tolerance = tolerance)
}
