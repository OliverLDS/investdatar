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
#' @param config OKX API config.
#' @param tz Output time zone.
#'
#' @return `data.table` or `NULL`.
#' @export
get_source_data_okx_candle <- function(inst_id, bar, limit = 100L, config, tz = "UTC") {
  .require_suggested_package("okxr", "to retrieve OKX candles.")
  dt <- okxr::get_market_candles(inst_id, bar, limit = limit, config = config, tz = tz)
  .normalize_okx_candles(dt, inst_id = inst_id, bar = bar)
}

#' Get Historical OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param before Optional pagination cursor.
#' @param limit Integer page size.
#' @param config OKX API config.
#' @param tz Output time zone.
#'
#' @return `data.table` or `NULL`.
#' @export
get_source_hist_data_okx_candle <- function(inst_id, bar, before = NULL, limit = 100L, config, tz = "UTC") {
  .require_suggested_package("okxr", "to retrieve OKX historical candles.")
  dt <- okxr::get_market_history_candles(inst_id, bar, before = before, limit = limit, config = config, tz = tz)
  .normalize_okx_candles(dt, inst_id = inst_id, bar = bar)
}

#' Get Local OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param local_path Optional OKX storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_okx_candle <- function(inst_id, bar, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("crypto", subdir = "okx")
  }
  .read_local_data_table(file.path(local_path, sprintf("%s_%s.rds", inst_id, bar)), sort_cols = "datetime")
}

#' Synchronize Local OKX Candle Data
#'
#' @param inst_id Instrument identifier.
#' @param bar Candle interval.
#' @param config OKX API config.
#' @param local_path Optional OKX storage path.
#' @param mode Either `"latest"` or `"history"`.
#' @param before Optional history cursor.
#' @param limit Integer page size.
#' @param tz Output time zone.
#'
#' @return A sync result list.
#' @export
sync_local_okx_candle <- function(inst_id, bar, config, local_path = NULL,
                                  mode = c("latest", "history"), before = NULL,
                                  limit = 100L, tz = "UTC") {
  mode <- match.arg(mode)
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

  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = "datetime",
    order_cols = "datetime",
    source_utime = source_utime
  )
}

#' Repair Local OKX Candle Data From Multiple History Pages
#'
#' Fetches multiple OKX historical candle pages in memory and writes the merged
#' repair result to local storage with one `sync_local_data()` call.
#'
#' @param before Character/numeric vector of OKX history pagination cursors.
#' @inheritParams sync_local_okx_candle
#'
#' @return A sync result list.
#' @export
repair_local_okx_candle_gaps <- function(inst_id, bar, before, config, local_path = NULL,
                                         limit = 100L, tz = "UTC") {
  if (missing(before) || is.null(before) || length(before) == 0L) {
    stop("before must contain at least one OKX history pagination cursor.")
  }
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

  sync_local_data_batches(
    batches = batches,
    local_file_path = file.path(local_path, sprintf("%s_%s.rds", inst_id, bar)),
    key_cols = "datetime",
    order_cols = "datetime",
    source_utime = NULL
  )
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
