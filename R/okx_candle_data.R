#' @import data.table

.okx_candle_timeframe_to <- function(tag, unit = c("seconds", "minutes", "hours")) {
  unit <- match.arg(unit)
  m <- regexec("^([0-9]+)([smhdw])$", tag, ignore.case = TRUE)
  parts <- regmatches(tag, m)[[1]]
  if (length(parts) < 3) {
    stop("Invalid timeframe tag: ", tag)
  }
  value <- as.numeric(parts[2])
  suffix <- tolower(parts[3])
  
  minutes <- switch(suffix,
    s = value / 60,   # seconds to minutes
    m = value,
    h = value * 60,
    d = value * 60 * 24,
    w = value * 60 * 24 * 7,
    stop("Unknown suffix: ", suffix)
  )
  
  switch(unit,
    seconds = minutes * 60,
    minutes = minutes,
    hours   = minutes / 60
  )
}

#' Get Last Completed Candle Time
#'
#' Floor current time to the last completed candle start for the given bar.
#'
#' @param bar Character. OKX timeframe tag (e.g., "1m", "15m", "4h", "1d").
#' @param tz Character. IANA timezone (e.g., "UTC").
#'
#' @return POSIXct in the given timezone.
#' @export
get_source_utime_okx_candle <- function(bar, tz) {
  now <- Sys.time()
  sec_duration <- .okx_candle_timeframe_to(bar)
  sec_since_midnight <- as.numeric(difftime(now,
                                            as.POSIXct(format(now, "%Y-%m-%d 00:00:00"), tz = tz),
                                            units = "secs"))
  floored <- sec_since_midnight %/% sec_duration * sec_duration
  as.POSIXct(format(now, "%Y-%m-%d 00:00:00"), tz = tz) + floored
}

#' @export
get_source_data_okx_candle <- function(inst_id, bar, limit = 100L, config, tz = "Asia/Hong_Kong") {
  dt <- okxr::get_market_candles(inst_id, bar, limit, config, tz)
  if (is.null(dt)) {
    return(NULL)
  } else {
    data.table::setnames(dt, 'timestamp', 'datetime')
    return(invisible(dt[confirm==1L]))
  }
}

#' @export
get_source_hist_data_okx_candle <- function(inst_id, bar, before = NULL, limit = 100L, config, tz = "Asia/Hong_Kong") {
  dt <- okxr::get_market_history_candles(inst_id, bar, before, limit, config, tz)
  if (is.null(dt)) {
    return(NULL)
  } else {
    data.table::setnames(dt, 'timestamp', 'datetime')
    return(invisible(dt[confirm==1L]))
  }
}

#' Download Candle File from VM
#'
#' Use gcloud scp to copy a candle .rds file from a VM to local.
#'
#' @param inst_id Character. Instrument ID (e.g., "ETH-USDT-SWAP").
#' @param bar Character. Timeframe tag (e.g., "15m").
#' @param crypto_data_path Character. Root data path (default: env Crypto_Data_Path).
#' @param exchange Character. Exchange subfolder (default: "okx").
#' @param vm_name Character. Remote VM name.
#' @param vm_zone Character. Remote VM zone.
#'
#' @return Integer status code from system().
#' @export
download_candle_from_vm <- function(inst_id = NULL, bar = "4H", crypto_data_path = Sys.getenv("Crypto_Data_Path"), exchange = 'okx', vm_name, vm_zone) {
  okx_data_path <- sprintf("%s/%s", crypto_data_path, exchange)
  if (is.null(inst_id)) {
    system(sprintf("gcloud compute scp --recurse %s:%s/* %s --zone=%s", 
      vm_name, okx_data_path, okx_data_path, vm_zone))
  } else {
    system(sprintf("gcloud compute scp %s:%s/%s_%s.rds %s --zone=%s", 
      vm_name, okx_data_path, inst_id, bar, okx_data_path, vm_zone))
  }
}

#' Upload Candle File to VM
#'
#' Use gcloud scp to copy a local candle .rds file to a VM.
#'
#' @param inst_id Character. Instrument ID (e.g., "ETH-USDT-SWAP").
#' @param bar Character. Timeframe tag (e.g., "15m").
#' @param crypto_data_path Character. Root data path (default: env Crypto_Data_Path).
#' @param exchange Character. Exchange subfolder (default: "okx").
#' @param vm_name Character. Remote VM name.
#' @param vm_zone Character. Remote VM zone.
#'
#' @return Integer status code from system().
#' @export
upload_candle_to_vm <- function(inst_id = NULL, bar = "4H", crypto_data_path = Sys.getenv("Crypto_Data_Path"), exchange = 'okx', vm_name, vm_zone) {
  okx_data_path <- sprintf("%s/%s", crypto_data_path, exchange)
  if (is.null(inst_id)) {
    system(sprintf("gcloud compute scp --recurse %s/* %s:%s --zone=%s", 
      okx_data_path, vm_name, okx_data_path, vm_zone))
  } else {
    system(sprintf("gcloud compute scp %s/%s_%s.rds %s:%s --zone=%s", 
      okx_data_path, inst_id, bar, vm_name, okx_data_path, vm_zone))
  }
}

#' @export
detect_time_gaps_okx_candle <- function(dt, bar = '4H', tolerance = 0.0001) {

  dt <- dt[order(as.numeric(dt$datetime)), ]
  ts_numeric <- as.numeric(dt$datetime)
  dt_minutes <- diff(ts_numeric) / 60

  expected_dt <- .okx_candle_timeframe_to(bar, 'minutes')
  gap_idx <- which(dt_minutes > expected_dt * (1 + tolerance))
  
  if (length(gap_idx) == 0) {
    message("✅ No time gaps detected.")
    return(data.frame())
  }

  data.table::data.table(
    gap_index     = gap_idx,
    from_time     = dt$datetime[gap_idx],
    to_time       = dt$datetime[gap_idx + 1],
    actual_minutes  = dt_minutes[gap_idx],
    expected_minutes = expected_dt
  )
}



# prep_candle_dt <- function(candle_df, bg_time = NULL, ed_time = NULL, tz = Sys.timezone()) {
# 
#   DT <- data.table::data.table(
#     datetime = as.POSIXct(candle_df$timestamp),
#     open = as.numeric(candle_df$open),
#     high = as.numeric(candle_df$high),
#     low = as.numeric(candle_df$low),
#     close = as.numeric(candle_df$close)
#   )
# 
#   # stopifnot(
#   #   "datetime" %in% names(DT),
#   #   "open" %in% names(DT),
#   #   "high" %in% names(DT),
#   #   "low" %in% names(DT),
#   #   "close" %in% names(DT),
#   #   inherits(DT$datetime, "POSIXct") || inherits(DT$datetime, "Date"),
#   #   is.numeric(DT$close)
#   # )
#   
#   if (!is.null(bg_time)) DT <- DT[datetime >= as.POSIXct(bg_time, tz = tz), ]
#   if (!is.null(ed_time)) DT <- DT[datetime < as.POSIXct(ed_time, tz = tz), ]
# 
#   data.table::setorder(DT, datetime)
#   
#   data.table::setattr(DT, "inst_id", attr(candle_df, 'inst_id'))
#   data.table::setattr(DT, "bar", attr(candle_df, 'bar'))
#   DT
# }