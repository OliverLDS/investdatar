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
download_candle_from_vm <- function(inst_id, bar, crypto_data_path = Sys.getenv("Crypto_Data_Path"), exchange = 'okx', vm_name, vm_zone) {
  system(sprintf("gcloud compute scp %s:%s/%s/%s_%s.rds %s/%s --zone=%s", 
    vm_name, crypto_data_path, exchange, inst_id, bar, crypto_data_path, exchange, vm_zone))
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
upload_candle_to_vm <- function(inst_id, bar, crypto_data_path = Sys.getenv("Crypto_Data_Path"), exchange = 'okx', vm_name, vm_zone) {
  system(sprintf("gcloud compute scp %s/%s/%s_%s.rds %s:%s/%s --zone=%s", 
    crypto_data_path, exchange, inst_id, bar, vm_name, crypto_data_path, exchange, vm_zone))
}

#' Detect Time Gaps in Candle Data
#'
#' Find gaps larger than expected between consecutive candle timestamps.
#'
#' @param dt data.frame or data.table with column timestamp (POSIXct).
#' @param bar Character. OKX timeframe tag (default: "4H").
#' @param tolerance Numeric. Relative tolerance (default: 0.0001).
#'
#' @return data.table with columns: gap_index, from_time, to_time, actual_minutes, expected_minutes.
#' @export
detect_time_gaps_okx_candle <- function(dt, bar = '4H', tolerance = 0.0001) {

  dt <- dt[order(as.numeric(dt$timestamp)), ]
  ts_numeric <- as.numeric(dt$timestamp)
  dt_minutes <- diff(ts_numeric) / 60

  expected_dt <- .okx_candle_timeframe_to(bar, 'minutes')
  gap_idx <- which(dt_minutes > expected_dt * (1 + tolerance))
  
  if (length(gap_idx) == 0) {
    message("✅ No time gaps detected.")
    return(data.frame())
  }

  data.table::data.table(
    gap_index     = gap_idx,
    from_time     = dt$timestamp[gap_idx],
    to_time       = dt$timestamp[gap_idx + 1],
    actual_minutes  = dt_minutes[gap_idx],
    expected_minutes = expected_dt
  )
}

# sync_and_save_candles <- function(df_new, data_path) {
#   df_new <- df_new[df_new$confirm==1L,]
#   key_column <- "timestamp"
#   
#   if (!file.exists(data_path)) {
#     df_new <- df_new[order(as.numeric(df_new$timestamp)), ] 
#     .safe_save_rds(df_new, data_path)
#     return(TRUE)
#   } else {
#     df_old <- .safe_read_rds(data_path)
#     old_keys <- as.character(unique(df_old[[key_column]]))
#     res <- util_sync_new_records(df_new, key_column, old_keys)
#     if (res$has_new) {
#       df_combined <- rbind(df_old, res$df)
#       df_combined <- df_combined[order(as.numeric(df_combined[[key_column]])), ]
#       .safe_save_rds(df_combined, data_path)
#     }
#     return(res$has_new)
#   }
# }

# load_candle_df <- function(inst_id, bar, root_path) { # we need to allow for more than one exchange in the future
#   candle_df_path <- file.path(root_path, sprintf("%s_%s.rds", inst_id, bar))
#   candle_df <- readRDS(candle_df_path)
#   data.table::setattr(candle_df, "inst_id", inst_id)
#   data.table::setattr(candle_df, "bar", bar)
#   candle_df
# }

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