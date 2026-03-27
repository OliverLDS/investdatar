.require_suggested_package <- function(pkg, why = NULL) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    msg <- sprintf("Package '%s' is required", pkg)
    if (!is.null(why)) {
      msg <- sprintf("%s %s", msg, why)
    }
    stop(msg, call. = FALSE)
  }
  invisible(TRUE)
}

.safe_read_rds <- function(path, default = NULL) {
  if (!file.exists(path)) {
    return(default)
  }
  readRDS(path)
}

.safe_save_rds <- function(object, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  saveRDS(object, path)
  invisible(path)
}

.meta_file_path <- function(local_file_path) {
  if (grepl("\\.rds$", local_file_path, ignore.case = TRUE)) {
    sub("\\.rds$", ".meta.rds", local_file_path, ignore.case = TRUE)
  } else {
    paste0(local_file_path, ".meta.rds")
  }
}

#' Get Local Data Metadata
#'
#' @param local_file_path Path to a local `.rds` data file.
#' @param fallback_to_file_mtime Logical. If no sidecar metadata file exists,
#'   use the data file modification time.
#'
#' @return A list of metadata, or `NULL`.
#' @export
get_local_data_meta <- function(local_file_path, fallback_to_file_mtime = TRUE) {
  meta_path <- .meta_file_path(local_file_path)
  meta <- .safe_read_rds(meta_path, default = NULL)

  if (is.null(meta) && fallback_to_file_mtime && file.exists(local_file_path)) {
    meta <- list(
      local_updated_at = as.POSIXct(file.info(local_file_path)$mtime, tz = "UTC")
    )
  }

  meta
}

#' Get Local Update Time
#'
#' @param local_file_path Path to a local `.rds` data file.
#'
#' @return POSIXct or `NULL`.
#' @export
get_local_data_utime <- function(local_file_path) {
  meta <- get_local_data_meta(local_file_path)
  if (is.null(meta)) {
    return(NULL)
  }
  meta$local_updated_at
}

.as_data_table <- function(x) {
  if (is.null(x)) {
    return(NULL)
  }
  data.table::as.data.table(x)
}

.unique_bind_rows <- function(old_dt, new_dt, key_cols, order_cols = key_cols) {
  all_dt <- data.table::rbindlist(list(old_dt, new_dt), use.names = TRUE, fill = TRUE)
  data.table::setorderv(all_dt, order_cols)
  if (length(key_cols) > 0L) {
    all_dt <- unique(all_dt, by = key_cols)
  } else {
    all_dt <- unique(all_dt)
  }
  data.table::setorderv(all_dt, order_cols)
  all_dt[]
}

.find_new_rows <- function(new_dt, key_cols, old_dt = NULL) {
  new_dt <- .as_data_table(new_dt)
  if (is.null(new_dt)) {
    return(NULL)
  }
  if (nrow(new_dt) == 0L) {
    return(new_dt[0])
  }

  if (is.null(old_dt) || nrow(old_dt) == 0L) {
    return(new_dt)
  }

  old_keys <- unique(old_dt[, key_cols, with = FALSE])
  new_dt[!old_keys, on = key_cols]
}

#' Synchronize Local Data
#'
#' Merges freshly retrieved source data into a local `.rds` file and stores a
#' sidecar metadata file containing the local update time and source update time.
#'
#' @param new_data A `data.table`-compatible object.
#' @param local_file_path Output `.rds` file path.
#' @param key_cols Character vector of key columns used for de-duplication.
#' @param order_cols Character vector used to sort the merged data.
#' @param source_utime Optional upstream update time recorded in metadata.
#' @param local_updated_at Optional local update time override.
#'
#' @return A list describing the sync result.
#' @export
sync_local_data <- function(new_data, local_file_path, key_cols, order_cols = key_cols,
                            source_utime = NULL, local_updated_at = Sys.time()) {
  new_dt <- .as_data_table(new_data)
  if (is.null(new_dt)) {
    return(list(updated = FALSE, reason = "new_data_is_null", data = NULL, file_path = local_file_path))
  }
  stopifnot(all(key_cols %in% names(new_dt)))

  old_dt <- .safe_read_rds(local_file_path, default = NULL)
  old_dt <- .as_data_table(old_dt)

  if (!is.null(old_dt) && nrow(old_dt) > 0L) {
    new_rows <- .find_new_rows(new_dt, key_cols = key_cols, old_dt = old_dt)
    merged_dt <- .unique_bind_rows(old_dt, new_rows, key_cols = key_cols, order_cols = order_cols)
    updated <- nrow(new_rows) > 0L
    n_new_rows <- nrow(new_rows)
  } else {
    data.table::setorderv(new_dt, order_cols)
    merged_dt <- unique(new_dt, by = key_cols)
    updated <- nrow(merged_dt) > 0L
    n_new_rows <- nrow(merged_dt)
  }

  if (updated || !file.exists(local_file_path)) {
    .safe_save_rds(merged_dt, local_file_path)
  }

  meta <- list(
    local_updated_at = as.POSIXct(local_updated_at, tz = "UTC"),
    source_updated_at = if (is.null(source_utime)) NULL else as.POSIXct(source_utime, tz = "UTC"),
    n_rows = nrow(merged_dt),
    key_cols = key_cols
  )
  .safe_save_rds(meta, .meta_file_path(local_file_path))

  list(
    updated = updated,
    n_new_rows = n_new_rows,
    n_rows = nrow(merged_dt),
    data = merged_dt,
    file_path = local_file_path,
    meta_path = .meta_file_path(local_file_path)
  )
}

.parse_candle_frequency <- function(tag) {
  m <- regexec("^([0-9]+)([smhdw])$", tag, ignore.case = TRUE)
  parts <- regmatches(tag, m)[[1]]
  if (length(parts) != 3L) {
    return(NULL)
  }

  value <- as.numeric(parts[2])
  unit <- tolower(parts[3])
  seconds <- switch(
    unit,
    s = value,
    m = value * 60,
    h = value * 3600,
    d = value * 86400,
    w = value * 7 * 86400,
    NULL
  )

  if (is.null(seconds)) {
    return(NULL)
  }

  list(type = "fixed", seconds = seconds, by = NULL, label = tag)
}

.parse_named_frequency <- function(frequency) {
  if (is.null(frequency) || !nzchar(frequency)) {
    return(NULL)
  }

  freq <- tolower(trimws(frequency))
  freq <- sub(",.*$", "", freq)

  if (freq %in% c("daily", "business daily")) {
    return(list(type = "fixed", seconds = 86400, by = "day", label = frequency))
  }
  if (freq %in% c("weekly")) {
    return(list(type = "fixed", seconds = 7 * 86400, by = "week", label = frequency))
  }
  if (freq %in% c("monthly")) {
    return(list(type = "calendar", seconds = NULL, by = "month", label = frequency))
  }
  if (freq %in% c("quarterly")) {
    return(list(type = "calendar", seconds = NULL, by = "quarter", label = frequency))
  }
  if (freq %in% c("annual", "yearly")) {
    return(list(type = "calendar", seconds = NULL, by = "year", label = frequency))
  }
  if (freq %in% c("hourly")) {
    return(list(type = "fixed", seconds = 3600, by = "hour", label = frequency))
  }
  if (freq %in% c("minute")) {
    return(list(type = "fixed", seconds = 60, by = "min", label = frequency))
  }

  NULL
}

.parse_frequency <- function(frequency) {
  if (is.numeric(frequency) && length(frequency) == 1L) {
    return(list(type = "fixed", seconds = as.numeric(frequency), by = NULL, label = as.character(frequency)))
  }

  if (is.character(frequency) && length(frequency) == 1L) {
    parsed <- .parse_candle_frequency(frequency)
    if (!is.null(parsed)) {
      return(parsed)
    }

    parsed <- .parse_named_frequency(frequency)
    if (!is.null(parsed)) {
      return(parsed)
    }
  }

  stop("Unsupported frequency: ", paste(frequency, collapse = ", "))
}

.coerce_time_vector <- function(x, tz = "UTC") {
  if (inherits(x, "POSIXt")) {
    return(as.POSIXct(x, tz = tz))
  }
  if (inherits(x, "Date")) {
    return(as.POSIXct(x, tz = tz))
  }
  as.POSIXct(x, tz = tz)
}

#' Infer Source Update Time From Frequency
#'
#' Provides a fallback source update time for resources that do not expose a
#' server-side update timestamp.
#'
#' @param frequency Frequency string or candle interval.
#' @param reference_time POSIXct reference time. Defaults to `Sys.time()`.
#' @param tz Time zone used for period flooring.
#'
#' @return POSIXct.
#' @export
infer_source_utime_from_frequency <- function(frequency, reference_time = Sys.time(), tz = "UTC") {
  parsed <- .parse_frequency(frequency)
  ref <- as.POSIXct(reference_time, tz = tz)

  if (parsed$type == "fixed") {
    seconds <- parsed$seconds
    day_start <- as.POSIXct(format(ref, "%Y-%m-%d 00:00:00", tz = tz), tz = tz)
    since_start <- as.numeric(difftime(ref, day_start, units = "secs"))
    floored <- since_start %/% seconds * seconds
    return(day_start + floored)
  }

  ref_lt <- as.POSIXlt(ref, tz = tz)
  if (parsed$by == "month") {
    ref_lt$mday <- 1L
  } else if (parsed$by == "quarter") {
    ref_lt$mon <- (ref_lt$mon %/% 3L) * 3L
    ref_lt$mday <- 1L
  } else if (parsed$by == "year") {
    ref_lt$mon <- 0L
    ref_lt$mday <- 1L
  }
  ref_lt$hour <- 0L
  ref_lt$min <- 0L
  ref_lt$sec <- 0L
  as.POSIXct(ref_lt, tz = tz)
}

#' Detect Time Gaps
#'
#' Detects gaps in a local time series using either a fixed or calendar
#' frequency.
#'
#' @param dt A data.table-compatible object.
#' @param time_col Name of the time column.
#' @param frequency Frequency string or candle interval.
#' @param tolerance Numeric tolerance applied to fixed-interval gaps.
#' @param tz Time zone used when coercing timestamps.
#'
#' @return A `data.table` describing detected gaps.
#' @export
detect_time_gaps <- function(dt, time_col, frequency, tolerance = 1e-04, tz = "UTC") {
  dt <- .as_data_table(dt)
  if (is.null(dt) || nrow(dt) <= 1L) {
    return(data.table::data.table())
  }
  if (!time_col %in% names(dt)) {
    stop("Column not found in dt: ", time_col)
  }

  parsed <- .parse_frequency(frequency)
  times <- .coerce_time_vector(dt[[time_col]], tz = tz)
  ord <- order(times)
  times <- times[ord]
  times <- unique(times)

  if (length(times) <= 1L) {
    return(data.table::data.table())
  }

  if (parsed$type == "fixed") {
    diff_seconds <- diff(as.numeric(times))
    gap_idx <- which(diff_seconds > parsed$seconds * (1 + tolerance))
    if (length(gap_idx) == 0L) {
      return(data.table::data.table())
    }

    return(data.table::data.table(
      gap_index = gap_idx,
      from_time = times[gap_idx],
      to_time = times[gap_idx + 1L],
      actual_seconds = diff_seconds[gap_idx],
      expected_seconds = parsed$seconds
    ))
  }

  if (inherits(dt[[time_col]], "Date")) {
    from <- as.Date(min(times), tz = tz)
    to <- as.Date(max(times), tz = tz)
    expected <- seq(from = from, to = to, by = parsed$by)
    actual <- unique(as.Date(times, tz = tz))
    missing <- setdiff(expected, actual)
    if (length(missing) == 0L) {
      return(data.table::data.table())
    }

    return(data.table::data.table(
      missing_time = missing,
      frequency = parsed$label
    ))
  }

  expected <- seq(from = min(times), to = max(times), by = parsed$by)
  missing <- setdiff(expected, times)
  if (length(missing) == 0L) {
    return(data.table::data.table())
  }

  data.table::data.table(
    missing_time = missing,
    frequency = parsed$label
  )
}

.read_local_data_table <- function(path, sort_cols = NULL) {
  dt <- .safe_read_rds(path, default = NULL)
  dt <- .as_data_table(dt)
  if (is.null(dt)) {
    return(NULL)
  }
  if (!is.null(sort_cols) && all(sort_cols %in% names(dt))) {
    data.table::setorderv(dt, sort_cols)
  }
  dt[]
}
