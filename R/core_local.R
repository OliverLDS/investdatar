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
  temp_path <- tempfile(
    pattern = paste0(".", basename(path), "."),
    tmpdir = dirname(path)
  )
  on.exit(unlink(temp_path), add = TRUE)

  saveRDS(object, temp_path)
  if (!file.rename(temp_path, path)) {
    if (!file.exists(path)) {
      stop("Could not move temporary RDS file into place: ", path, call. = FALSE)
    }

    backup_path <- tempfile(
      pattern = paste0(".", basename(path), ".backup."),
      tmpdir = dirname(path)
    )
    if (!file.rename(path, backup_path)) {
      stop("Could not preserve existing RDS file before replacement: ", path, call. = FALSE)
    }
    if (!file.rename(temp_path, path)) {
      restored <- file.rename(backup_path, path)
      stop(
        "Could not replace local RDS file: ", path,
        if (!restored) paste0(". Existing data remains at ", backup_path) else "",
        call. = FALSE
      )
    }
    unlink(backup_path)
  }
  invisible(path)
}

.empty_posixct <- function(n = 0L) {
  as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = "UTC")
}

.normalize_sync_summary <- function(summary, source_id, run_started_at,
                                    run_finished_at = Sys.time()) {
  dt <- data.table::copy(.as_data_table(summary))
  if (is.null(dt)) {
    dt <- data.table::data.table()
  }
  n <- nrow(dt)

  defaults <- list(
    source_id = rep(as.character(source_id), n),
    status = rep(NA_character_, n),
    updated = rep(FALSE, n),
    n_rows = rep(NA_integer_, n),
    n_new_rows = rep(NA_integer_, n),
    source_utime = .empty_posixct(n),
    local_utime = .empty_posixct(n),
    error_class = rep(NA_character_, n),
    error_message = rep(NA_character_, n),
    http_status = rep(NA_integer_, n),
    started_at = rep(as.POSIXct(run_started_at, tz = "UTC"), n),
    finished_at = rep(as.POSIXct(run_finished_at, tz = "UTC"), n),
    elapsed_seconds = rep(as.numeric(difftime(run_finished_at, run_started_at, units = "secs")), n)
  )

  for (nm in names(defaults)) {
    if (!nm %in% names(dt)) {
      dt[, (nm) := defaults[[nm]]]
    }
  }

  if ("error" %in% names(dt)) {
    dt[is.na(error_message) & !is.na(error), error_message := as.character(error)]
  }
  dt[!is.na(status) & status == "error" & is.na(error_class), error_class := "sync_error"]

  standard_cols <- names(defaults)
  data.table::setcolorder(dt, c(setdiff(names(dt), standard_cols), standard_cols))
  dt[]
}

.meta_file_path <- function(local_file_path) {
  if (grepl("\\.rds$", local_file_path, ignore.case = TRUE)) {
    sub("\\.rds$", ".meta.rds", local_file_path, ignore.case = TRUE)
  } else {
    paste0(local_file_path, ".meta.rds")
  }
}

.sync_run_log_dir <- function(local_path) {
  file.path(local_path, "_sync_runs")
}

.sync_run_log_path <- function(source_id, local_path, run_finished_at = Sys.time()) {
  stamp <- format(as.POSIXct(run_finished_at, tz = "UTC"), "%Y%m%dT%H%M%SZ", tz = "UTC")
  source_id <- gsub("[^A-Za-z0-9._-]+", "_", source_id)
  file.path(.sync_run_log_dir(local_path), sprintf("%s__%s.rds", source_id, stamp))
}

.write_sync_run_log <- function(source_id, summary, local_path, params = list(),
                                run_started_at = Sys.time(), run_finished_at = Sys.time()) {
  if (is.null(local_path) || !nzchar(local_path)) {
    return(NULL)
  }

  log_path <- .sync_run_log_path(source_id, local_path = local_path, run_finished_at = run_finished_at)
  .safe_save_rds(
    list(
      source_id = source_id,
      run_started_at = as.POSIXct(run_started_at, tz = "UTC"),
      run_finished_at = as.POSIXct(run_finished_at, tz = "UTC"),
      params = params,
      summary = .as_data_table(summary)
    ),
    log_path
  )
  log_path
}

#' Get The Latest Batch Sync Run Log
#'
#' @param source_id Source identifier used for the sync log filename prefix.
#' @param local_path Local source data path that contains the `_sync_runs`
#'   directory.
#'
#' @return A run-log list, or `NULL` when no matching log file exists.
#' @export
get_latest_sync_run <- function(source_id, local_path) {
  run_dir <- .sync_run_log_dir(local_path)
  if (!dir.exists(run_dir)) {
    return(NULL)
  }

  source_id <- gsub("[^A-Za-z0-9._-]+", "_", source_id)
  pattern <- sprintf("^%s__.*\\.rds$", source_id)
  paths <- list.files(run_dir, pattern = pattern, full.names = TRUE)
  if (length(paths) == 0L) {
    return(NULL)
  }

  .safe_read_rds(sort(paths)[[length(paths)]], default = NULL)
}

#' Check Whether A Batch Sync Run Succeeded
#'
#' A run is successful when its stored summary contains no row with an error
#' status or non-empty error message. Empty summaries are successful because an
#' intentionally empty registry has no failed work.
#'
#' @param run A run-log object returned by `get_latest_sync_run()`.
#'
#' @return Logical scalar.
#' @export
is_sync_run_successful <- function(run) {
  if (is.null(run) || is.null(run$summary)) return(FALSE)
  summary <- tryCatch(data.table::as.data.table(run$summary), error = function(e) NULL)
  if (is.null(summary)) return(FALSE)
  if (nrow(summary) == 0L) return(TRUE)

  if ("status" %in% names(summary)) {
    status <- tolower(trimws(as.character(summary$status)))
    if (any(status == "error", na.rm = TRUE)) return(FALSE)
  }
  for (nm in intersect(c("error", "error_message"), names(summary))) {
    value <- trimws(as.character(summary[[nm]]))
    if (any(!is.na(value) & nzchar(value))) return(FALSE)
  }
  TRUE
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

.prepare_compare_rows <- function(dt, cols, order_cols) {
  dt <- data.table::copy(.as_data_table(dt))
  if (is.null(dt)) {
    return(NULL)
  }

  missing_cols <- setdiff(cols, names(dt))
  if (length(missing_cols) > 0L) {
    for (nm in missing_cols) {
      dt[, (nm) := NA]
    }
  }

  data.table::setcolorder(dt, cols)
  if (!is.null(order_cols) && all(order_cols %in% names(dt))) {
    data.table::setorderv(dt, order_cols)
  }
  dt[]
}

.has_changed_rows <- function(old_dt, new_dt, key_cols, order_cols = key_cols) {
  old_dt <- .as_data_table(old_dt)
  new_dt <- .as_data_table(new_dt)

  if (is.null(new_dt) || nrow(new_dt) == 0L) {
    return(FALSE)
  }
  if (is.null(old_dt) || nrow(old_dt) == 0L) {
    return(TRUE)
  }

  new_unique <- unique(new_dt, by = key_cols)
  old_matching <- old_dt[new_unique[, key_cols, with = FALSE], on = key_cols, nomatch = 0L]
  compare_cols <- union(names(old_matching), names(new_unique))

  old_norm <- .prepare_compare_rows(old_matching, cols = compare_cols, order_cols = key_cols)
  new_norm <- .prepare_compare_rows(new_unique, cols = compare_cols, order_cols = key_cols)

  !identical(old_norm, new_norm)
}

.upsert_rows_by_key <- function(old_dt, new_dt, key_cols, order_cols = key_cols) {
  new_dt <- .as_data_table(new_dt)
  if (is.null(new_dt)) {
    return(old_dt)
  }

  new_unique <- unique(new_dt, by = key_cols)
  if (is.null(old_dt) || nrow(old_dt) == 0L) {
    data.table::setorderv(new_unique, order_cols)
    return(new_unique[])
  }

  old_dt <- .as_data_table(old_dt)
  old_remaining <- old_dt[!new_unique[, key_cols, with = FALSE], on = key_cols]
  merged_dt <- data.table::rbindlist(list(old_remaining, new_unique), use.names = TRUE, fill = TRUE)
  data.table::setorderv(merged_dt, order_cols)
  merged_dt[]
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
  new_dt <- unique(new_dt, by = key_cols)

  old_dt <- .safe_read_rds(local_file_path, default = NULL)
  old_dt <- .as_data_table(old_dt)

  if (!is.null(old_dt) && nrow(old_dt) > 0L) {
    new_rows <- .find_new_rows(new_dt, key_cols = key_cols, old_dt = old_dt)
    changed_existing <- .has_changed_rows(old_dt, new_dt, key_cols = key_cols, order_cols = order_cols)
    merged_dt <- .upsert_rows_by_key(old_dt, new_dt, key_cols = key_cols, order_cols = order_cols)
    updated <- nrow(new_rows) > 0L || changed_existing
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

#' Synchronize Local Data From In-Memory Batches
#'
#' Combines multiple freshly retrieved data batches in memory, de-duplicates
#' them by `key_cols`, then calls `sync_local_data()` once. This is useful for
#' gap repair workflows where calling `sync_local_data()` once per small page
#' would repeatedly rewrite the same local `.rds` file.
#'
#' @param batches A list of `data.table`-compatible batches.
#' @inheritParams sync_local_data
#'
#' @return A sync result list.
#' @export
sync_local_data_batches <- function(batches, local_file_path, key_cols, order_cols = key_cols,
                                    source_utime = NULL, local_updated_at = Sys.time()) {
  if (is.null(batches)) {
    batches <- list()
  }
  if (!is.list(batches) || data.table::is.data.table(batches) || is.data.frame(batches)) {
    batches <- list(batches)
  }

  batch_dts <- lapply(batches, .as_data_table)
  batch_dts <- Filter(function(x) !is.null(x) && nrow(x) > 0L, batch_dts)
  if (length(batch_dts) == 0L) {
    combined_dt <- data.table::data.table()
  } else {
    combined_dt <- data.table::rbindlist(batch_dts, use.names = TRUE, fill = TRUE)
    combined_dt <- unique(combined_dt, by = key_cols)
    if (!is.null(order_cols) && all(order_cols %in% names(combined_dt))) {
      data.table::setorderv(combined_dt, order_cols)
    }
  }

  sync_local_data(
    new_data = combined_dt,
    local_file_path = local_file_path,
    key_cols = key_cols,
    order_cols = order_cols,
    source_utime = source_utime,
    local_updated_at = local_updated_at
  )
}

.partition_data_dir <- function(local_file_path) {
  if (grepl("\\.rds$", local_file_path, ignore.case = TRUE)) {
    sub("\\.rds$", ".parts", local_file_path, ignore.case = TRUE)
  } else {
    paste0(local_file_path, ".parts")
  }
}

.monthly_partition_id <- function(x, tz = "UTC") {
  format(as.POSIXct(x, tz = tz), "%Y-%m", tz = tz)
}

.partition_file_paths <- function(local_file_path) {
  partition_dir <- .partition_data_dir(local_file_path)
  if (!dir.exists(partition_dir)) return(character())
  sort(list.files(partition_dir, pattern = "^[0-9]{4}-[0-9]{2}\\.rds$", full.names = TRUE))
}

.filter_partition_paths <- function(paths, from = NULL, to = NULL, tz = "UTC") {
  if (length(paths) == 0L || (is.null(from) && is.null(to))) return(paths)
  ids <- sub("\\.rds$", "", basename(paths))
  if (!is.null(from)) ids_from <- .monthly_partition_id(from, tz = tz) else ids_from <- min(ids)
  if (!is.null(to)) ids_to <- .monthly_partition_id(to, tz = tz) else ids_to <- max(ids)
  paths[ids >= ids_from & ids <= ids_to]
}

#' Read Monthly Partitioned Local Data
#'
#' Reads a dataset stored as monthly RDS partitions. Time bounds are applied to
#' partition discovery before files are loaded. If no partition directory exists,
#' an existing monolithic RDS file is read for backward compatibility.
#'
#' @param local_file_path Virtual monolithic `.rds` path used to derive the
#'   sibling `.parts` directory and metadata sidecar.
#' @param time_col Name of the partition timestamp column.
#' @param from,to Optional inclusive time bounds.
#' @param order_cols Optional output ordering columns.
#' @param tz Time zone used to determine month boundaries.
#'
#' @return A `data.table`, or `NULL` when no local data exists.
#' @export
get_local_data_partitioned <- function(local_file_path, time_col,
                                       from = NULL, to = NULL,
                                       order_cols = time_col, tz = "UTC") {
  all_paths <- .partition_file_paths(local_file_path)
  paths <- .filter_partition_paths(all_paths, from = from, to = to, tz = tz)
  if (length(paths) == 0L) {
    if (length(all_paths) > 0L) {
      dt <- data.table::as.data.table(readRDS(all_paths[[1L]]))[0]
    } else {
      dt <- .as_data_table(.safe_read_rds(local_file_path, default = NULL))
    }
  } else {
    dt <- data.table::rbindlist(lapply(paths, readRDS), use.names = TRUE, fill = TRUE)
  }
  if (is.null(dt)) return(NULL)
  if (!time_col %in% names(dt)) stop("Partitioned data is missing time_col: ", time_col, call. = FALSE)
  if (!is.null(from)) dt <- dt[get(time_col) >= as.POSIXct(from, tz = tz)]
  if (!is.null(to)) dt <- dt[get(time_col) <= as.POSIXct(to, tz = tz)]
  if (length(order_cols) > 0L && all(order_cols %in% names(dt))) data.table::setorderv(dt, order_cols)
  dt[]
}

.write_monthly_partitions <- function(dt, local_file_path, time_col, key_cols,
                                      order_cols, tz = "UTC") {
  if (is.null(dt) || nrow(dt) == 0L) return(integer())
  partition_dir <- .partition_data_dir(local_file_path)
  dir.create(partition_dir, recursive = TRUE, showWarnings = FALSE)
  dt <- data.table::copy(dt)
  data.table::set(dt, j = "partition_id__", value = .monthly_partition_id(dt[[time_col]], tz = tz))
  ids <- sort(unique(dt[["partition_id__"]]))
  rows <- stats::setNames(integer(length(ids)), ids)
  for (id in ids) {
    part <- dt[dt[["partition_id__"]] == id]
    data.table::set(part, j = "partition_id__", value = NULL)
    path <- file.path(partition_dir, paste0(id, ".rds"))
    old <- .as_data_table(.safe_read_rds(path, default = NULL))
    merged <- .upsert_rows_by_key(old, part, key_cols = key_cols, order_cols = order_cols)
    .safe_save_rds(merged, path)
    rows[[id]] <- nrow(merged)
  }
  rows
}

#' Synchronize Monthly Partitioned Local Data
#'
#' Upserts only monthly partitions touched by `new_data`. On the first
#' partitioned sync, an existing monolithic RDS cache is copied into monthly
#' partitions so callers can opt in without a separate migration step.
#'
#' @inheritParams sync_local_data
#' @param time_col Name of the timestamp column used for monthly partitioning.
#' @param tz Time zone used to determine month boundaries.
#'
#' @return A sync result list. Its `data` element contains the touched rows,
#'   rather than the complete potentially large dataset.
#' @export
sync_local_data_partitioned <- function(new_data, local_file_path, time_col,
                                        key_cols, order_cols = key_cols,
                                        source_utime = NULL,
                                        local_updated_at = Sys.time(), tz = "UTC") {
  new_dt <- .as_data_table(new_data)
  if (is.null(new_dt)) {
    return(list(updated = FALSE, reason = "new_data_is_null", data = NULL, file_path = local_file_path))
  }
  stopifnot(all(c(time_col, key_cols) %in% names(new_dt)))
  new_dt <- unique(new_dt, by = key_cols)
  if (nrow(new_dt) > 0L && any(is.na(new_dt[[time_col]]))) {
    stop("Partition timestamp column contains missing values: ", time_col, call. = FALSE)
  }

  partition_paths <- .partition_file_paths(local_file_path)
  if (length(partition_paths) == 0L && file.exists(local_file_path)) {
    legacy <- .as_data_table(readRDS(local_file_path))
    .write_monthly_partitions(
      legacy, local_file_path, time_col = time_col, key_cols = key_cols,
      order_cols = order_cols, tz = tz
    )
  }

  old_touched <- if (nrow(new_dt) == 0L) {
    new_dt[0]
  } else {
    ids <- unique(.monthly_partition_id(new_dt[[time_col]], tz = tz))
    paths <- file.path(.partition_data_dir(local_file_path), paste0(ids, ".rds"))
    paths <- paths[file.exists(paths)]
    if (length(paths) == 0L) new_dt[0] else data.table::rbindlist(lapply(paths, readRDS), use.names = TRUE, fill = TRUE)
  }
  new_rows <- .find_new_rows(new_dt, key_cols = key_cols, old_dt = old_touched)
  changed_existing <- .has_changed_rows(old_touched, new_dt, key_cols = key_cols, order_cols = order_cols)
  updated <- nrow(new_rows) > 0L || changed_existing

  partition_rows <- integer()
  if (nrow(new_dt) > 0L && updated) {
    partition_rows <- .write_monthly_partitions(
      new_dt, local_file_path, time_col = time_col, key_cols = key_cols,
      order_cols = order_cols, tz = tz
    )
  }
  meta_path <- .meta_file_path(local_file_path)
  old_meta <- .safe_read_rds(meta_path, default = list())
  known_rows <- old_meta$partition_rows %||% integer()
  if (length(partition_rows) > 0L) known_rows[names(partition_rows)] <- partition_rows
  missing_ids <- setdiff(
    sub("\\.rds$", "", basename(.partition_file_paths(local_file_path))),
    names(known_rows)
  )
  if (length(missing_ids) > 0L) {
    missing_paths <- file.path(.partition_data_dir(local_file_path), paste0(missing_ids, ".rds"))
    known_rows[missing_ids] <- vapply(missing_paths, function(path) nrow(readRDS(path)), integer(1))
  }
  meta <- list(
    storage = "monthly", partition = "month", partition_time_col = time_col,
    partition_rows = known_rows,
    local_updated_at = as.POSIXct(local_updated_at, tz = "UTC"),
    source_updated_at = if (is.null(source_utime)) NULL else as.POSIXct(source_utime, tz = "UTC"),
    n_rows = sum(known_rows), key_cols = key_cols
  )
  .safe_save_rds(meta, meta_path)

  list(
    updated = updated, n_new_rows = nrow(new_rows), n_rows = meta$n_rows,
    data = new_dt, file_path = local_file_path, partition_path = .partition_data_dir(local_file_path),
    meta_path = meta_path
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
