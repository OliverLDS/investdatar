.csi300_cache_audit_schema_version <- "1.0.0"

.csi300_cache_audit_paths <- function(output_dir, generated_at = Sys.time()) {
  stamp <- format(as.POSIXct(generated_at, tz = "UTC"), "%Y%m%dT%H%M%SZ", tz = "UTC")
  stem <- sprintf("csi300_cache_integrity_v%s_%s",
                  gsub("[^A-Za-z0-9]+", "_", .csi300_cache_audit_schema_version), stamp)
  list(
    json = file.path(output_dir, paste0(stem, ".json")),
    csv = file.path(output_dir, paste0(stem, ".csv"))
  )
}

.csi300_audit_date_range <- function(dt) {
  if (is.null(dt) || nrow(dt) == 0L || !"date" %in% names(dt)) {
    return(list(from = NA_character_, to = NA_character_))
  }
  list(from = as.character(min(dt$date)), to = as.character(max(dt$date)))
}

.csi300_audit_discrepancies <- function(cached, fresh, tolerance) {
  cached_cols <- c("date", "source", "symbol", "interval", "datetime", "open", "high", "low", "close", "volume", "adj_close")
  fresh_cols <- c("date", "source", "symbol", "interval", "datetime", "open", "high", "low", "close", "volume", "adj_close")
  for (nm in setdiff(cached_cols, names(cached))) cached[, (nm) := NA]
  for (nm in setdiff(fresh_cols, names(fresh))) fresh[, (nm) := NA]

  cached <- data.table::copy(cached[, cached_cols, with = FALSE])
  fresh <- data.table::copy(fresh[, fresh_cols, with = FALSE])
  data.table::setnames(cached, setdiff(names(cached), "date"), paste0("cached_", setdiff(names(cached), "date")))
  data.table::setnames(fresh, setdiff(names(fresh), "date"), paste0("eastmoney_", setdiff(names(fresh), "date")))

  common <- merge(cached, fresh, by = "date", all = FALSE, sort = TRUE)
  # Eastmoney daily index quotes are expressed to two decimal places. Normalize
  # comparison values to that published precision before applying tolerance.
  common[, `:=`(
    raw_close_difference = cached_close - eastmoney_close,
    cached_close_cents = as.integer(round(cached_close * 100)),
    eastmoney_close_cents = as.integer(round(eastmoney_close * 100))
  )]
  common[, `:=`(
    close_difference = (cached_close_cents - eastmoney_close_cents) / 100,
    close_abs_difference = abs(cached_close_cents - eastmoney_close_cents) / 100
  )]
  discrepancies <- common[
    is.finite(close_abs_difference) &
      abs(cached_close_cents - eastmoney_close_cents) > tolerance * 100 + sqrt(.Machine$double.eps)
  ]
  discrepancies[, c("cached_close_cents", "eastmoney_close_cents") := NULL]
  discrepancies
}

#' Audit CSI 300 Cache Integrity Against Eastmoney
#'
#' Read the local Yahoo Finance cache for `000300.SS`, compare its common daily
#' coverage with a fresh Eastmoney request, and write versioned JSON and CSV
#' audit artifacts. This function is read-only with respect to cached OHLC and
#' metadata files: it does not synchronize, repair, or overwrite market data.
#'
#' `baostock_corroboration` is optional external evidence recorded verbatim in
#' the JSON report. It never triggers a BaoStock request and does not add a
#' BaoStock runtime dependency.
#'
#' @param local_path Yahoo Finance cache directory. Defaults to the configured
#'   Yahoo Finance data path.
#' @param output_dir Directory for audit artifacts. Defaults to
#'   `<local_path>/_audits/csi300_cache_integrity`.
#' @param from Optional inclusive start date for cached rows under review.
#' @param to Optional inclusive end date for cached rows under review.
#' @param close_tolerance Non-negative absolute close-price tolerance in index
#'   points after both closes are normalized to Eastmoney's published two-decimal
#'   quote precision. Rows strictly above this value are discrepancies.
#' @param baostock_corroboration Optional named list of independently obtained
#'   BaoStock comparison evidence. It is recorded only as corroboration.
#'
#' @return A list containing an audit summary, discrepancy `data.table`, and
#'   JSON and CSV artifact paths.
#' @export
audit_csi300_cache_integrity <- function(local_path = NULL,
                                         output_dir = NULL,
                                         from = NULL,
                                         to = NULL,
                                         close_tolerance = 0.01,
                                         baostock_corroboration = NULL) {
  close_tolerance <- as.numeric(close_tolerance)
  if (length(close_tolerance) != 1L || is.na(close_tolerance) || close_tolerance < 0) {
    stop("close_tolerance must be one non-negative number.", call. = FALSE)
  }
  if (is.null(local_path)) {
    local_path <- .quantmod_default_local_path(src = "yahoo", create = FALSE)
  }
  if (is.null(output_dir)) {
    output_dir <- file.path(local_path, "_audits", "csi300_cache_integrity")
  }

  cached <- get_local_quantmod_OHLC("000300.SS", src = "yahoo", interval = "1d", local_path = local_path)
  if (is.null(cached) || nrow(cached) == 0L) {
    stop("No local CSI 300 cache exists for 000300.SS at: ", local_path, call. = FALSE)
  }
  cached <- data.table::copy(.as_data_table(cached))
  if (!"date" %in% names(cached)) {
    stop("Local CSI 300 cache has no date column.", call. = FALSE)
  }
  cached[, date := as.Date(date)]
  if (!is.null(from)) cached <- cached[date >= as.Date(from)]
  if (!is.null(to)) cached <- cached[date <= as.Date(to)]
  if (nrow(cached) == 0L) {
    stop("No local CSI 300 rows remain in the requested audit window.", call. = FALSE)
  }

  requested_range <- .csi300_audit_date_range(cached)
  fresh <- .fetch_eastmoney_ohlc(
    ticker = "1.000300", label = "000300.SS",
    from = requested_range$from, to = requested_range$to
  )
  fresh <- data.table::copy(.as_data_table(fresh))
  fresh[, date := as.Date(date)]
  discrepancies <- .csi300_audit_discrepancies(cached, fresh, close_tolerance)

  cached_dates <- unique(cached$date)
  fresh_dates <- unique(fresh$date)
  common_dates <- intersect(cached_dates, fresh_dates)
  generated_at <- as.POSIXct(Sys.time(), tz = "UTC")
  paths <- .csi300_cache_audit_paths(output_dir, generated_at)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(discrepancies, paths$csv)

  if (is.null(baostock_corroboration)) {
    baostock_corroboration <- list(
      queried = FALSE,
      note = "BaoStock is not queried by this audit and is not a runtime fallback dependency."
    )
  }
  report <- list(
    schema_version = .csi300_cache_audit_schema_version,
    audit_id = tools::file_path_sans_ext(basename(paths$json)),
    generated_at_utc = format(generated_at, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    symbol = "000300.SS",
    comparison_source = list(provider = "eastmoney", symbol = "1.000300"),
    cache_path = normalizePath(local_path, winslash = "/", mustWork = FALSE),
    close_tolerance = close_tolerance,
    requested_coverage = requested_range,
    cached_rows = nrow(cached),
    eastmoney_rows = nrow(fresh),
    common_date_rows = length(common_dates),
    cached_only_date_rows = length(setdiff(cached_dates, fresh_dates)),
    eastmoney_only_date_rows = length(setdiff(fresh_dates, cached_dates)),
    cached_finite_close_rows = sum(is.finite(cached$close)),
    eastmoney_finite_close_rows = sum(is.finite(fresh$close)),
    discrepancy_rows = nrow(discrepancies),
    baostock_corroboration = baostock_corroboration,
    discrepancies = as.data.frame(discrepancies)
  )
  jsonlite::write_json(report, paths$json, pretty = TRUE, auto_unbox = TRUE, null = "null", na = "null")

  list(
    summary = report[names(report) != "discrepancies"],
    discrepancies = discrepancies,
    artifact_paths = paths
  )
}

.csi300_cache_repair_schema_version <- "1.0.0"

.csi300_cache_repair_paths <- function(output_dir, generated_at = Sys.time()) {
  stamp <- format(as.POSIXct(generated_at, tz = "UTC"), "%Y%m%dT%H%M%SZ", tz = "UTC")
  stem <- sprintf("csi300_cache_repair_v%s_%s",
                  gsub("[^A-Za-z0-9]+", "_", .csi300_cache_repair_schema_version), stamp)
  list(
    log = file.path(output_dir, paste0(stem, ".json")),
    backup = file.path(output_dir, "backups", paste0(stem, "__000300.SS__yahoo__1d.rds"))
  )
}

.csi300_close_cents <- function(x) {
  as.integer(round(as.numeric(x) * 100))
}

.csi300_read_audit_artifact <- function(path) {
  if (!file.exists(path)) {
    stop("CSI 300 audit artifact does not exist: ", path, call. = FALSE)
  }
  report <- jsonlite::fromJSON(path, simplifyDataFrame = TRUE)
  if (!identical(report$schema_version, .csi300_cache_audit_schema_version) ||
      !identical(report$symbol, "000300.SS") ||
      is.null(report$comparison_source$provider) ||
      !identical(report$comparison_source$provider, "eastmoney")) {
    stop("Audit artifact is not a supported CSI 300 Eastmoney audit report.", call. = FALSE)
  }
  discrepancies <- .as_data_table(report$discrepancies)
  required <- c(
    "date", "cached_source", "cached_open", "cached_high", "cached_low", "cached_close",
    "cached_volume", "cached_adj_close", "eastmoney_open", "eastmoney_high", "eastmoney_low",
    "eastmoney_close", "eastmoney_volume", "eastmoney_adj_close"
  )
  if (is.null(discrepancies) || !all(required %in% names(discrepancies))) {
    stop("Audit artifact has no usable CSI 300 discrepancy records.", call. = FALSE)
  }
  discrepancies[, date := as.Date(date)]
  if (anyNA(discrepancies$date) || anyDuplicated(discrepancies$date)) {
    stop("Audit artifact discrepancy dates must be unique and valid.", call. = FALSE)
  }
  list(report = report, discrepancies = discrepancies)
}

.csi300_validate_approved_dates <- function(approved_dates, discrepancies) {
  dates <- as.Date(approved_dates)
  if (length(dates) == 0L || anyNA(dates) || anyDuplicated(dates)) {
    stop("approved_dates must contain one or more unique valid dates.", call. = FALSE)
  }
  unauthorized <- setdiff(dates, discrepancies$date)
  if (length(unauthorized) > 0L) {
    stop(
      "approved_dates contains date(s) absent from the audit discrepancy set: ",
      paste(as.character(unauthorized), collapse = ", "),
      call. = FALSE
    )
  }
  sort(dates)
}

.csi300_validate_cached_audit_rows <- function(current, audited, approved_dates) {
  current <- data.table::copy(current[current[["date"]] %in% approved_dates])
  if (nrow(current) != length(approved_dates) || anyDuplicated(current$date)) {
    stop("Current CSI 300 cache no longer has exactly one row for every approved date.", call. = FALSE)
  }
  merged <- merge(current, audited, by = "date", all.x = TRUE, sort = TRUE)
  if (any(is.na(merged$cached_close)) ||
      any(.csi300_close_cents(merged$close) != .csi300_close_cents(merged$cached_close))) {
    stop("Current CSI 300 cache no longer matches the approved audit rows; run a new audit.", call. = FALSE)
  }
  merged
}

.csi300_build_repair_rows <- function(current_audited, fresh, close_tolerance) {
  fresh <- data.table::copy(fresh)
  fresh[, "date" := as.Date(fresh[["date"]])]
  merged <- merge(current_audited, fresh, by = "date", all.x = TRUE, suffixes = c("_current", "_fresh"), sort = TRUE)
  required_fresh <- c("open_fresh", "high_fresh", "low_fresh", "close_fresh")
  has_complete_fresh <- all(required_fresh %in% names(merged)) && all(vapply(
    required_fresh, function(nm) all(is.finite(merged[[nm]])), logical(1)
  ))
  if (!has_complete_fresh) {
    stop("Fresh Eastmoney candidate is missing complete OHLC for one or more approved dates.", call. = FALSE)
  }
  tolerance_cents <- close_tolerance * 100 + sqrt(.Machine$double.eps)
  if (any(abs(.csi300_close_cents(merged$close_fresh) - .csi300_close_cents(merged$eastmoney_close)) > tolerance_cents)) {
    stop("Fresh Eastmoney candidate no longer matches the audit candidate within its close tolerance.", call. = FALSE)
  }
  merged[, c(
    "old_source", "old_symbol", "old_interval", "old_datetime", "old_open", "old_high", "old_low",
    "old_close", "old_volume", "old_adj_close", "new_source", "new_symbol", "new_interval", "new_datetime",
    "new_open", "new_high", "new_low", "new_close", "new_volume", "new_adj_close", "audit_eastmoney_close"
  ) := list(
    merged[["source_current"]], merged[["symbol_current"]], merged[["interval_current"]],
    merged[["datetime_current"]], merged[["open_current"]], merged[["high_current"]], merged[["low_current"]],
    merged[["close_current"]], merged[["volume_current"]], merged[["adj_close_current"]], "eastmoney",
    merged[["symbol_fresh"]], merged[["interval_fresh"]], merged[["datetime_fresh"]],
    merged[["open_fresh"]], merged[["high_fresh"]], merged[["low_fresh"]], merged[["close_fresh"]],
    merged[["volume_fresh"]], merged[["adj_close_fresh"]], merged[["eastmoney_close"]]
  )]
  merged[, .(
    date, old_source, old_symbol, old_interval, old_datetime, old_open, old_high, old_low,
    old_close, old_volume, old_adj_close, new_source, new_symbol, new_interval, new_datetime,
    new_open, new_high, new_low, new_close, new_volume, new_adj_close, audit_eastmoney_close
  )]
}

.csi300_replace_approved_rows <- function(current, repair_rows) {
  updated <- data.table::copy(current)
  for (i in seq_len(nrow(repair_rows))) {
    row <- repair_rows[i]
    updated[updated[["date"]] == row$date,
            c("source", "symbol", "interval", "datetime", "open", "high", "low", "close", "volume", "adj_close") := list(
              row$new_source, row$new_symbol, row$new_interval, row$new_datetime,
              row$new_open, row$new_high, row$new_low, row$new_close, row$new_volume, row$new_adj_close
            )]
  }
  updated
}

#' Repair Approved CSI 300 Cache Rows From Eastmoney
#'
#' Apply a manually approved repair derived from a versioned
#' [audit_csi300_cache_integrity()] artifact. The function authorizes only
#' dates present in that artifact's discrepancy set, verifies that the current
#' cache has not changed since the audit, and verifies the fresh Eastmoney
#' candidate against the audited candidate using the audit's close tolerance.
#'
#' The default is a dry run. It always writes a versioned repair log; dry runs
#' do not mutate the cache or create a backup. A non-dry-run first copies the
#' cache to the repair directory, then replaces only the explicitly approved
#' rows. If writing fails, it restores the cache from that backup.
#'
#' @param audit_artifact_path JSON report produced by
#'   [audit_csi300_cache_integrity()].
#' @param approved_dates Explicit unique dates to repair. Every date must be in
#'   the audit artifact's discrepancy set.
#' @param local_path Yahoo Finance cache directory. Defaults to the configured
#'   Yahoo Finance data path.
#' @param output_dir Directory for repair logs and backups. Defaults to
#'   `<local_path>/_repairs/csi300_cache_integrity`.
#' @param dry_run Whether to write only a proposed repair log. Defaults to
#'   `TRUE`.
#'
#' @return A list with the proposed or applied repair rows, log path, and (when
#'   applied) backup path.
#' @export
repair_csi300_cache_integrity <- function(audit_artifact_path,
                                          approved_dates,
                                          local_path = NULL,
                                          output_dir = NULL,
                                          dry_run = TRUE) {
  audit <- .csi300_read_audit_artifact(audit_artifact_path)
  approved_dates <- .csi300_validate_approved_dates(approved_dates, audit$discrepancies)
  close_tolerance <- as.numeric(audit$report$close_tolerance)
  if (length(close_tolerance) != 1L || is.na(close_tolerance) || close_tolerance < 0) {
    stop("Audit artifact has an invalid close tolerance.", call. = FALSE)
  }
  if (is.null(local_path)) local_path <- .quantmod_default_local_path(src = "yahoo", create = FALSE)
  if (is.null(output_dir)) output_dir <- file.path(local_path, "_repairs", "csi300_cache_integrity")
  local_file_path <- file.path(local_path, .quantmod_local_filename("000300.SS", "yahoo", "1d"))
  current <- .as_data_table(.safe_read_rds(local_file_path, default = NULL))
  if (is.null(current) || nrow(current) == 0L) {
    stop("No local CSI 300 cache exists for 000300.SS at: ", local_path, call. = FALSE)
  }
  current <- data.table::copy(current)
  current[, "date" := as.Date(current[["date"]])]
  audited <- audit$discrepancies[audit$discrepancies[["date"]] %in% approved_dates]
  current_audited <- .csi300_validate_cached_audit_rows(current, audited, approved_dates)
  fresh <- .fetch_eastmoney_ohlc(
    ticker = "1.000300", label = "000300.SS",
    from = min(approved_dates), to = max(approved_dates)
  )
  repair_rows <- .csi300_build_repair_rows(current_audited, fresh, close_tolerance)

  generated_at <- as.POSIXct(Sys.time(), tz = "UTC")
  paths <- .csi300_cache_repair_paths(output_dir, generated_at)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  status <- if (isTRUE(dry_run)) "dry_run" else "applied"
  backup_path <- NULL
  if (!isTRUE(dry_run)) {
    dir.create(dirname(paths$backup), recursive = TRUE, showWarnings = FALSE)
    if (!file.copy(local_file_path, paths$backup, overwrite = FALSE)) {
      stop("Could not back up CSI 300 cache before repair: ", paths$backup, call. = FALSE)
    }
    backup_path <- paths$backup
    updated <- .csi300_replace_approved_rows(current, repair_rows)
    tryCatch(
      .safe_save_rds(updated, local_file_path),
      error = function(e) {
        restored <- file.copy(paths$backup, local_file_path, overwrite = TRUE)
        if (!restored) {
          stop("CSI 300 repair write failed and backup restoration also failed: ", conditionMessage(e), call. = FALSE)
        }
        stop("CSI 300 repair write failed; cache was restored from backup: ", conditionMessage(e), call. = FALSE)
      }
    )
  }
  repair_log <- list(
    schema_version = .csi300_cache_repair_schema_version,
    repair_id = tools::file_path_sans_ext(basename(paths$log)),
    status = status,
    dry_run = isTRUE(dry_run),
    generated_at_utc = format(generated_at, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    package_version = as.character(utils::packageVersion("investdatar")),
    audit_id = audit$report$audit_id,
    audit_artifact_path = normalizePath(audit_artifact_path, winslash = "/", mustWork = FALSE),
    cache_path = normalizePath(local_file_path, winslash = "/", mustWork = FALSE),
    backup_path = backup_path,
    source_provenance = list(provider = "eastmoney", symbol = "1.000300"),
    close_tolerance = close_tolerance,
    repairs = as.data.frame(repair_rows)
  )
  jsonlite::write_json(repair_log, paths$log, pretty = TRUE, auto_unbox = TRUE, null = "null", na = "null")
  list(
    dry_run = isTRUE(dry_run),
    updated = !isTRUE(dry_run),
    repair_rows = repair_rows,
    repair_log_path = paths$log,
    backup_path = backup_path
  )
}
