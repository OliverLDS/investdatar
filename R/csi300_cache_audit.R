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
