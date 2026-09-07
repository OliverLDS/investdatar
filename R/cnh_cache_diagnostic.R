.cnh_diagnostic_schema_version <- "1.0.0"

.cnh_diagnostic_paths <- function(output_dir, generated_at = Sys.time()) {
  stamp <- format(as.POSIXct(generated_at, tz = "UTC"), "%Y%m%dT%H%M%SZ", tz = "UTC")
  stem <- paste0("cnh_cache_diagnostic_v", gsub("[^A-Za-z0-9]+", "_", .cnh_diagnostic_schema_version), "_", stamp)
  list(
    json = file.path(output_dir, paste0(stem, ".json")),
    csv = file.path(output_dir, paste0(stem, ".csv"))
  )
}

.cnh_diagnostic_timezone <- function(x, default = NA_character_) {
  value <- attr(x, "tzone")
  if (is.null(value) || length(value) == 0L || !nzchar(value[[1L]])) default else value[[1L]]
}

.cnh_diagnostic_empty_differences <- function() {
  data.table::data.table(
    date = as.Date(character()), field = character(), cached_value = numeric(),
    fresh_eastmoney_value = numeric(), absolute_difference = numeric(),
    relative_difference = numeric(), tolerance_absolute = numeric(),
    tolerance_relative = numeric(), cached_source = character(), fresh_source = character(),
    cached_datetime_utc = character(), fresh_datetime_utc = character(),
    cached_label = character(), fresh_trading_date_label = character(),
    cached_timezone = character(), fresh_timezone = character(), classification = character()
  )
}

.cnh_diagnostic_classify <- function(cached_source, cached_datetime, fresh_datetime, date) {
  cached_label <- as.Date(cached_datetime, tz = "UTC")
  fresh_label <- as.Date(fresh_datetime, tz = "UTC")
  if (!is.na(cached_label) && !is.na(fresh_label) &&
      (!identical(cached_label, date) || !identical(fresh_label, date))) {
    return("likely_session_label")
  }
  if (identical(cached_source, "eastmoney")) return("likely_provider_revision")
  "unexplained"
}

#' Diagnose CNH/USD Cache Differences
#'
#' Compares the read-only local `CNH=X` cache with a fresh TLS-verified
#' Eastmoney `133.USDCNH` pull. The resulting JSON and CSV artifacts contain
#' field-level differences, source timestamps, trading-date labels, provenance,
#' tolerance calculations, and conservative classifications. This function
#' never modifies cache or synchronization files and does not create repair
#' candidates.
#'
#' The Yahoo and Eastmoney identifiers are both interpreted as USD/CNH, meaning
#' CNH units per one USD. Yahoo's `CNH=X` is retained as the provider symbol;
#' Eastmoney's `133.USDCNH` is the corresponding provider symbol. Eastmoney
#' daily labels are reported with the provider's Asia/Shanghai market-date
#' convention; its HTTP response does not expose a bar timezone explicitly.
#'
#' @param local_path Yahoo Finance cache directory. Defaults to the configured
#'   Yahoo Finance data path.
#' @param output_dir Directory for diagnostic artifacts. Defaults to
#'   `<local_path>/_audits/cnh_cache_diagnostic`.
#' @param from Optional inclusive date. Defaults to the recent cache overlap.
#' @param to Optional inclusive date. Defaults to the recent cache overlap.
#' @param overlap_days Number of calendar days used when `from` and `to` are
#'   both omitted.
#'
#' @return A list with `summary`, field-level `differences`, and artifact paths.
#' @export
diagnose_cnh_cache_integrity <- function(local_path = NULL, output_dir = NULL,
                                         from = NULL, to = NULL, overlap_days = 35L) {
  overlap_days <- as.integer(overlap_days)
  if (length(overlap_days) != 1L || is.na(overlap_days) || overlap_days < 1L) {
    stop("overlap_days must be a positive integer.", call. = FALSE)
  }
  if (is.null(local_path)) local_path <- .quantmod_default_local_path(src = "yahoo", create = FALSE)
  if (is.null(output_dir)) output_dir <- file.path(local_path, "_audits", "cnh_cache_diagnostic")

  cached <- get_local_quantmod_OHLC("CNH=X", src = "yahoo", interval = "1d", local_path = local_path)
  if (is.null(cached) || nrow(cached) == 0L) stop("No local CNH=X cache exists at: ", local_path, call. = FALSE)
  cached <- data.table::copy(.as_data_table(cached))
  cached[, date := as.Date(date)]
  cache_max <- max(cached$date, na.rm = TRUE)
  if (is.null(to)) to <- cache_max
  if (is.null(from)) from <- as.Date(to) - overlap_days + 1L
  from <- as.Date(from)
  to <- as.Date(to)
  if (is.na(from) || is.na(to) || from > to) stop("from and to must be valid ordered dates.", call. = FALSE)
  cached <- cached[date >= from & date <= to]
  if (nrow(cached) == 0L) stop("No local CNH=X rows remain in the requested diagnostic window.", call. = FALSE)

  fresh <- tryCatch(
    .fetch_eastmoney_ohlc("133.USDCNH", label = "CNH=X", from = from, to = to),
    error = function(e) e
  )
  if (inherits(fresh, "error")) {
    generated_at <- as.POSIXct(Sys.time(), tz = "UTC")
    paths <- .cnh_diagnostic_paths(output_dir, generated_at)
    dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
    differences <- .cnh_diagnostic_empty_differences()
    data.table::fwrite(differences, paths$csv)
    report <- list(
      schema_version = .cnh_diagnostic_schema_version,
      diagnostic_id = tools::file_path_sans_ext(basename(paths$json)),
      generated_at_utc = format(generated_at, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      symbol = "CNH=X", comparison_source = list(provider = "eastmoney", symbol = "133.USDCNH"),
      status = "provider_unavailable", comparison_error = conditionMessage(fresh),
      requested_coverage = list(from = as.character(from), to = as.character(to)),
      direction_units = list(yahoo = "USD/CNH", eastmoney = "USD/CNH", same_direction = TRUE,
                             base_currency = "USD", quote_currency = "CNH", units = "CNH per USD"),
      difference_rows = 0L, differences = data.frame()
    )
    jsonlite::write_json(report, paths$json, pretty = TRUE, auto_unbox = TRUE, null = "null", na = "null")
    return(list(summary = report[setdiff(names(report), "differences")], differences = differences, artifact_paths = paths))
  }
  fresh <- data.table::copy(.as_data_table(fresh))
  fresh[, date := as.Date(date)]
  profile <- .yahoo_overlap_tolerance("foreign_exchange", "spot_fx")
  common <- merge(
    cached[, .(date, cached_source = source, cached_datetime = datetime,
               cached_open = open, cached_high = high, cached_low = low,
               cached_close = close, cached_volume = volume)],
    fresh[, .(date, fresh_source = source, fresh_datetime = datetime,
              fresh_open = open, fresh_high = high, fresh_low = low,
              fresh_close = close, fresh_volume = volume)],
    by = "date", all = FALSE, sort = TRUE
  )
  difference_rows <- list()
  for (i in seq_len(nrow(common))) {
    for (field in c("open", "high", "low", "close")) {
      cached_value <- common[[paste0("cached_", field)]][[i]]
      fresh_value <- common[[paste0("fresh_", field)]][[i]]
      difference <- abs(cached_value - fresh_value)
      threshold <- max(profile$absolute, profile$relative * max(abs(cached_value), abs(fresh_value), 1e-12))
      if (is.finite(difference) && difference > threshold) {
        difference_rows[[length(difference_rows) + 1L]] <- data.table::data.table(
          date = common$date[[i]], field = field,
          cached_value = cached_value, fresh_eastmoney_value = fresh_value,
          absolute_difference = difference,
          relative_difference = difference / max(abs(cached_value), abs(fresh_value), 1e-12),
          tolerance_absolute = profile$absolute, tolerance_relative = profile$relative,
          cached_source = common$cached_source[[i]], fresh_source = common$fresh_source[[i]],
          cached_datetime_utc = format(common$cached_datetime[[i]], "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
          fresh_datetime_utc = format(common$fresh_datetime[[i]], "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
          cached_label = as.character(common$date[[i]]), fresh_trading_date_label = as.character(common$date[[i]]),
          cached_timezone = .cnh_diagnostic_timezone(common$cached_datetime[[i]], "UTC"),
          fresh_timezone = "Asia/Shanghai",
          classification = .cnh_diagnostic_classify(
            common$cached_source[[i]], common$cached_datetime[[i]], common$fresh_datetime[[i]], common$date[[i]]
          )
        )
      }
    }
  }
  differences <- if (length(difference_rows)) data.table::rbindlist(difference_rows) else .cnh_diagnostic_empty_differences()
  generated_at <- as.POSIXct(Sys.time(), tz = "UTC")
  paths <- .cnh_diagnostic_paths(output_dir, generated_at)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(differences, paths$csv)
  source_counts <- differences[, .N, by = .(cached_source, classification)]
  report <- list(
    schema_version = .cnh_diagnostic_schema_version,
    diagnostic_id = tools::file_path_sans_ext(basename(paths$json)),
    generated_at_utc = format(generated_at, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    symbol = "CNH=X",
    cache_path = normalizePath(local_path, winslash = "/", mustWork = FALSE),
    requested_coverage = list(from = as.character(from), to = as.character(to)),
    comparison_source = list(provider = "eastmoney", symbol = "133.USDCNH"),
    direction_units = list(
      yahoo = "USD/CNH", eastmoney = "USD/CNH", same_direction = TRUE,
      base_currency = "USD", quote_currency = "CNH", units = "CNH per USD"
    ),
    source_time_semantics = list(
      yahoo = list(identifier = "CNH=X", timezone = "cache row timezone or UTC", label = "Yahoo provider date"),
      eastmoney = list(identifier = "133.USDCNH", timezone = "Asia/Shanghai", label = "Eastmoney trading-date label")
    ),
    cached_rows = nrow(cached), eastmoney_rows = nrow(fresh),
    common_date_rows = nrow(common), difference_rows = nrow(differences),
    differences_by_cached_source = source_counts,
    classification_counts = differences[, .N, by = classification],
    differences = as.data.frame(differences)
  )
  jsonlite::write_json(report, paths$json, pretty = TRUE, auto_unbox = TRUE, null = "null", na = "null")
  list(summary = report[setdiff(names(report), "differences")], differences = differences, artifact_paths = paths)
}
