.yahoo_overlap_audit_schema_version <- "1.0.0"

.yahoo_overlap_audit_paths <- function(output_dir, generated_at = Sys.time()) {
  stamp <- format(as.POSIXct(generated_at, tz = "UTC"), "%Y%m%dT%H%M%SZ", tz = "UTC")
  stem <- sprintf("yahoo_recent_overlap_v%s_%s",
                  gsub("[^A-Za-z0-9]+", "_", .yahoo_overlap_audit_schema_version), stamp)
  list(
    json = file.path(output_dir, paste0(stem, ".json")),
    csv = file.path(output_dir, paste0(stem, ".csv"))
  )
}

.yahoo_overlap_calendar_today <- function(calendar, as_of) {
  tz <- switch(
    calendar,
    XNYS = "America/New_York",
    US_EQUITY = "America/New_York",
    US_FUTURES = "America/New_York",
    CME_FUTURES = "America/Chicago",
    ICE_FUTURES = "America/New_York",
    XSHG = "Asia/Shanghai",
    FX_24_5 = "UTC",
    CRYPTO_24_7 = "UTC",
    "UTC"
  )
  as.Date(as.POSIXct(as_of, tz = "UTC"), tz = tz)
}

.yahoo_overlap_known_holidays <- function(calendar, years) {
  if (!identical(calendar, "XNYS")) return(as.Date(character()))
  years <- sort(unique(as.integer(years)))
  holidays <- unlist(lapply(years, function(year) {
    dates <- as.Date(sprintf("%s-%s", year, c("01-01", "06-19", "07-04", "12-25")))
    dates <- dates + ifelse(weekdays(dates) == "Saturday", -1L, ifelse(weekdays(dates) == "Sunday", 1L, 0L))
    nth_weekday <- function(month, weekday, n) {
      first <- as.Date(sprintf("%s-%02d-01", year, month))
      candidates <- seq(first, by = "day", length.out = 31L)
      candidates <- candidates[format(candidates, "%m") == sprintf("%02d", month)]
      candidates[weekdays(candidates) == weekday][[n]]
    }
    last_weekday <- function(month, weekday) {
      first <- as.Date(sprintf("%s-%02d-01", year, month))
      candidates <- seq(first, by = "day", length.out = 31L)
      candidates <- candidates[format(candidates, "%m") == sprintf("%02d", month)]
      tail(candidates[weekdays(candidates) == weekday], 1L)
    }
    c(
      dates,
      nth_weekday(1L, "Monday", 3L),
      nth_weekday(2L, "Monday", 3L),
      last_weekday(5L, "Monday"),
      nth_weekday(9L, "Monday", 1L),
      nth_weekday(11L, "Thursday", 4L)
    )
  }), use.names = FALSE)
  as.Date(holidays, origin = "1970-01-01")
}

.yahoo_overlap_nontrading_dates <- function(calendar, dates) {
  dates <- as.Date(dates)
  weekend <- weekdays(dates) %in% c("Saturday", "Sunday")
  holidays <- .yahoo_overlap_known_holidays(calendar, format(dates, "%Y"))
  dates[weekend | dates %in% holidays]
}

.yahoo_overlap_tolerance <- function(asset_class, instrument_type, audit_profile = NULL) {
  if (is.list(audit_profile) && is.list(audit_profile$price_tolerance) &&
      !is.null(audit_profile$price_tolerance$absolute) &&
      !is.null(audit_profile$price_tolerance$relative)) {
    volume <- audit_profile$volume %||% list()
    return(list(
      absolute = as.numeric(audit_profile$price_tolerance$absolute),
      relative = as.numeric(audit_profile$price_tolerance$relative),
      volume_meaningful = isTRUE(volume$meaningful),
      volume_relative = as.numeric(volume$relative_tolerance %||% 0.01)
    ))
  }
  if (identical(instrument_type, "spot_fx") || identical(asset_class, "foreign_exchange")) {
    return(list(absolute = 0.0001, relative = 1e-6, volume_meaningful = FALSE))
  }
  if (identical(instrument_type, "equity_index")) {
    return(list(absolute = 0.01, relative = 1e-6, volume_meaningful = FALSE))
  }
  if (identical(asset_class, "cryptocurrency")) {
    return(list(absolute = 0.01, relative = 1e-6, volume_meaningful = TRUE))
  }
  list(absolute = 0.01, relative = 1e-6, volume_meaningful = TRUE)
}

.yahoo_overlap_issue <- function(ticker, instrument_id, date, issue_type, field = NA_character_,
                                 cached_value = NA, yahoo_value = NA, abs_difference = NA_real_,
                                 relative_difference = NA_real_, cached_source = NA_character_,
                                 yahoo_source = NA_character_, severity = "integrity",
                                 related_integrity_issue = TRUE, detail = NA_character_) {
  issue_date <- if (inherits(date, "Date")) date else as.Date(date, origin = "1970-01-01")
  data.table::data.table(
    ticker = ticker, instrument_id = instrument_id, date = issue_date,
    issue_type = issue_type, field = field, cached_value = as.character(cached_value),
    yahoo_value = as.character(yahoo_value), abs_difference = abs_difference,
    relative_difference = relative_difference, cached_source = cached_source,
    yahoo_source = yahoo_source, severity = severity,
    related_integrity_issue = related_integrity_issue, detail = detail
  )
}

.yahoo_overlap_empty_findings <- function() {
  data.table::data.table(
    ticker = character(), instrument_id = character(), date = as.Date(character()),
    issue_type = character(), field = character(), cached_value = character(), yahoo_value = character(),
    abs_difference = numeric(), relative_difference = numeric(), cached_source = character(),
    yahoo_source = character(), severity = character(), related_integrity_issue = logical(), detail = character()
  )
}

.yahoo_overlap_compare_rows <- function(ticker, instrument_id, cached, yahoo, profile, calendar, from, to,
                                        session_metadata = NULL, comparison_provider = "Yahoo") {
  all_dates <- seq(as.Date(from), as.Date(to), by = "day")
  nontrading <- .yahoo_overlap_nontrading_dates(calendar, all_dates)
  cached_dates <- unique(cached$date)
  yahoo_dates <- unique(yahoo$date)
  issues <- list()
  date_setdiff <- function(x, y) as.Date(setdiff(as.character(x), as.character(y)))
  date_intersect <- function(x, y) as.Date(intersect(as.character(x), as.character(y)))

  missing_cache <- date_setdiff(yahoo_dates, cached_dates)
  missing_cache <- date_setdiff(missing_cache, nontrading)
  if (length(missing_cache)) {
    issues[[length(issues) + 1L]] <- data.table::rbindlist(lapply(missing_cache, function(d) {
      .yahoo_overlap_issue(ticker, instrument_id, d, "missing_in_cache", detail = paste0("Completed date returned by ", comparison_provider, " is absent locally."))
    }))
  }
  missing_yahoo <- date_setdiff(cached_dates, yahoo_dates)
  missing_yahoo <- date_setdiff(missing_yahoo, nontrading)
  fx_label_gap <- identical(calendar, "FX_24_5") && is.list(session_metadata) &&
    identical(session_metadata$instrument_type, "CURRENCY") &&
    identical(session_metadata$exchange_timezone, "Europe/London") &&
    23L %in% session_metadata$utc_timestamp_hours
  if (fx_label_gap) {
    friday_gaps <- missing_yahoo[weekdays(missing_yahoo) == "Friday"]
    if (length(friday_gaps)) {
      issues[[length(issues) + 1L]] <- data.table::rbindlist(lapply(friday_gaps, function(d) {
        row <- cached[cached$date == d]
        .yahoo_overlap_issue(
          ticker, instrument_id, d, "fx_session_label_gap", "date",
          cached_value = as.character(d), yahoo_value = NA_character_,
          cached_source = row$source, severity = "informational",
          related_integrity_issue = FALSE,
          detail = paste0(
            "Yahoo CURRENCY daily epochs use a Europe/London session boundary at 23:00 UTC; ",
            "the Friday cache label is not classified as a missing completed bar."
          )
        )
      }))
      missing_yahoo <- date_setdiff(missing_yahoo, friday_gaps)
    }
  }
  if (length(missing_yahoo)) {
    issues[[length(issues) + 1L]] <- data.table::rbindlist(lapply(missing_yahoo, function(d) {
      row <- cached[cached$date == d]
      .yahoo_overlap_issue(ticker, instrument_id, d, "yahoo_missing_completed_bar",
                           cached_source = row$source, detail = paste0("Completed local date was not returned by ", comparison_provider, "."))
    }))
  }

  common_dates <- date_intersect(cached_dates, yahoo_dates)
  for (d in common_dates) {
    old <- cached[cached$date == d]
    new <- yahoo[yahoo$date == d]
    valid_old <- all(vapply(c("open", "high", "low", "close"), function(nm) is.finite(old[[nm]]), logical(1)))
    valid_new <- all(vapply(c("open", "high", "low", "close"), function(nm) is.finite(new[[nm]]), logical(1)))
    if (!valid_old) issues[[length(issues) + 1L]] <- .yahoo_overlap_issue(ticker, instrument_id, d, "invalid_cached_ohlc", cached_source = old$source)
    if (!valid_new) issues[[length(issues) + 1L]] <- .yahoo_overlap_issue(ticker, instrument_id, d, "invalid_yahoo_ohlc", yahoo_source = new$source)
    if (valid_old && valid_new) {
      for (field in c("open", "high", "low", "close")) {
        difference <- abs(old[[field]] - new[[field]])
        threshold <- max(profile$absolute, profile$relative * max(abs(old[[field]]), abs(new[[field]])))
        if (is.finite(difference) && difference > threshold) {
          issues[[length(issues) + 1L]] <- .yahoo_overlap_issue(
            ticker, instrument_id, d, "ohlc_discrepancy", field,
            old[[field]], new[[field]], difference,
            difference / max(abs(old[[field]]), abs(new[[field]]), 1e-12),
        old$source, new$source, detail = paste0("OHLC difference exceeds asset-appropriate tolerance in the ", comparison_provider, " comparison.")
          )
        }
      }
    }
    if (isTRUE(profile$volume_meaningful) && is.finite(old$volume) && is.finite(new$volume) && old$volume > 0 && new$volume > 0) {
      difference <- abs(old$volume - new$volume)
      relative <- difference / max(abs(old$volume), abs(new$volume), 1)
      if (difference >= 1 && relative > (profile$volume_relative %||% 0.01)) {
        issues[[length(issues) + 1L]] <- .yahoo_overlap_issue(
          ticker, instrument_id, d, "volume_discrepancy", "volume", old$volume, new$volume,
          difference, relative, old$source, new$source,
        detail = paste0("Volume differs by at least one unit and more than 1 percent in the ", comparison_provider, " comparison.")
        )
      }
    }
    if (!identical(as.character(old$source), as.character(new$source))) {
      issues[[length(issues) + 1L]] <- .yahoo_overlap_issue(
        ticker, instrument_id, d, "provenance_change", "source", old$source, new$source,
        cached_source = old$source, yahoo_source = new$source, severity = "informational",
        related_integrity_issue = FALSE, detail = paste0("Cached row provenance differs from the ", comparison_provider, " audit fetch.")
      )
    }
  }
  findings <- if (length(issues)) data.table::rbindlist(issues, fill = TRUE) else .yahoo_overlap_empty_findings()
  if (nrow(findings)) {
    integrity_dates <- unique(findings[issue_type != "provenance_change", date])
    findings[issue_type == "provenance_change" & date %in% integrity_dates,
             `:=`(severity = "integrity", related_integrity_issue = TRUE)]
  }
  list(findings = findings, nontrading_dates = nontrading)
}

.yahoo_fx_session_date <- function(timestamp, exchange_timezone) {
  if (!is.character(exchange_timezone) || length(exchange_timezone) != 1L || !nzchar(exchange_timezone)) {
    stop("Yahoo FX chart metadata must provide exchangeTimezoneName.", call. = FALSE)
  }
  # Investdatar's existing Yahoo cache uses the UTC calendar date of Yahoo's
  # epoch. Preserve that key convention; exchange_timezone is retained as an
  # explicit metadata guard for the FX session-label classification below.
  as.Date(as.POSIXct(as.numeric(timestamp), origin = "1970-01-01", tz = "UTC"), tz = "UTC")
}

.fetch_yahoo_fx_ohlc <- function(ticker, label, from, to, max_attempts = 3L, retry_delay_seconds = 1) {
  url <- paste0("https://query1.finance.yahoo.com/v8/finance/chart/", utils::URLencode(ticker, reserved = TRUE))
  response <- .http_request(
    "GET", url,
    query = list(
      period1 = as.numeric(as.POSIXct(as.Date(from) - 2L, tz = "UTC")),
      period2 = as.numeric(as.POSIXct(as.Date(to) + 2L, tz = "UTC")),
      interval = "1d", events = "history", includePrePost = "false",
      includeAdjustedClose = "true"
    ),
    max_attempts = max_attempts,
    retry_status = c(408L, 425L, 429L, 500L, 502L, 503L, 504L),
    timeout_seconds = 30
  )
  payload <- jsonlite::fromJSON(.http_response_text(response), simplifyVector = FALSE)
  result <- payload$chart$result[[1L]]
  metadata <- if (is.null(result)) NULL else result$meta
  if (is.null(result) || is.null(result$timestamp) || is.null(result$indicators$quote[[1L]]) ||
      is.null(metadata$instrumentType) || !identical(metadata$instrumentType, "CURRENCY") ||
      is.null(metadata$exchangeTimezoneName)) {
    stop("Yahoo FX chart response lacks required currency session metadata for ", ticker, call. = FALSE)
  }
  timestamps <- unlist(result$timestamp, use.names = FALSE)
  quote <- result$indicators$quote[[1L]]
  n <- length(timestamps)
  session_dates <- .yahoo_fx_session_date(timestamps, metadata$exchangeTimezoneName)
  dt <- data.table::data.table(
    date = session_dates,
    datetime = as.POSIXct(as.numeric(timestamps), origin = "1970-01-01", tz = "UTC"),
    open = .yahoo_chart_numeric(quote$open, n), high = .yahoo_chart_numeric(quote$high, n),
    low = .yahoo_chart_numeric(quote$low, n), close = .yahoo_chart_numeric(quote$close, n),
    volume = .yahoo_chart_numeric(quote$volume, n), adj_close = NA_real_, symbol = label
  )
  dt <- .standardize_market_ohlcv(dt, source = "quantmod_yahoo", symbol = label, interval = "1d", time_col = "datetime")
  dt[, date := session_dates]
  attr(dt, "investdatar_yahoo_session_metadata") <- list(
    instrument_type = metadata$instrumentType,
    exchange_timezone = metadata$exchangeTimezoneName,
    utc_timestamp_hours = unique(as.integer(format(as.POSIXct(as.numeric(timestamps), origin = "1970-01-01", tz = "UTC"), "%H")))
  )
  dt[date >= as.Date(from) & date <= as.Date(to)]
}

.yahoo_overlap_fetch <- function(ticker, from, to, calendar = NULL, max_attempts = 3L, retry_delay_seconds = 1) {
  if (identical(calendar, "FX_24_5")) {
    return(.fetch_yahoo_fx_ohlc(ticker, label = ticker, from = from, to = to,
                                max_attempts = max_attempts, retry_delay_seconds = retry_delay_seconds))
  }
  fetch_quantmod_OHLC(
    ticker = ticker, label = ticker, from = from, to = to, src = "yahoo",
    fallback_source = NULL, max_attempts = max_attempts,
    retry_delay_seconds = retry_delay_seconds, require_start_coverage = FALSE
  )
}

.yahoo_overlap_fallback_mapping <- function(registry, ticker) {
  if (!ticker %in% c("000300.SS", "CNH=X") ||
      !all(c("fallback_source", "fallback_ticker") %in% names(registry))) {
    return(NULL)
  }
  row <- registry[registry[["yahoo_finance_ticker"]] == ticker]
  if (nrow(row) != 1L || is.na(row$fallback_source[[1L]]) ||
      !identical(tolower(row$fallback_source[[1L]]), "eastmoney") ||
      is.na(row$fallback_ticker[[1L]]) || !nzchar(row$fallback_ticker[[1L]])) {
    return(NULL)
  }
  list(provider = "eastmoney", symbol = row$fallback_ticker[[1L]])
}

.yahoo_overlap_has_usable_ohlc <- function(x) {
  nrow(x) > 0L && all(c("open", "high", "low", "close") %in% names(x)) && any(vapply(
    seq_len(nrow(x)), function(i) all(is.finite(as.numeric(x[i, .(open, high, low, close)]))), logical(1)
  ))
}

.yahoo_overlap_handle_unavailable <- function(ticker, instrument_id, cached, profile,
                                              calendar, requested_from, requested_to,
                                              primary_error, fallback_mapping,
                                              fallback_corroboration) {
  findings <- list(.yahoo_overlap_issue(
    ticker, instrument_id, requested_to, "yahoo_request_error",
    severity = "availability", related_integrity_issue = FALSE,
    detail = primary_error
  ))
  report <- list(
    status = "audit_incomplete_provider_unavailable",
    yahoo_error = primary_error,
    fallback_attempted = FALSE,
    fallback_status = "not_requested",
    integrity_issue_rows = 0L,
    informational_rows = 0L
  )
  if (!is.null(fallback_mapping) && fallback_corroboration != "none") {
    report$fallback_attempted <- TRUE
    fallback <- tryCatch(
      .fetch_eastmoney_ohlc(fallback_mapping$symbol, label = ticker,
                            from = requested_from, to = requested_to),
      error = function(e) e
    )
    fallback_usable <- !inherits(fallback, "error") && .yahoo_overlap_has_usable_ohlc(fallback)
    if (fallback_usable) {
      compared <- .yahoo_overlap_compare_rows(
        ticker, instrument_id, cached, fallback, profile, calendar,
        requested_from, requested_to, comparison_provider = "Eastmoney"
      )
      if (nrow(cached) == 0L) {
        compared$findings <- data.table::rbindlist(list(
          .yahoo_overlap_issue(ticker, instrument_id, requested_to, "invalid_cached_ohlc",
                               severity = "integrity",
                               detail = "No completed local rows were available in the audit window."),
          compared$findings
        ), fill = TRUE)
      }
      if (nrow(compared$findings)) findings <- c(findings, list(compared$findings))
      report$status <- if (any(compared$findings$severity == "integrity")) "integrity_issue" else "audited"
      report$fallback_status <- "success"
      report$corroboration_status <- "success"
      report$fallback_provider <- fallback_mapping$provider
      report$fallback_symbol <- fallback_mapping$symbol
      report$fallback_rows <- nrow(fallback)
      report$integrity_issue_rows <- sum(compared$findings$severity == "integrity")
      report$informational_rows <- sum(compared$findings$severity == "informational")
      report$calendar_nontrading_dates <- as.character(compared$nontrading_dates)
    } else {
      fallback_error <- if (inherits(fallback, "error")) conditionMessage(fallback) else
        "Eastmoney returned no usable OHLC rows for the requested overlap."
      report$fallback_status <- "error"
      report$corroboration_status <- "error"
      report$fallback_error <- fallback_error
      findings <- list(.yahoo_overlap_issue(
        ticker, instrument_id, requested_to, "audit_incomplete_provider_unavailable",
        severity = "availability", related_integrity_issue = FALSE,
        detail = paste0("Yahoo error: ", primary_error, " Eastmoney error: ", fallback_error)
      ))
    }
  }
  list(findings = findings, report = report)
}

#' Audit Recent Yahoo Finance Cache Overlaps
#'
#' Compare completed daily rows in catalog-backed Yahoo Finance caches with a
#' fresh Yahoo overlap. The audit writes versioned JSON and CSV artifacts only;
#' it never synchronizes, repairs, or modifies a cache. Registry symbols without
#' catalog metadata produce explicit `skipped_missing_catalog_metadata` findings.
#'
#' @param registry Yahoo Finance registry table. Defaults to the configured
#'   runtime registry.
#' @param local_path Yahoo Finance cache directory. Defaults to the configured
#'   Yahoo Finance data path.
#' @param output_dir Audit artifact directory. Defaults to
#'   `<local_path>/_audits/yahoo_recent_overlap`.
#' @param overlap_days Number of recent calendar days to inspect. Defaults to
#'   `35`.
#' @param as_of UTC time used to exclude the current market-calendar date.
#'   For `FX_24_5`, Yahoo currency chart epochs are retained under their UTC
#'   cache-date convention; the response's exchange timezone and session-boundary
#'   metadata are used only to classify a narrow Friday session-label gap.
#' @param fallback_corroboration One of `"on_issue"`, `"none"`, or `"all"`.
#'   The default uses only existing TLS-verified Eastmoney mappings for
#'   `000300.SS` and `CNH=X`; BaoStock is never queried.
#' @param require_catalog_metadata When `TRUE`, the default, unclassified
#'   registry symbols are skipped rather than assigned inferred metadata.
#' @param max_attempts Maximum Yahoo attempts per catalog-backed instrument.
#' @param retry_delay_seconds Initial retry delay in seconds.
#'
#' @return A list with `summary`, `findings`, and JSON/CSV `artifact_paths`.
#' @export
audit_yahoofinance_recent_overlap <- function(registry = get_yahoofinance_registry(),
                                              local_path = NULL,
                                              output_dir = NULL,
                                              overlap_days = 35L,
                                              as_of = Sys.time(),
                                              fallback_corroboration = c("on_issue", "none", "all"),
                                              require_catalog_metadata = TRUE,
                                              max_attempts = 3L,
                                              retry_delay_seconds = 1) {
  fallback_corroboration <- match.arg(fallback_corroboration)
  if (!isTRUE(require_catalog_metadata)) {
    stop("Phase 1 requires catalog metadata; set require_catalog_metadata = TRUE.", call. = FALSE)
  }
  overlap_days <- as.integer(overlap_days)
  if (length(overlap_days) != 1L || is.na(overlap_days) || overlap_days < 1L) {
    stop("overlap_days must be a positive integer.", call. = FALSE)
  }
  max_attempts <- max(1L, as.integer(max_attempts))
  retry_delay_seconds <- max(0, as.numeric(retry_delay_seconds))
  as_of <- as.POSIXct(as_of, tz = "UTC")
  if (is.na(as_of)) stop("as_of must be a valid UTC date-time.", call. = FALSE)
  if (is.null(local_path)) local_path <- .quantmod_default_local_path(src = "yahoo", create = FALSE)
  if (is.null(output_dir)) output_dir <- file.path(local_path, "_audits", "yahoo_recent_overlap")

  catalog <- get_instrument_catalog()
  catalog_yahoo <- vapply(catalog$provider_identifiers, function(x) x$yahoo %||% NA_character_, character(1))
  registry <- data.table::as.data.table(registry)
  if (!"yahoo_finance_ticker" %in% names(registry)) stop("registry must contain yahoo_finance_ticker.", call. = FALSE)
  generated_at <- as.POSIXct(Sys.time(), tz = "UTC")
  findings <- list()
  instrument_reports <- list()

  for (ticker in as.character(registry$yahoo_finance_ticker)) {
    catalog_index <- match(ticker, catalog_yahoo)
    if (is.na(catalog_index)) {
      findings[[length(findings) + 1L]] <- .yahoo_overlap_issue(
        ticker, NA_character_, as.Date(NA), "skipped_missing_catalog_metadata",
        severity = "skipped", related_integrity_issue = FALSE,
        detail = "No provider-neutral instrument catalog entry defines calendar, asset class, or tolerances."
      )
      instrument_reports[[length(instrument_reports) + 1L]] <- list(
        ticker = ticker, status = "skipped_missing_catalog_metadata", catalog_metadata = FALSE
      )
      next
    }
    yahoo_symbol <- catalog_yahoo[[catalog_index]]
    calendar <- catalog$market_calendar[[catalog_index]]
    cutoff <- .yahoo_overlap_calendar_today(calendar, as_of)
    requested_to <- cutoff - 1L
    requested_from <- requested_to - overlap_days + 1L
    profile <- .yahoo_overlap_tolerance(
      catalog$asset_class[[catalog_index]], catalog$instrument_type[[catalog_index]],
      catalog$audit_profile[[catalog_index]]
    )
    cached <- tryCatch(
      get_completed_local_quantmod_OHLC(ticker, local_path = local_path, as_of = as_of),
      error = function(e) NULL
    )
    cached <- if (is.null(cached)) data.table::data.table() else data.table::copy(
      cached[cached$date < cutoff & cached$date >= requested_from & cached$date <= requested_to]
    )
    report <- list(
      ticker = ticker, instrument_id = catalog$instrument_id[[catalog_index]], status = "audited",
      market_calendar = calendar, requested_from = as.character(requested_from), requested_to = as.character(requested_to),
      cache_path = normalizePath(file.path(local_path, .quantmod_local_filename(ticker, "yahoo", "1d")), winslash = "/", mustWork = FALSE),
      cached_rows = nrow(cached), tolerance = profile, corroboration_status = "not_needed"
    )
    fallback_mapping <- .yahoo_overlap_fallback_mapping(registry, ticker)
    yahoo <- tryCatch(
      .yahoo_overlap_fetch(yahoo_symbol, requested_from, requested_to, calendar = calendar,
                           max_attempts = max_attempts, retry_delay_seconds = retry_delay_seconds),
      error = function(e) e
    )
    if (inherits(yahoo, "error")) {
      unavailable <- .yahoo_overlap_handle_unavailable(
        ticker, catalog$instrument_id[[catalog_index]], cached, profile, calendar,
        requested_from, requested_to, conditionMessage(yahoo), fallback_mapping,
        fallback_corroboration
      )
      findings <- c(findings, unavailable$findings)
      report <- modifyList(report, unavailable$report)
      report$yahoo_rows <- 0L
    } else {
      session_metadata <- attr(yahoo, "investdatar_yahoo_session_metadata")
      yahoo <- data.table::copy(yahoo)
      yahoo[, date := as.Date(date)]
      yahoo <- yahoo[date >= requested_from & date <= requested_to]
      report$yahoo_rows <- nrow(yahoo)
      yahoo_usable <- .yahoo_overlap_has_usable_ohlc(yahoo)
      if (!yahoo_usable) {
        unavailable <- .yahoo_overlap_handle_unavailable(
          ticker, catalog$instrument_id[[catalog_index]], cached, profile, calendar,
          requested_from, requested_to,
          paste0("Yahoo returned no usable OHLC rows for the requested overlap (",
                 as.character(requested_from), " through ", as.character(requested_to), ")."),
          fallback_mapping, fallback_corroboration
        )
        findings <- c(findings, unavailable$findings)
        report <- modifyList(report, unavailable$report)
      } else {
        compared <- .yahoo_overlap_compare_rows(
          ticker, catalog$instrument_id[[catalog_index]], cached, yahoo, profile, calendar,
          requested_from, requested_to, session_metadata = session_metadata
        )
        if (nrow(cached) == 0L) {
          compared$findings <- data.table::rbindlist(list(
            .yahoo_overlap_issue(ticker, catalog$instrument_id[[catalog_index]], requested_to,
                                 "invalid_cached_ohlc", severity = "integrity",
                                 detail = "No completed local rows were available in the audit window."),
            compared$findings
          ), fill = TRUE)
        }
        if (nrow(compared$findings)) findings[[length(findings) + 1L]] <- compared$findings
        integrity_issue <- nrow(compared$findings[compared$findings[["severity"]] == "integrity"]) > 0L
        if (integrity_issue) report$status <- "integrity_issue"
        report$integrity_issue_rows <- sum(compared$findings$severity == "integrity")
        report$informational_rows <- sum(compared$findings$severity == "informational")
        report$calendar_nontrading_dates <- as.character(compared$nontrading_dates)
      }

      if (yahoo_usable) {
        registry_row <- registry[registry[["yahoo_finance_ticker"]] == ticker]
      fallback_source <- if ("fallback_source" %in% names(registry) && nrow(registry_row) > 0L) {
        as.character(registry_row[["fallback_source"]][[1L]])
      } else ""
      fallback_ticker <- if ("fallback_ticker" %in% names(registry) && nrow(registry_row) > 0L) {
        as.character(registry_row[["fallback_ticker"]][[1L]])
      } else ""
      eligible_corroboration <- ticker %in% c("000300.SS", "CNH=X") &&
        !is.na(fallback_source) && identical(tolower(fallback_source), "eastmoney") &&
        !is.na(fallback_ticker) && nzchar(fallback_ticker)
      should_corroborate <- eligible_corroboration &&
        (fallback_corroboration == "all" || (fallback_corroboration == "on_issue" && integrity_issue))
      if (should_corroborate) {
        corroboration <- tryCatch(
          .fetch_eastmoney_ohlc(fallback_ticker, label = ticker, from = requested_from, to = requested_to),
          error = function(e) e
        )
        report$corroboration_status <- if (inherits(corroboration, "error")) "error" else "success"
        if (inherits(corroboration, "error")) report$corroboration_error <- conditionMessage(corroboration)
      } else if (eligible_corroboration) {
        report$corroboration_status <- if (fallback_corroboration == "none") "not_requested" else "not_needed"
        } else {
          report$corroboration_status <- "unavailable"
        }
      }
    }
    instrument_reports[[length(instrument_reports) + 1L]] <- report
  }

  all_findings <- if (length(findings)) data.table::rbindlist(findings, fill = TRUE) else .yahoo_overlap_empty_findings()
  audited_count <- sum(vapply(instrument_reports, function(x) identical(x$status, "audited") || identical(x$status, "integrity_issue"), logical(1)))
  skipped_count <- sum(vapply(instrument_reports, function(x) identical(x$status, "skipped_missing_catalog_metadata"), logical(1)))
  informational_count <- sum(vapply(instrument_reports, function(x) isTRUE(x$informational_rows > 0L), logical(1)))
  integrity_count <- sum(vapply(instrument_reports, function(x) identical(x$status, "integrity_issue"), logical(1)))
  unavailable_count <- sum(vapply(instrument_reports, function(x) identical(x$status, "audit_incomplete_provider_unavailable"), logical(1)))
  generated_at <- as.POSIXct(Sys.time(), tz = "UTC")
  paths <- .yahoo_overlap_audit_paths(output_dir, generated_at)
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  data.table::fwrite(all_findings, paths$csv)
  report <- list(
    schema_version = .yahoo_overlap_audit_schema_version,
    audit_id = tools::file_path_sans_ext(basename(paths$json)),
    generated_at_utc = format(generated_at, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    provider = "yahoo", overlap_days = overlap_days,
    as_of_utc = format(as_of, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    fallback_corroboration = fallback_corroboration,
    baostock = list(queried = FALSE, note = "BaoStock is excluded from this audit."),
    manifest_summary = list(
      registered = nrow(registry), audited = audited_count, skipped = skipped_count,
      informational_instruments = informational_count, integrity_issue_instruments = integrity_count,
      provider_unavailable_instruments = unavailable_count,
      issue_rows = nrow(all_findings)
    ),
    instruments = instrument_reports,
    findings = as.data.frame(all_findings)
  )
  jsonlite::write_json(report, paths$json, pretty = TRUE, auto_unbox = TRUE, null = "null", na = "null")
  list(
    summary = report[c("schema_version", "audit_id", "provider", "overlap_days", "as_of_utc", "manifest_summary")],
    findings = all_findings,
    artifact_paths = paths
  )
}
