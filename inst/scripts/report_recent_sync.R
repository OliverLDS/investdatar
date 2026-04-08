options(width = 120)

cat_section <- function(title) {
  cat("\n", title, "\n", strrep("-", nchar(title)), "\n", sep = "")
}

format_num <- function(x, digits = 2) {
  if (is.null(x) || length(x) == 0L || is.na(x)) {
    return("NA")
  }
  format(round(as.numeric(x), digits), nsmall = digits, trim = TRUE, scientific = FALSE)
}

tail_rows <- function(x, n) {
  if (is.null(x) || nrow(x) == 0L || is.na(n) || n <= 0L) {
    return(x[0])
  }
  utils::tail(x, n = min(as.integer(n), nrow(x)))
}

build_fred_update_lines <- function(limit = 15L) {
  local_path <- tryCatch(investdatar::get_source_data_path("fred", create = FALSE), error = function(e) NULL)
  run <- tryCatch(investdatar::get_latest_sync_run("fred", local_path = local_path), error = function(e) NULL)
  if (is.null(run) || is.null(run$summary) || !"series_id" %in% names(run$summary)) {
    return(character())
  }

  summary_dt <- data.table::as.data.table(run$summary)
  summary_dt <- summary_dt[status == "success" & ((updated %in% TRUE) | (!is.na(n_new_rows) & n_new_rows > 0L))]
  if (nrow(summary_dt) == 0L) {
    return(character())
  }

  registry <- tryCatch(investdatar::get_fred_registry(), error = function(e) NULL)
  if (!is.null(registry) && "series_id" %in% names(registry)) {
    summary_dt <- merge(summary_dt, registry[, intersect(c("series_id", "title", "units"), names(registry)), with = FALSE], by = "series_id", all.x = TRUE)
  }
  if (nrow(summary_dt) > limit) {
    summary_dt <- summary_dt[seq_len(limit)]
  }

  vapply(seq_len(nrow(summary_dt)), function(i) {
    series_id <- summary_dt$series_id[[i]]
    x <- tryCatch(investdatar::get_local_FRED_data(series_id, local_path = local_path), error = function(e) NULL)
    if (is.null(x) || nrow(x) == 0L) {
      return(sprintf("%s: local data unavailable.", series_id))
    }
    latest <- x[.N]
    prev <- if (nrow(x) >= 2L) x[.N - 1L] else NULL
    title <- if ("title" %in% names(summary_dt)) summary_dt$title[[i]] else series_id
    units <- if ("units" %in% names(summary_dt)) summary_dt$units[[i]] else NA_character_
    change_txt <- if (is.null(prev)) {
      "no prior local observation"
    } else {
      sprintf("previous %s on %s", format_num(prev$value), prev$date)
    }
    sprintf(
      "%s [%s%s]: latest %s on %s; %s.",
      title,
      series_id,
      if (!is.na(units) && nzchar(units)) paste0(", unit=", units) else "",
      format_num(latest$value),
      latest$date,
      change_txt
    )
  }, character(1))
}

build_yahoo_update_lines <- function(limit = 15L) {
  local_path <- tryCatch(investdatar::get_source_data_path("yahoofinance", create = FALSE), error = function(e) NULL)
  run <- tryCatch(investdatar::get_latest_sync_run("yahoofinance", local_path = local_path), error = function(e) NULL)
  if (is.null(run) || is.null(run$summary) || !"yahoo_finance_ticker" %in% names(run$summary)) {
    return(character())
  }

  summary_dt <- data.table::as.data.table(run$summary)
  summary_dt <- summary_dt[status == "success" & ((updated %in% TRUE) | (!is.na(n_new_rows) & n_new_rows > 0L))]
  if (nrow(summary_dt) == 0L) {
    return(character())
  }

  registry <- tryCatch(investdatar::get_yahoofinance_registry(), error = function(e) NULL)
  if (!is.null(registry) && "yahoo_finance_ticker" %in% names(registry)) {
    summary_dt <- merge(summary_dt, registry[, intersect(c("yahoo_finance_ticker", "definition"), names(registry)), with = FALSE], by = "yahoo_finance_ticker", all.x = TRUE)
  }
  if (nrow(summary_dt) > limit) {
    summary_dt <- summary_dt[seq_len(limit)]
  }

  vapply(seq_len(nrow(summary_dt)), function(i) {
    ticker <- summary_dt$yahoo_finance_ticker[[i]]
    x <- tryCatch(investdatar::get_local_quantmod_OHLC(ticker, src = "yahoo", local_path = local_path), error = function(e) NULL)
    if (is.null(x) || nrow(x) == 0L) {
      return(sprintf("%s: local data unavailable.", ticker))
    }
    latest <- x[.N]
    prev <- if (nrow(x) >= 2L) x[.N - 1L] else NULL
    definition <- if ("definition" %in% names(summary_dt)) summary_dt$definition[[i]] else NA_character_
    change_txt <- if (is.null(prev) || is.na(prev$close) || prev$close == 0) {
      "no prior close available"
    } else {
      sprintf(
        "previous close %s on %s (delta %s, %s%%)",
        format_num(prev$close),
        prev$date,
        format_num(latest$close - prev$close),
        format_num((latest$close / prev$close - 1) * 100)
      )
    }
    sprintf(
      "%s%s: latest close %s on %s; %s.",
      ticker,
      if (!is.na(definition) && nzchar(definition)) paste0(" [", definition, "]") else "",
      format_num(latest$close),
      latest$date,
      change_txt
    )
  }, character(1))
}

query_short_shock_summary <- function(lines, topic) {
  if (length(lines) == 0L) {
    return(NULL)
  }

  if (!requireNamespace("inferencer", quietly = TRUE)) {
    return(sprintf("Skipping %s AI summary because `inferencer` is not installed.", topic))
  }

  prompt <- paste(
    sprintf("You are reviewing %s data that were updated in the latest local sync.", topic),
    "Write 2 to 4 concise sentences.",
    "Focus on the main shock, unusual move, or cross-series pattern in the latest updates.",
    "If the updates look routine, say so directly.",
    "Do not use bullet points or headers.",
    "",
    paste(lines, collapse = "\n"),
    sep = "\n"
  )

  tryCatch(
    inferencer::query_openrouter(prompt),
    error = function(e) sprintf("OpenRouter summary failed for %s: %s", topic, conditionMessage(e))
  )
}

report_rss_updates <- function() {
  local_path <- tryCatch(investdatar::get_source_data_path("rss", create = FALSE), error = function(e) NULL)
  run <- tryCatch(investdatar::get_latest_sync_run("rss", local_path = local_path), error = function(e) NULL)
  if (is.null(run) || is.null(run$summary) || !"feed_id" %in% names(run$summary)) {
    cat("No RSS batch sync log is available.\n")
    return(invisible(NULL))
  }

  summary_dt <- data.table::as.data.table(run$summary)
  summary_dt <- summary_dt[status == "success" & !is.na(n_new_rows) & n_new_rows > 0L]
  if (nrow(summary_dt) == 0L) {
    cat("The latest RSS sync inserted no new feed items.\n")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(summary_dt))) {
    feed_id <- summary_dt$feed_id[[i]]
    x <- tryCatch(investdatar::get_local_rss_data(feed_id, local_path = local_path), error = function(e) NULL)
    cat(sprintf("[%s]\n", feed_id))
    if (is.null(x) || nrow(x) == 0L) {
      cat("  local data unavailable\n")
      next
    }

    rows <- tail_rows(x[order(published_at)], summary_dt$n_new_rows[[i]])
    for (j in seq_len(nrow(rows))) {
      row <- rows[j]
      cat(sprintf("  - %s | %s\n", row$published_at, row$title))
      if (!is.na(row$link) && nzchar(row$link)) {
        cat(sprintf("    %s\n", row$link))
      }
    }
  }

  invisible(NULL)
}

cat_section("RSS Updates")
report_rss_updates()

fred_lines <- build_fred_update_lines()
cat_section("FRED Shock Summary")
if (length(fred_lines) == 0L) {
  cat("The latest FRED sync log shows no updated series.\n")
} else {
  cat(query_short_shock_summary(fred_lines, "FRED"), "\n", sep = "")
}

yahoo_lines <- build_yahoo_update_lines()
cat_section("Yahoo Shock Summary")
if (length(yahoo_lines) == 0L) {
  cat("The latest Yahoo Finance sync log shows no updated tickers.\n")
} else {
  cat(query_short_shock_summary(yahoo_lines, "Yahoo Finance"), "\n", sep = "")
}
