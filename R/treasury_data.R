.treasury_dataset_map <- function() {
  list(
    bill_rates = list(
      archive_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives/bill-rates-2002-2023.xml",
      current_data_key = "daily_treasury_bill_rates",
      archive_end_year = 2023L
    ),
    par_yield_curve = list(
      archive_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives/par-yield-curve-rates-1990-2023.xml",
      current_data_key = "daily_treasury_yield_curve",
      archive_end_year = 2023L
    ),
    long_term_rates = list(
      archive_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives/long-term-rates-2000-2023.xml",
      current_data_key = "daily_treasury_long_term_rate",
      archive_end_year = 2023L
    ),
    real_yield_curve = list(
      archive_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives/par-real-yield-curve-rates-2003-2023.xml",
      current_data_key = "daily_treasury_real_yield_curve",
      archive_end_year = 2023L
    ),
    real_long_term_rates = list(
      archive_url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rate-archives/real-long-term-rates-2000-2023.xml",
      current_data_key = "daily_treasury_real_long_term",
      archive_end_year = 2023L
    )
  )
}

.normalize_treasury_dataset <- function(dataset) {
  stopifnot(is.character(dataset), length(dataset) == 1L)
  aliases <- list(
    bill_rates = c("bill_rates", "bill", "bill_rate", "daily_treasury_bill_rates"),
    par_yield_curve = c("par_yield_curve", "yield_curve", "par_curve", "daily_treasury_yield_curve"),
    long_term_rates = c("long_term_rates", "long_term", "daily_treasury_long_term_rate"),
    real_yield_curve = c("real_yield_curve", "par_real_yield_curve", "daily_treasury_real_yield_curve"),
    real_long_term_rates = c("real_long_term_rates", "real_long_term", "daily_treasury_real_long_term")
  )

  key <- tolower(dataset)
  matched <- names(aliases)[vapply(aliases, function(x) key %in% x, logical(1))]
  if (length(matched) == 0L) {
    stop("Unknown Treasury dataset: ", dataset)
  }
  matched[[1]]
}

.treasury_current_year_url <- function(data_key, year) {
  sprintf(
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=%s&field_tdr_date_value=%s",
    data_key,
    as.integer(year)
  )
}

.treasury_requested_years <- function(years = NULL) {
  if (is.null(years)) {
    return(seq.int(2024L, as.integer(format(Sys.Date(), "%Y"))))
  }

  years <- as.integer(years)
  years <- years[!is.na(years)]
  sort(unique(years))
}

.treasury_resolve_urls <- function(dataset, years = NULL) {
  dataset <- .normalize_treasury_dataset(dataset)
  meta <- .treasury_dataset_map()[[dataset]]
  requested_years <- .treasury_requested_years(years)

  urls <- list()
  if (is.null(years) || any(requested_years <= meta$archive_end_year)) {
    urls[["archive"]] <- meta$archive_url
  }

  current_years <- requested_years[requested_years > meta$archive_end_year]
  if (length(current_years) > 0L) {
    for (year in current_years) {
      urls[[as.character(year)]] <- .treasury_current_year_url(meta$current_data_key, year)
    }
  }

  urls
}

.parse_treasury_atom_feed <- function(feed_text) {
  doc <- xml2::read_xml(feed_text)
  ns <- xml2::xml_ns(doc)
  search_ns <- c(d1 = "http://www.w3.org/2005/Atom", ns)
  entries <- xml2::xml_find_all(doc, ".//d1:entry", ns = search_ns)
  feed_updated <- xml2::xml_text(xml2::xml_find_first(doc, "(//d1:updated)[1]", ns = search_ns))
  feed_updated <- as.POSIXct(feed_updated, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  entry_rows <- lapply(entries, function(entry) {
    props <- xml2::xml_find_all(entry, "./d1:content/m:properties/*", ns = search_ns)
    if (length(props) == 0L) {
      return(NULL)
    }
    out <- as.list(xml2::xml_text(props, trim = TRUE))
    names(out) <- xml2::xml_name(props)
    data.table::as.data.table(out)
  })
  entry_rows <- Filter(Negate(is.null), entry_rows)
  entry_dt <- if (length(entry_rows) == 0L) data.table::data.table() else data.table::rbindlist(entry_rows, use.names = TRUE, fill = TRUE)

  if (is.null(entry_dt) || nrow(entry_dt) == 0L) {
    entry_dt <- data.table::data.table()
  }

  list(data = entry_dt, source_updated_at = feed_updated)
}

.combine_treasury_fetches <- function(fetches) {
  data_parts <- Filter(function(x) !is.null(x$data), fetches)
  data_tables <- lapply(data_parts, `[[`, "data")
  data_tables <- Filter(function(x) !is.null(x) && nrow(x) > 0L, data_tables)
  data_dt <- if (length(data_tables) == 0L) data.table::data.table() else data.table::rbindlist(data_tables, use.names = TRUE, fill = TRUE)

  updated_parts <- vapply(fetches, function(x) {
    if (is.null(x$source_updated_at)) {
      return(as.numeric(NA))
    }
    as.numeric(x$source_updated_at)
  }, numeric(1))
  source_updated_at <- if (all(is.na(updated_parts))) NULL else as.POSIXct(max(updated_parts, na.rm = TRUE), origin = "1970-01-01", tz = "UTC")

  list(data = data_dt, source_updated_at = source_updated_at)
}

.as_numeric_or_na <- function(x) {
  x <- trimws(as.character(x))
  x[!nzchar(x)] <- NA_character_
  suppressWarnings(as.numeric(x))
}

.as_date_or_na <- function(x) {
  x <- trimws(as.character(x))
  x[!nzchar(x)] <- NA_character_
  as.Date(sub("T.*$", "", x))
}

.treasury_long_empty <- function() {
  data.table::data.table(
    dataset = character(),
    date = as.Date(character()),
    series_id = character(),
    tenor = character(),
    measure = character(),
    value = numeric(),
    rate_type = character(),
    source_field = character(),
    maturity_date = as.Date(character()),
    cusip = character()
  )
}

.append_treasury_series_rows <- function(rows, dataset, date, series_id, tenor, measure, value,
                                         rate_type = NA_character_, source_field = NA_character_,
                                         maturity_date = as.Date(NA), cusip = NA_character_) {
  value <- .as_numeric_or_na(value)
  if (is.na(value)) {
    return(rows)
  }

  rows[[length(rows) + 1L]] <- data.table::data.table(
    dataset = dataset,
    date = as.Date(date),
    series_id = series_id,
    tenor = tenor,
    measure = measure,
    value = value,
    rate_type = rate_type,
    source_field = source_field,
    maturity_date = as.Date(maturity_date),
    cusip = cusip
  )
  rows
}

.normalize_treasury_bill_rates <- function(dt, dataset) {
  if (nrow(dt) == 0L) {
    return(.treasury_long_empty())
  }

  dt <- data.table::copy(dt)
  date <- .as_date_or_na(if ("QUOTE_DATE" %in% names(dt)) dt$QUOTE_DATE else dt$INDEX_DATE)

  tenors <- c("4WK", "6WK", "8WK", "13WK", "17WK", "26WK", "52WK")
  rows <- list()
  for (i in seq_len(nrow(dt))) {
    for (tenor in tenors) {
      close_col <- paste0("ROUND_B1_CLOSE_", tenor, "_2")
      yield_col <- paste0("ROUND_B1_YIELD_", tenor, "_2")
      maturity_col <- paste0("MATURITY_DATE_", tenor)
      cusip_col <- paste0("CUSIP_", tenor)

      rows <- .append_treasury_series_rows(
        rows, dataset, date[[i]],
        series_id = paste0("bill_close_", tolower(tenor)),
        tenor = tenor,
        measure = "close",
        value = if (close_col %in% names(dt)) dt[[close_col]][[i]] else NA_character_,
        source_field = close_col,
        maturity_date = if (maturity_col %in% names(dt)) .as_date_or_na(dt[[maturity_col]][[i]]) else as.Date(NA),
        cusip = if (cusip_col %in% names(dt)) dt[[cusip_col]][[i]] else NA_character_
      )
      rows <- .append_treasury_series_rows(
        rows, dataset, date[[i]],
        series_id = paste0("bill_yield_", tolower(tenor)),
        tenor = tenor,
        measure = "yield",
        value = if (yield_col %in% names(dt)) dt[[yield_col]][[i]] else NA_character_,
        source_field = yield_col,
        maturity_date = if (maturity_col %in% names(dt)) .as_date_or_na(dt[[maturity_col]][[i]]) else as.Date(NA),
        cusip = if (cusip_col %in% names(dt)) dt[[cusip_col]][[i]] else NA_character_
      )
    }
  }

  if (length(rows) == 0L) {
    return(.treasury_long_empty())
  }
  out <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  data.table::setorderv(out, c("date", "series_id"))
  out[]
}

.normalize_treasury_curve_rates <- function(dt, dataset, prefix) {
  if (nrow(dt) == 0L) {
    return(.treasury_long_empty())
  }

  dt <- data.table::copy(dt)
  date <- .as_date_or_na(dt$NEW_DATE)
  value_cols <- grep(paste0("^", prefix, "_"), names(dt), value = TRUE)
  value_cols <- setdiff(value_cols, c(paste0(prefix, "_30YEARDISPLAY")))

  rows <- list()
  for (i in seq_len(nrow(dt))) {
    for (col in value_cols) {
      tenor <- tolower(sub(paste0("^", prefix, "_"), "", col))
      rows <- .append_treasury_series_rows(
        rows, dataset, date[[i]],
        series_id = paste0(dataset, "_", tenor),
        tenor = toupper(tenor),
        measure = "yield",
        value = dt[[col]][[i]],
        source_field = col
      )
    }
  }

  if (length(rows) == 0L) {
    return(.treasury_long_empty())
  }
  out <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  data.table::setorderv(out, c("date", "series_id"))
  out[]
}

.normalize_treasury_long_term_rates <- function(dt, dataset) {
  if (nrow(dt) == 0L) {
    return(.treasury_long_empty())
  }

  dt <- data.table::copy(dt)
  dt[, date := .as_date_or_na(QUOTE_DATE)]
  dt[, rate_type := as.character(RATE_TYPE)]
  dt[, series_id := paste0("long_term_", tolower(gsub("[^A-Za-z0-9]+", "_", rate_type)))]
  dt[, tenor := data.table::fifelse(rate_type == "BC_20year", "20YEAR", data.table::fifelse(rate_type == "Over_10_Years", "OVER_10_YEARS", NA_character_))]
  dt[, measure := "yield"]
  dt[, value := .as_numeric_or_na(RATE)]
  dt[, source_field := "RATE"]
  dt[, maturity_date := as.Date(NA)]
  dt[, cusip := NA_character_]

  out <- dt[!is.na(value), .(
    dataset = dataset,
    date,
    series_id,
    tenor,
    measure,
    value,
    rate_type,
    source_field,
    maturity_date,
    cusip
  )]
  data.table::setorderv(out, c("date", "series_id"))
  out[]
}

.normalize_treasury_real_long_term_rates <- function(dt, dataset) {
  if (nrow(dt) == 0L) {
    return(.treasury_long_empty())
  }

  dt <- data.table::copy(dt)
  dt[, date := .as_date_or_na(QUOTE_DATE)]
  dt[, value := .as_numeric_or_na(RATE)]
  out <- dt[!is.na(value), .(
    dataset = dataset,
    date,
    series_id = "real_long_term_rate",
    tenor = NA_character_,
    measure = "yield",
    value,
    rate_type = "real_long_term_rate",
    source_field = "RATE",
    maturity_date = as.Date(NA),
    cusip = NA_character_
  )]
  data.table::setorderv(out, c("date", "series_id"))
  out[]
}

.normalize_treasury_rates <- function(dt, dataset) {
  dataset_name <- .normalize_treasury_dataset(dataset)
  normalizer <- switch(
    dataset_name,
    bill_rates = .normalize_treasury_bill_rates,
    par_yield_curve = function(x, y) .normalize_treasury_curve_rates(x, y, prefix = "BC"),
    long_term_rates = .normalize_treasury_long_term_rates,
    real_yield_curve = function(x, y) .normalize_treasury_curve_rates(x, y, prefix = "TC"),
    real_long_term_rates = .normalize_treasury_real_long_term_rates
  )

  out <- normalizer(dt, dataset_name)
  if (nrow(out) == 0L) {
    return(.treasury_long_empty())
  }

  out[, dataset := dataset_name]
  data.table::setorderv(out, c("date", "series_id"))
  out[]
}

.treasury_local_filename <- function(dataset) {
  sprintf("%s.rds", .normalize_treasury_dataset(dataset))
}

.fetch_treasury_feed_text <- function(url) {
  res <- curl::curl_fetch_memory(url)
  rawToChar(res$content)
}

.collect_treasury_dataset <- function(dataset, years = NULL) {
  dataset <- .normalize_treasury_dataset(dataset)
  urls <- .treasury_resolve_urls(dataset, years = years)
  if (length(urls) == 0L) {
    return(list(data = .treasury_long_empty(), source_updated_at = NULL))
  }

  fetches <- lapply(urls, function(url) {
    parsed <- .parse_treasury_atom_feed(.fetch_treasury_feed_text(url))
    parsed$data <- .normalize_treasury_rates(parsed$data, dataset = dataset)
    parsed
  })

  combined <- .combine_treasury_fetches(fetches)
  dt <- combined$data
  if (nrow(dt) == 0L) {
    return(list(data = .treasury_long_empty(), source_updated_at = combined$source_updated_at))
  }

  requested_years <- .treasury_requested_years(years)
  if (!is.null(years)) {
    dt <- dt[as.integer(format(date, "%Y")) %in% requested_years]
  }

  data.table::setorderv(dt, c("date", "series_id"))
  dt <- unique(dt, by = c("dataset", "date", "series_id"))
  list(data = dt, source_updated_at = combined$source_updated_at)
}

#' Get Treasury Rates Data
#'
#' Download U.S. Treasury raw daily rates and return a normalized long-format
#' `data.table`.
#'
#' @param dataset Treasury dataset key. Supported values are `"bill_rates"`,
#'   `"par_yield_curve"`, `"long_term_rates"`, `"real_yield_curve"`, and
#'   `"real_long_term_rates"`.
#' @param years Optional integer vector of calendar years to fetch. If omitted,
#'   the function combines the historical archive with current-year annual XML
#'   feeds from 2024 through the current year.
#'
#' @return `data.table`.
#' @export
get_source_data_treasury_rates <- function(dataset, years = NULL) {
  .collect_treasury_dataset(dataset = dataset, years = years)$data
}

#' Get Treasury Rates Last Update Time
#'
#' @inheritParams get_source_data_treasury_rates
#'
#' @return POSIXct or `NULL`.
#' @export
get_source_utime_treasury_rates <- function(dataset, years = NULL) {
  .collect_treasury_dataset(dataset = dataset, years = years)$source_updated_at
}

#' Get Local Treasury Rates
#'
#' @param dataset Treasury dataset key.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_treasury_rates <- function(dataset, local_path = NULL) {
  dataset <- .normalize_treasury_dataset(dataset)
  if (is.null(local_path)) {
    local_path <- get_source_data_path("treasury")
  }

  .read_local_data_table(
    file.path(local_path, .treasury_local_filename(dataset)),
    sort_cols = c("date", "series_id")
  )
}

#' Synchronize Local Treasury Rates
#'
#' @inheritParams get_source_data_treasury_rates
#' @param local_path Optional local storage path.
#'
#' @return A sync result list.
#' @export
sync_local_treasury_rates <- function(dataset, years = NULL, local_path = NULL) {
  dataset <- .normalize_treasury_dataset(dataset)
  if (is.null(local_path)) {
    local_path <- get_source_data_path("treasury", create = TRUE)
  }

  fetched <- .collect_treasury_dataset(dataset = dataset, years = years)
  source_dt <- fetched$data
  source_utime <- fetched$source_updated_at

  sync_local_data(
    new_data = source_dt,
    local_file_path = file.path(local_path, .treasury_local_filename(dataset)),
    key_cols = c("dataset", "date", "series_id"),
    order_cols = c("date", "series_id"),
    source_utime = source_utime
  )
}

#' Synchronize All Treasury Rate Datasets
#'
#' @param datasets Optional dataset vector. If omitted, sync all supported
#'   Treasury rate datasets.
#' @param years Optional year filter passed to `sync_local_treasury_rates()`.
#' @param local_path Optional local storage path.
#'
#' @return Summary `data.table`.
#' @export
sync_all_treasury_rates <- function(datasets = names(.treasury_dataset_map()), years = NULL, local_path = NULL) {
  datasets <- unique(vapply(datasets, .normalize_treasury_dataset, character(1)))

  if (is.null(local_path)) {
    local_path <- get_source_data_path("treasury", create = TRUE)
  }

  summary_list <- lapply(datasets, function(dataset) {
    tryCatch(
      {
        res <- sync_local_treasury_rates(dataset = dataset, years = years, local_path = local_path)
        data.table::data.table(
          dataset = dataset,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          dataset = dataset,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          error = conditionMessage(e)
        )
      }
    )
  })

  data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE)
}

#' Describe Treasury Rates
#'
#' Creates a compact narrative for locally stored Treasury raw rates.
#'
#' @param dataset Treasury dataset key.
#' @param local_path Optional local storage path.
#'
#' @return Character scalar narrative.
#' @export
describe_treasury_rates <- function(dataset, local_path = NULL) {
  dataset <- .normalize_treasury_dataset(dataset)
  dt <- get_local_treasury_rates(dataset = dataset, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) {
    stop("Local Treasury data not found for dataset: ", dataset)
  }

  paste(
    sprintf("This object is a data.table of raw U.S. Treasury rates for dataset %s.", dataset),
    "Core columns are dataset, date, series_id, tenor, measure, value, rate_type, source_field, maturity_date, and cusip.",
    sprintf("The table currently contains %s rows across %s unique series.", nrow(dt), data.table::uniqueN(dt$series_id)),
    .describe_time_coverage(dt$date),
    .describe_value_summary(dt$value),
    "This summary is intended to help another analyst agent understand the dataset before writing R analysis or visualization code."
  )
}
