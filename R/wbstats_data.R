.wbstats_frequency_label <- function(freq) {
  freq <- toupper(freq)
  switch(
    freq,
    Y = "Yearly",
    Q = "Quarterly",
    M = "Monthly",
    stop("Unsupported wbstats frequency: ", freq)
  )
}

.parse_wbstats_date <- function(x, freq = "Y") {
  if (inherits(x, "Date")) {
    return(as.Date(x))
  }

  x <- as.character(x)
  freq <- toupper(freq)

  if (freq == "Y") {
    return(as.Date(sprintf("%s-01-01", x)))
  }
  if (freq == "Q") {
    year <- sub("Q[1-4].*$", "", x)
    quarter <- sub("^.*Q([1-4])$", "\\1", x)
    month <- sprintf("%02d", (as.integer(quarter) - 1L) * 3L + 1L)
    return(as.Date(sprintf("%s-%s-01", year, month)))
  }
  if (freq == "M") {
    year <- sub("M[0-9]{2}$", "", x)
    month <- sub("^.*M([0-9]{2})$", "\\1", x)
    return(as.Date(sprintf("%s-%s-01", year, month)))
  }

  stop("Unsupported wbstats frequency: ", freq)
}

.normalize_wbstats_data <- function(dt, indicator = NULL, country = NULL, freq = "Y") {
  dt <- .as_data_table(dt)
  if (is.null(dt)) {
    return(NULL)
  }

  rename_map <- c(
    indicatorID = "indicator_id",
    indicator_id = "indicator_id",
    iso2Code = "iso2c",
    iso3Code = "iso3c"
  )
  for (old_name in names(rename_map)) {
    new_name <- rename_map[[old_name]]
    if (old_name %in% names(dt) && old_name != new_name && !new_name %in% names(dt)) {
      data.table::setnames(dt, old_name, new_name)
    }
  }

  if (!"indicator_id" %in% names(dt) && !is.null(indicator) && length(indicator) == 1L) {
    dt[, indicator_id := indicator]
  }
  if (!"country" %in% names(dt) && !is.null(country) && length(country) == 1L) {
    dt[, country := country]
  }

  if ("value" %in% names(dt)) {
    dt[, value := as.numeric(value)]
  } else {
    dt[, value := NA_real_]
  }

  if (!"date" %in% names(dt)) {
    stop("wbstats data must contain a date column.")
  }
  dt[, date := .parse_wbstats_date(date, freq = freq)]
  dt[, source := "wbstats"]
  dt[, frequency := toupper(freq)]

  first_cols <- c("source", "indicator_id", "country", "iso2c", "iso3c", "frequency", "date", "value")
  for (nm in first_cols) {
    if (!nm %in% names(dt)) {
      dt[, (nm) := NA]
    }
  }
  data.table::setcolorder(dt, c(first_cols, setdiff(names(dt), first_cols)))
  data.table::setorderv(dt, "date")
  dt[]
}

.wbstats_local_filename <- function(indicator, country, freq = "Y") {
  indicator <- gsub("[^A-Za-z0-9._-]+", "_", indicator)
  country <- gsub("[^A-Za-z0-9._-]+", "_", country)
  sprintf("%s__%s__%s.rds", indicator, country, toupper(freq))
}

#' Get World Bank Data via wbstats
#'
#' Downloads indicator data using the `wbstats` package and standardizes it to
#' a package-native long schema.
#'
#' @param indicator World Bank indicator code.
#' @param country Country or aggregate code.
#' @param start_date Optional start date.
#' @param end_date Optional end date.
#' @param mrv Optional number of most recent values.
#' @param mrnev Optional number of most recent non-empty values.
#' @param cache Optional cache object passed to `wbstats::wb_data()`.
#' @param freq Frequency code: `"Y"`, `"Q"`, or `"M"`.
#' @param gapfill Logical passed to `wbstats::wb_data()`.
#' @param date_as_class_date Logical passed to `wbstats::wb_data()`.
#' @param lang Optional language code.
#'
#' @return Standardized `data.table`.
#' @export
get_source_data_wbstats <- function(indicator, country = "countries_only",
                                    start_date = NULL, end_date = NULL,
                                    mrv = NULL, mrnev = NULL, cache = NULL,
                                    freq = "Y", gapfill = FALSE,
                                    date_as_class_date = FALSE, lang = NULL) {
  .require_suggested_package("wbstats", "to retrieve World Bank data.")
  dt <- wbstats::wb_data(
    indicator = indicator,
    country = country,
    start_date = start_date,
    end_date = end_date,
    return_wide = FALSE,
    mrv = mrv,
    mrnev = mrnev,
    cache = cache,
    freq = freq,
    gapfill = gapfill,
    date_as_class_date = date_as_class_date,
    lang = lang
  )

  .normalize_wbstats_data(dt, indicator = indicator, country = country, freq = freq)
}

#' Get Fallback Source Update Time for wbstats Data
#'
#' @param freq Frequency code: `"Y"`, `"Q"`, or `"M"`.
#' @param tz Time zone used for the inferred timestamp.
#'
#' @return POSIXct.
#' @export
get_source_utime_wbstats <- function(freq = "Y", tz = "UTC") {
  infer_source_utime_from_frequency(.wbstats_frequency_label(freq), tz = tz)
}

#' Get Local World Bank Data
#'
#' @param indicator World Bank indicator code.
#' @param country Country or aggregate code.
#' @param freq Frequency code.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_wbstats_data <- function(indicator, country, freq = "Y", local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("wbstats")
  }
  file_path <- file.path(local_path, .wbstats_local_filename(indicator, country, freq))
  .read_local_data_table(file_path, sort_cols = "date")
}

#' Synchronize Local World Bank Data
#'
#' @param indicator World Bank indicator code.
#' @param country Country or aggregate code.
#' @param freq Frequency code.
#' @param local_path Optional local storage path.
#' @param ... Passed to `get_source_data_wbstats()`.
#'
#' @return A sync result list.
#' @export
sync_local_wbstats_data <- function(indicator, country, freq = "Y", local_path = NULL, ...) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("wbstats", create = TRUE)
  }
  dir.create(local_path, recursive = TRUE, showWarnings = FALSE)
  local_file_path <- file.path(local_path, .wbstats_local_filename(indicator, country, freq))

  new_dt <- get_source_data_wbstats(indicator = indicator, country = country, freq = freq, ...)
  source_utime <- get_source_utime_wbstats(freq = freq)

  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = c("indicator_id", "country", "date"),
    order_cols = "date",
    source_utime = source_utime
  )
}

#' Detect Gaps In Local World Bank Data
#'
#' @param x A World Bank `data.table`, or `NULL` when reading from `local_path`.
#' @param indicator Optional indicator code when reading local data.
#' @param country Optional country code when reading local data.
#' @param freq Frequency code.
#' @param local_path Optional local storage path.
#'
#' @return A `data.table` of gaps.
#' @export
detect_time_gaps_wbstats <- function(x = NULL, indicator = NULL, country = NULL, freq = "Y", local_path = NULL) {
  if (is.null(x)) {
    dt <- get_local_wbstats_data(indicator = indicator, country = country, freq = freq, local_path = local_path)
  } else {
    dt <- .as_data_table(x)
  }
  detect_time_gaps(dt, time_col = "date", frequency = .wbstats_frequency_label(freq))
}
