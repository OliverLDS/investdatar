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

#' Get World Bank Registry File Path
#'
#' Resolve the JSON registry path for World Bank indicator metadata. If no
#' explicit `registry_file` is configured, the function falls back to a default
#' filename in the package config directory.
#'
#' @param config_dir Optional configuration directory used for the fallback
#'   registry path.
#'
#' @return Character scalar path.
#' @export
get_wbstats_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("wbstats"), error = function(e) list())
  registry_file <- cfg$registry_file

  if (is.null(registry_file) || !nzchar(registry_file)) {
    if (is.null(config_dir)) {
      config_dir <- getOption("investdatar.config_dir")
    }
    if (is.null(config_dir) || !nzchar(config_dir)) {
      stop(
        "No WorldBank registry path is configured. Set WorldBank.registry_file in your ",
        "config or load a config file rooted at the desired directory."
      )
    }
    return(file.path(config_dir, "world_bank_series_registry.json"))
  }

  .normalize_scalar_path(registry_file, config_dir = getOption("investdatar.config_dir"))
}

#' Get World Bank Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return `data.table`.
#' @export
get_wbstats_registry <- function(registry_path = get_wbstats_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("indicator", "country", "freq", "main_group", "label", "notes", "active", "update_time")
  )
}

#' Add Or Update One World Bank Registry Entry
#'
#' @param indicator World Bank indicator code.
#' @param country Country or aggregate code.
#' @param freq Frequency code.
#' @param main_group Optional grouping label.
#' @param label Optional display label.
#' @param notes Optional free-text notes.
#' @param active Logical flag stored in the registry.
#' @param registry_path Optional registry JSON path.
#'
#' @return The added or updated row as a `data.table`.
#' @export
add_wbstats_registry_series <- function(indicator, country = NULL, freq = "Y",
                                        main_group = NA_character_,
                                        label = NA_character_,
                                        notes = NA_character_,
                                        active = TRUE,
                                        registry_path = get_wbstats_registry_file_path()) {
  registry <- get_wbstats_registry(registry_path = registry_path)
  template_names <- names(registry)
  country_value <- if (is.null(country) || (is.character(country) && length(country) == 1L && (!nzchar(country) || is.na(country)))) "" else as.character(country)

  new_row <- data.table::data.table(
    indicator = indicator,
    country = country_value,
    freq = toupper(freq),
    main_group = main_group,
    label = label,
    notes = notes,
    active = active,
    update_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  new_row <- .align_registry_schema(new_row, template_names)

  if (nrow(registry) > 0L) {
    indicator_value <- indicator
    freq_value <- toupper(freq)
    registry <- registry[!(indicator == indicator_value & country == country_value & toupper(freq) == freq_value)]
  }

  registry <- data.table::rbindlist(list(registry, new_row), use.names = TRUE, fill = TRUE)
  data.table::setorderv(registry, c("indicator", "country", "freq"))
  .write_json_registry(registry, registry_path)

  indicator_value <- indicator
  freq_value <- toupper(freq)
  registry[indicator == indicator_value & country == country_value & toupper(freq) == freq_value]
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
  wb_args <- list(
    indicator = indicator,
    country = country,
    return_wide = FALSE
  )
  if (!is.null(start_date)) {
    wb_args$start_date <- start_date
  }
  if (!is.null(end_date)) {
    wb_args$end_date <- end_date
  }
  if (!is.null(mrv)) {
    wb_args$mrv <- mrv
  }
  if (!is.null(mrnev)) {
    wb_args$mrnev <- mrnev
  }
  if (!is.null(cache)) {
    wb_args$cache <- cache
  }
  if (!missing(freq)) {
    wb_args$freq <- freq
  }
  if (isTRUE(gapfill)) {
    wb_args$gapfill <- TRUE
  }
  if (isTRUE(date_as_class_date)) {
    wb_args$date_as_class_date <- TRUE
  }
  if (!is.null(lang)) {
    wb_args$lang <- lang
  }

  dt <- do.call(wbstats::wb_data, wb_args)

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

#' Synchronize All World Bank Registry Series
#'
#' @param registry Optional World Bank registry table.
#' @param local_path Optional local storage path.
#' @param ... Passed to `sync_local_wbstats_data()`.
#'
#' @return Summary `data.table`.
#' @export
sync_all_wbstats_registry_data <- function(registry = get_wbstats_registry(), local_path = NULL, ...) {
  stopifnot(all(c("indicator", "country") %in% names(registry)))

  if (is.null(local_path)) {
    local_path <- get_source_data_path("wbstats", create = TRUE)
  }
  run_started_at <- Sys.time()

  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry[["active"]]))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }

  summary_list <- lapply(seq_len(nrow(registry)), function(i) {
    indicator <- registry$indicator[[i]]
    country <- if ("country" %in% names(registry)) registry$country[[i]] else ""
    if (is.null(country) || is.na(country) || !nzchar(country)) {
      country <- "countries_only"
    }
    freq <- if ("freq" %in% names(registry)) registry$freq[[i]] else "Y"
    if (is.null(freq) || is.na(freq) || !nzchar(freq)) {
      freq <- "Y"
    }
    freq <- toupper(freq)

    tryCatch(
      {
        res <- sync_local_wbstats_data(
          indicator = indicator,
          country = country,
          freq = freq,
          local_path = local_path,
          ...
        )
        data.table::data.table(
          indicator = indicator,
          country = country,
          freq = freq,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          indicator = indicator,
          country = country,
          freq = freq,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          error = conditionMessage(e)
        )
      }
    )
  })

  summary_dt <- data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE)
  .write_sync_run_log(
    source_id = "wbstats",
    summary = summary_dt,
    local_path = local_path,
    params = list(),
    run_started_at = run_started_at,
    run_finished_at = Sys.time()
  )
  summary_dt
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
