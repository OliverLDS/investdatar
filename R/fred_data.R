.new_fred_api_error <- function(status_code, error_code = NULL, error_message = NULL) {
  details <- c(
    paste0("HTTP status ", status_code),
    if (!is.null(error_code) && length(error_code) > 0L && !is.na(error_code)) paste0("FRED error_code ", error_code),
    if (!is.null(error_message) && length(error_message) > 0L && !is.na(error_message) && nzchar(error_message)) paste0("FRED error_message: ", error_message)
  )
  structure(
    list(
      message = paste("FRED API request failed:", paste(details, collapse = "; ")),
      call = NULL,
      status_code = as.integer(status_code),
      error_code = error_code,
      error_message = error_message
    ),
    class = c("investdatar_fred_api_error", "error", "condition")
  )
}

.new_fred_empty_observations_error <- function(series_id, attempts, metadata = NULL, status_code = NA_integer_) {
  range <- if (!is.null(metadata) && .fred_metadata_is_available(metadata)) {
    paste0(" (metadata observation range ", metadata$start, " to ", metadata$end, ")")
  } else {
    ""
  }
  structure(
    list(
      message = paste0(
        "FRED observations endpoint returned zero rows for available series: ",
        series_id,
        " after ",
        attempts,
        " attempt(s)",
        range,
        "."
      ),
      call = NULL,
      series_id = series_id,
      attempts = attempts,
      status_code = status_code,
      metadata = metadata
    ),
    class = c("investdatar_fred_empty_observations_error", "error", "condition")
  )
}

.fred_metadata_is_available <- function(metadata) {
  !is.null(metadata) &&
    is.character(metadata$start) && length(metadata$start) == 1L && !is.na(metadata$start) && nzchar(metadata$start) &&
    is.character(metadata$end) && length(metadata$end) == 1L && !is.na(metadata$end) && nzchar(metadata$end)
}

.fred_empty_observation_retry_delay <- function(attempt, max_delay = 30) {
  min(2^(attempt - 1L), max_delay)
}

.fred_observations_are_empty <- function(data) {
  observations <- data$observations
  is.null(observations) || nrow(data.table::as.data.table(observations)) == 0L
}

.fred_api_query <- function(series_id, api_key, mode) {
  list(series_id = series_id, api_key = api_key, file_type = mode)
}

# Fetch JSON from a FRED API URL and retain the response status for callers.
.fetch_fred_json <- function(url, query = NULL) {
  response <- tryCatch(
    .http_request("GET", url, query = query),
    investdatar_http_error = function(error) {
      list(
        status_code = error$status_code,
        headers = error$response_headers,
        content = charToRaw(error$response_body),
        url = error$url
      )
    }
  )
  status_code <- as.integer(response$status_code)
  response_text <- .http_response_text(response)
  data <- tryCatch(
    jsonlite::fromJSON(response_text, simplifyVector = TRUE),
    error = function(error) {
      stop(
        .new_fred_api_error(
          status_code,
          error_message = paste0("Invalid JSON response: ", conditionMessage(error))
        )
      )
    }
  )
  error_code <- data$error_code
  error_message <- data$error_message
  if (status_code < 200L || status_code >= 300L || !is.null(error_code) || !is.null(error_message)) {
    stop(.new_fred_api_error(status_code, error_code, error_message))
  }
  attr(data, "investdatar_http_status") <- status_code
  data
}

#' Get FRED Series Data
#'
#' Download a FRED series and return dates and numeric values.
#'
#' @param series_id Character. FRED series ID (e.g., "DGS10").
#' @param config Optional list with fields: `api_key`, `url`, `mode`.
#'
#' @return data.table with columns: date (Date), value (numeric, NA for ".").
#' @export
get_source_data_fred <- function(series_id, config = NULL) {
  config <- .get_api_config("fred", config = config)
  api_key <- config$api_key
  url <- paste0(config$url, "/observations")
  mode <- config$mode
  query <- .fred_api_query(series_id, api_key, mode)
  
  max_attempts <- 3L
  data <- .fetch_fred_json(url, query = query)
  attempts <- 1L

  if (.fred_observations_are_empty(data)) {
    metadata <- tryCatch(get_source_metadata_fred(series_id, config = config), error = function(e) NULL)
    if (.fred_metadata_is_available(metadata)) {
      while (.fred_observations_are_empty(data) && attempts < max_attempts) {
        Sys.sleep(.fred_empty_observation_retry_delay(attempts))
        data <- .fetch_fred_json(url, query = query)
        attempts <- attempts + 1L
      }
      if (.fred_observations_are_empty(data)) {
        stop(.new_fred_empty_observations_error(
          series_id, attempts, metadata,
          status_code = attr(data, "investdatar_http_status") %||% NA_integer_
        ))
      }
    }

    if (.fred_observations_are_empty(data)) {
      registry <- tryCatch(get_fred_registry(), error = function(e) NULL)
      if (!is.null(registry) && "series_id" %in% names(registry) && series_id %in% registry$series_id) {
        stop(.new_fred_empty_observations_error(
          series_id, attempts, metadata,
          status_code = attr(data, "investdatar_http_status") %||% NA_integer_
        ))
      }
      stop("FRED observations endpoint returned zero rows for series: ", series_id, call. = FALSE)
    }
  }

  observations <- data$observations
  # contains "." in early GDP
  raw_values <- observations$value
  raw_values[raw_values == "."] <- NA_character_ # because the returned value is string
  numeric_values <- as.numeric(raw_values)
  
  out <- data.table::data.table(
    date = as.Date(observations$date),
    value = numeric_values
  )
  data.table::setorder(out, date)
  out[]
}

#' Get FRED Series Last Update Time
#'
#' Return the last update time of a FRED series as POSIXct in UTC.
#'
#' @param series_id Character. FRED series ID.
#' @param config Optional list with fields: `api_key`, `url`, `mode`.
#' @param from_server Logical. If `TRUE`, query the FRED series endpoint for
#'   the reported update time. Otherwise infer it from the registry frequency.
#' @param tz Time zone used when parsing or inferring the update time.
#'
#' @return POSIXct (UTC).
#' @export
get_source_utime_fred <- function(series_id, config = NULL, from_server = FALSE, tz = "America/Chicago") {
  config <- .get_api_config("fred", config = config)
  if (from_server) {
    api_key <- config$api_key
    url <- config$url
    mode <- config$mode
    query <- .fred_api_query(series_id, api_key, mode)
    data <- .fetch_fred_json(url, query = query)
    
    update_time_str <- data$seriess$last_updated # we suppose it is central time
    out <- as.POSIXct(update_time_str, format = "%Y-%m-%d %H:%M:%S", tz = tz)
  } else {
    registry <- tryCatch(get_fred_registry(), error = function(e) NULL)
    freq <- NULL
    if (!is.null(registry)) {
      series_id_value <- series_id
      freq <- registry[series_id == series_id_value, freq][[1]]
      if (length(freq) == 0L) {
        freq <- NULL
      }
    }
    if (is.null(freq)) {
      freq <- "Daily"
    }
    out <- infer_source_utime_from_frequency(freq, reference_time = Sys.time(), tz = tz)
  }
  out
}

#' Retrieve metadata for a FRED series
#'
#' Queries the FRED API to obtain basic metadata of a given series, such as
#' title, observation range, frequency, units, and seasonal adjustment.
#'
#' @param series_id A character string. The FRED series ID (e.g., "AMERIBOR").
#' @param config A list containing API configuration with elements:
#'   \describe{
#'     \item{api_key}{Your FRED API key as a character string.}
#'     \item{url}{Base URL of the FRED API endpoint for series (e.g., "https://api.stlouisfed.org/fred/series").}
#'     \item{mode}{File type to request, usually "json".}
#'   }
#'
#' @return A list with elements:
#' \itemize{
#'   \item \code{title} — Series title
#'   \item \code{start} — Observation start date
#'   \item \code{end} — Observation end date
#'   \item \code{freq} — Data frequency
#'   \item \code{units} — Measurement units
#'   \item \code{season} — Seasonal adjustment type
#' }
#'
#' @examples
#' \dontrun{
#' config <- list(
#'   api_key = "your_api_key",
#'   url = "https://api.stlouisfed.org/fred/series",
#'   mode = "json"
#' )
#' get_source_metadata_fred("AMERIBOR", config)
#' }
#'
#' @export
get_source_metadata_fred <- function(series_id, config = NULL) {
  config <- .get_api_config("fred", config = config)
  api_key <- config$api_key
  url <- config$url
  mode <- config$mode
  query <- .fred_api_query(series_id, api_key, mode)
  data <- .fetch_fred_json(url, query = query)
  
  res <- data$seriess
  list(
    title = res$title,
    start = res$observation_start,
    end = res$observation_end,
    freq = res$frequency,
    units = res$units,
    season = res$seasonal_adjustment
  )
}

#' Get Local FRED Data
#'
#' @param series_id FRED series identifier.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_FRED_data <- function(series_id, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("fred")
  }

  .read_local_data_table(file.path(local_path, paste0(series_id, ".rds")), sort_cols = "date")
}

#' Get FRED Registry File Path
#'
#' Resolve the JSON registry path for FRED metadata. If no explicit
#' `registry_file` is configured, the function falls back to a default filename
#' in the package config directory.
#'
#' @param config_dir Optional configuration directory used for the fallback
#'   registry path.
#'
#' @return Character scalar path.
#' @export
get_fred_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("fred"), error = function(e) list())
  registry_file <- cfg$registry_file

  if (is.null(registry_file) || !nzchar(registry_file)) {
    if (is.null(config_dir)) {
      config_dir <- getOption("investdatar.config_dir")
    }
    if (is.null(config_dir) || !nzchar(config_dir)) {
      stop(
        "No FRED registry path is configured. Set FRED.registry_file in your ",
        "config or load a config file rooted at the desired directory."
      )
    }
    return(file.path(config_dir, "fred_macro_series_registry.json"))
  }

  .normalize_scalar_path(registry_file, config_dir = getOption("investdatar.config_dir"))
}

#' Get FRED Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return `data.table`.
#' @export
get_fred_registry <- function(registry_path = get_fred_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("series_id", "main_group", "title", "start", "end", "freq", "units", "season", "update_time")
  )
}

#' Add Or Update One FRED Registry Entry
#'
#' @param series_id FRED series identifier.
#' @param main_group Optional grouping label. If `NULL`, read one line from
#'   stdin after showing existing `main_group` hints.
#' @param registry_path Optional registry JSON path.
#' @param config Optional FRED API config.
#'
#' @return The added or updated row as a `data.table`.
#' @export
add_fred_registry_series <- function(series_id, main_group = NULL,
                                     registry_path = get_fred_registry_file_path(),
                                     config = NULL) {
  registry <- .read_json_registry(
    registry_path,
    empty_cols = c("series_id", "main_group", "title", "start", "end", "freq", "units", "season", "update_time")
  )
  template_names <- names(registry)
  existing_groups <- sort(unique(stats::na.omit(registry$main_group)))

  if (is.null(main_group) || !nzchar(main_group)) {
    main_group <- .prompt_stdin_value(
      sprintf("Enter main_group for FRED series '%s': ", series_id),
      hints = existing_groups
    )
  }
  if (!nzchar(main_group)) {
    stop("main_group must be a non-empty string.")
  }

  if (!(main_group %in% existing_groups) && length(existing_groups) > 0L) {
    confirmed <- .confirm_stdin(sprintf("main_group '%s' is new. Add it? [y/N]: ", main_group))
    if (!isTRUE(confirmed)) {
      stop("Aborted by user.")
    }
  }

  metadata <- get_source_metadata_fred(series_id, config = config)
  if (is.null(metadata$title) || !nzchar(metadata$title)) {
    stop("Failed to retrieve FRED metadata for series_id: ", series_id)
  }

  new_row <- data.table::data.table(
    series_id = series_id,
    main_group = main_group,
    title = metadata$title,
    start = as.character(metadata$start),
    end = as.character(metadata$end),
    freq = metadata$freq,
    units = metadata$units,
    season = metadata$season,
    update_time = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  )
  new_row <- .align_registry_schema(new_row, template_names)

  if (nrow(registry) > 0L && any(registry$series_id == series_id)) {
    series_id_value <- series_id
    registry <- registry[series_id != series_id_value]
  }
  registry <- data.table::rbindlist(list(registry, new_row), use.names = TRUE, fill = TRUE)
  data.table::setorderv(registry, "series_id")
  .write_json_registry(registry, registry_path)

  series_id_value <- series_id
  registry[series_id == series_id_value]
}

#' Synchronize Local FRED Data
#'
#' @param series_id FRED series identifier.
#' @param config Optional FRED API config.
#' @param local_path Optional local storage path.
#' @param from_server Logical. If `TRUE`, use the FRED server-reported update
#'   time. Otherwise use a frequency-based fallback.
#' @param tz Time zone used for source update time inference.
#'
#' @return A sync result list.
#' @export
sync_local_fred_data <- function(series_id, config = NULL, local_path = NULL,
                                 from_server = FALSE, tz = "America/Chicago") {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("fred", create = TRUE)
  }

  local_file_path <- file.path(local_path, paste0(series_id, ".rds"))
  source_utime <- get_source_utime_fred(series_id, config = config, from_server = from_server, tz = tz)
  new_dt <- get_source_data_fred(series_id, config = config)

  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = "date",
    order_cols = "date",
    source_utime = source_utime
  )
}

#' Synchronize All FRED Series In The Registry
#'
#' @param registry Optional FRED registry table.
#' @param config Optional FRED API config.
#' @param local_path Optional local storage path.
#' @param from_server Logical. If `TRUE`, use FRED server-reported update time.
#' @param tz Time zone used for source update time inference.
#'
#' @return Summary `data.table`.
#' @export
sync_all_fred_registry_data <- function(registry = get_fred_registry(), config = NULL,
                                        local_path = NULL, from_server = FALSE,
                                        tz = "America/Chicago") {
  stopifnot("series_id" %in% names(registry))
  if (is.null(local_path)) {
    local_path <- get_source_data_path("fred", create = TRUE)
  }
  run_started_at <- Sys.time()

  summary_list <- lapply(registry$series_id, function(series_id) {
    tryCatch(
      {
        res <- sync_local_fred_data(
          series_id = series_id,
          config = config,
          local_path = local_path,
          from_server = from_server,
          tz = tz
        )
        data.table::data.table(
          series_id = series_id,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          series_id = series_id,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          error = conditionMessage(e),
          error_class = class(e)[[1L]],
          http_status = if (inherits(e, "investdatar_fred_api_error") || inherits(e, "investdatar_http_error")) {
            as.integer(e$status_code)
          } else {
            NA_integer_
          }
        )
      }
    )
  })

  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE),
    source_id = "fred",
    run_started_at = run_started_at,
    run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "fred",
    summary = summary_dt,
    local_path = local_path,
    params = list(from_server = from_server, tz = tz),
    run_started_at = run_started_at,
    run_finished_at = run_finished_at
  )
  summary_dt
}

#' Detect Gaps In Local FRED Data
#'
#' @param x A FRED `data.table`, or a series id.
#' @param frequency Optional explicit frequency string.
#' @param local_path Optional local storage path if `x` is a series id.
#'
#' @return A `data.table` of gaps.
#' @export
detect_time_gaps_fred <- function(x, frequency = NULL, local_path = NULL) {
  if (is.character(x) && length(x) == 1L) {
    series_id <- x
    dt <- get_local_FRED_data(series_id, local_path = local_path)
    if (is.null(frequency)) {
      registry <- tryCatch(get_fred_registry(), error = function(e) NULL)
      if (!is.null(registry)) {
        series_id_value <- x
        frequency <- registry[series_id == series_id_value, freq][[1]]
      }
    }
  } else {
    dt <- .as_data_table(x)
  }

  if (is.null(frequency) || !nzchar(frequency)) {
    stop("frequency is required when it cannot be inferred from the FRED registry.")
  }

  detect_time_gaps(dt, time_col = "date", frequency = frequency)
}
