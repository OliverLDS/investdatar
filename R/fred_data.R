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
  url <- config$url
  mode <- config$mode
  url <- sprintf("%s/observations?series_id=%s&api_key=%s&file_type=%s", url, series_id, api_key, mode)
  
  # data <- jsonlite::fromJSON(url) curl is more stable sometimes
  res <- curl::curl_fetch_memory(url)
  data <- jsonlite::fromJSON(rawToChar(res$content), simplifyVector = TRUE)
  
  # contains "." in early GDP
  raw_values <- data$observations$value
  raw_values[raw_values == "."] <- NA_character_ # because the returned value is string
  numeric_values <- as.numeric(raw_values)
  
  out <- data.table::data.table(
    date = as.Date(data$observations$date),
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
    
    url <- sprintf("%s?series_id=%s&api_key=%s&file_type=%s", url, series_id, api_key, mode)
    # data <- jsonlite::fromJSON(url)
    res <- curl::curl_fetch_memory(url)
    data <- jsonlite::fromJSON(rawToChar(res$content), simplifyVector = TRUE)
    
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
  
  url <- sprintf("%s?series_id=%s&api_key=%s&file_type=%s", url, series_id, api_key, mode)
  res <- curl::curl_fetch_memory(url)
  data <- jsonlite::fromJSON(rawToChar(res$content), simplifyVector = TRUE)
  
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
  registry <- jsonlite::fromJSON(registry_path, simplifyDataFrame = TRUE)
  data.table::as.data.table(registry)
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
