#' Get FRED Series Data
#'
#' Download a FRED series and return dates and numeric values.
#'
#' @param series_id Character. FRED series ID (e.g., "DGS10").
#' @param config List with fields: api_key, url, mode.
#'
#' @return data.table with columns: date (Date), value (numeric, NA for ".").
#' @export
get_source_data_fred <- function(series_id, config) {
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
  
  data.table::data.table(
    date = as.Date(data$observations$date),
    value = numeric_values,
    stringsAsFactors = FALSE
  )
}

#' Get FRED Series Last Update Time
#'
#' Return the last update time of a FRED series as POSIXct in UTC.
#'
#' @param series_id Character. FRED series ID.
#' @param config List with fields: api_key, url, mode.
#'
#' @return POSIXct (UTC).
#' @export
get_source_utime_fred <- function(series_id, config, from_server = FALSE, tz = "America/Chicago") {
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
    now <- Sys.time()
    latest_midnight_in_tz_string <- format(now, "%Y-%m-%d 00:00:00", tz = tz)
    out <- as.POSIXct(latest_midnight_in_tz_string, tz = tz)
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
get_source_metadata_fred <- function(series_id, config) {
  api_key <- config$api_key
  url <- config$url
  mode <- config$mode
  
  url <- sprintf("%s?series_id=%s&api_key=%s&file_type=%s", url, series_id, api_key, mode)
  res <- curl::curl_fetch_memory(url)
  data <- jsonlite::fromJSON(rawToChar(res$content), simplifyVector = TRUE)
  
  res <- data$seriess
  list(title = res$title, start = res$observation_start, end = res$observation_end, freq = res$frequency, units = res$units, season = res$seasonal_adjustment)
}

#' @export
download_fred_from_vm <- function(vm_name, vm_zone, series_id = NULL, fred_data_path = Sys.getenv("FRED_Data_Path")) {
  if (is.null(series_id)) {
    system(sprintf("gcloud compute scp --recurse %s:%s/* %s --zone=%s", 
      vm_name, fred_data_path, fred_data_path, vm_zone))
  } else {
    system(sprintf("gcloud compute scp %s:%s/%s.rds %s --zone=%s", 
      vm_name, fred_data_path, series_id, fred_data_path, vm_zone))
  }
}

#' @export
upload_fred_to_vm <- function(vm_name, vm_zone, series_id = NULL, fred_data_path = Sys.getenv("FRED_Data_Path")) {
  if (is.null(series_id)) {
    system(sprintf("gcloud compute scp --recurse %s/* %s:%s --zone=%s", 
      fred_data_path, vm_name, fred_data_path, vm_zone))
  } else {
    system(sprintf("gcloud compute scp %s/%s.rds %s:%s --zone=%s", 
      fred_data_path, series_id, vm_name, fred_data_path, vm_zone))
  }
}

