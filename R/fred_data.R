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
get_source_utime_fred <- function(series_id, config) {
  api_key <- config$api_key
  url <- config$url
  mode <- config$mode
  
  url <- sprintf("%s?series_id=%s&api_key=%s&file_type=%s", url, series_id, api_key, mode)
  data <- jsonlite::fromJSON(url)
  
  update_time_str <- data$seriess$last_updated # UTC+5 by defult
  as.POSIXct(update_time_str, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
}

