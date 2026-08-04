.standardize_bea_regional <- function(data, series_id, label) {
  dt <- data.table::as.data.table(data)
  if (!nrow(dt)) return(data.table::data.table())
  required <- c("GeoFIPS", "GeoName", "TimePeriod", "DataValue")
  if (length(setdiff(required, names(dt)))) stop("BEA Regional response is missing required fields.", call. = FALSE)
  value <- suppressWarnings(as.numeric(gsub(",", "", dt$DataValue, fixed = TRUE)))
  out <- data.table::data.table(
    source = "bea", series_id = series_id, label = label,
    geo_id = trimws(as.character(dt$GeoFIPS)), geo_name = trimws(as.character(dt$GeoName)),
    period = as.character(dt$TimePeriod), date = as.Date(paste0(substr(dt$TimePeriod, 1L, 4L), "-01-01")),
    value = value, unit = as.character(dt$CL_UNIT), unit_multiplier = suppressWarnings(as.integer(dt$UNIT_MULT))
  )
  data.table::setorderv(out, c("date", "geo_id")); out[]
}

#' Retrieve BEA Regional Data
#' @param series_id Stable local series identifier.
#' @param table_name BEA Regional table name.
#' @param line_code BEA line code.
#' @param geofips Geography selector such as `STATE` or `COUNTY`.
#' @param year Year selector, default `ALL`.
#' @param label Optional local label.
#' @param config Optional BEA configuration.
#' @return A standardized regional panel.
#' @export
get_source_data_bea <- function(series_id, table_name, line_code, geofips = "STATE", year = "ALL", label = NULL, config = NULL) {
  config <- .get_api_config("bea", config = config)
  if (is.null(config$api_key) || !nzchar(config$api_key)) stop("BEA API key is missing. Set BEA_API_KEY or BEA.api_key.", call. = FALSE)
  response <- .http_get_json(config$url, query = list(
    UserID = config$api_key, method = "GetData", datasetname = "Regional", TableName = table_name,
    LineCode = line_code, GeoFIPS = geofips, Year = year, ResultFormat = "JSON"
  ))
  error <- response$BEAAPI$Error$APIErrorDescription %||% response$BEAAPI$Results$Error$APIErrorDescription
  if (!is.null(error)) stop("BEA API error: ", error, call. = FALSE)
  .standardize_bea_regional(response$BEAAPI$Results$Data, series_id, label %||% series_id)
}

.bea_local_file <- function(series_id, local_path) file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]", "_", series_id), ".rds"))

#' Read Local BEA Data
#' @param series_id Registered series identifier.
#' @param local_path Optional BEA storage directory.
#' @return A `data.table`, or `NULL`.
#' @export
get_local_bea_data <- function(series_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("bea")
  .read_local_data_table(.bea_local_file(series_id, local_path), sort_cols = c("date", "geo_id"))
}

#' Synchronize One BEA Regional Series
#' @inheritParams get_source_data_bea
#' @param local_path Optional BEA storage directory.
#' @return A standard synchronization result.
#' @export
sync_local_bea_data <- function(series_id, table_name, line_code, geofips = "STATE", year = "ALL", label = NULL,
                                config = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("bea", create = TRUE)
  new <- get_source_data_bea(series_id, table_name, line_code, geofips, year, label, config)
  sync_local_data(new, .bea_local_file(series_id, local_path), c("series_id", "geo_id", "period"),
                  c("date", "geo_id"), if (nrow(new)) max(new$date) else NULL)
}

#' Get BEA Registry File Path
#' @param config_dir Optional configuration directory.
#' @return Character scalar path.
#' @export
get_bea_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("bea"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) return(.normalize_scalar_path(cfg$registry_file, getOption("investdatar.config_dir")))
  config_dir <- config_dir %||% getOption("investdatar.config_dir"); if (is.null(config_dir)) stop("No BEA registry path is configured.", call. = FALSE)
  file.path(config_dir, "bea_series_registry.json")
}

#' Get BEA Series Registry
#' @param registry_path Optional registry path.
#' @return A registry `data.table`.
#' @export
get_bea_registry <- function(registry_path = get_bea_registry_file_path()) .read_json_registry(registry_path, c("series_id", "table_name", "line_code", "geofips", "year", "label", "active"))

#' Synchronize Registered BEA Series
#' @param registry Optional BEA registry.
#' @param config Optional BEA configuration.
#' @param local_path Optional storage directory.
#' @return A batch summary.
#' @export
sync_all_bea_registry_data <- function(registry = get_bea_registry(), config = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("bea", create = TRUE)
  started <- Sys.time(); registry <- registry[tolower(as.character(registry$active)) %in% c("true", "1", "yes", "y")]
  rows <- lapply(seq_len(nrow(registry)), function(i) tryCatch({
    res <- sync_local_bea_data(registry$series_id[[i]], registry$table_name[[i]], registry$line_code[[i]], registry$geofips[[i]], registry$year[[i]], registry$label[[i]], config, local_path)
    data.table::data.table(series_id = registry$series_id[[i]], status = "success", updated = isTRUE(res$updated), n_rows = res$n_rows, n_new_rows = res$n_new_rows)
  }, error = function(e) data.table::data.table(series_id = registry$series_id[[i]], status = "error", updated = FALSE, error = conditionMessage(e))))
  finished <- Sys.time(); summary <- .normalize_sync_summary(data.table::rbindlist(rows, fill = TRUE), "bea", started, finished)
  .write_sync_run_log("bea", summary, local_path, list(), started, finished); summary
}

#' Describe Local BEA Data
#' @inheritParams get_local_bea_data
#' @return Character scalar narrative.
#' @export
describe_bea_data <- function(series_id, local_path = NULL) {
  dt <- get_local_bea_data(series_id, local_path)
  if (is.null(dt) || !nrow(dt)) stop("Local BEA data not found: ", series_id, call. = FALSE)
  paste(sprintf("BEA Regional series %s contains %s observations across %s geographies.", series_id, nrow(dt), data.table::uniqueN(dt$geo_id)), .describe_time_coverage(dt$date), .describe_value_summary(dt$value))
}
