.census_response_table <- function(response) {
  if (is.null(response) || length(response) < 2L) return(data.table::data.table())
  matrix <- if (is.matrix(response) || is.data.frame(response)) as.matrix(response) else do.call(rbind, lapply(response, as.character))
  out <- data.table::as.data.table(matrix[-1L, , drop = FALSE])
  data.table::setnames(out, matrix[1L, ])
  out
}

.census_period_date <- function(period) {
  period <- as.character(period)
  out <- as.Date(rep(NA_character_, length(period)))
  monthly <- grepl("^[0-9]{4}-[0-9]{2}$", period)
  if (any(monthly)) out[monthly] <- as.Date(paste0(period[monthly], "-01"))
  annual <- grepl("^[0-9]{4}$", period)
  if (any(annual)) out[annual] <- as.Date(paste0(period[annual], "-01-01"))
  out
}

#' Retrieve A Selected Census Economic Indicator Series
#' @param series_id Stable local series identifier.
#' @param dataset Economic Indicators dataset suffix, such as `marts`.
#' @param data_type_code Census item type code.
#' @param category_code Census industry/category code.
#' @param seasonally_adj Seasonal adjustment value.
#' @param from,to Optional time bounds.
#' @param label Optional local label.
#' @param config Optional Census configuration.
#' @return A standardized long `data.table`.
#' @export
get_source_data_census <- function(series_id, dataset, data_type_code, category_code,
                                   seasonally_adj = "yes", from = NULL, to = NULL,
                                   label = NULL, config = NULL) {
  config <- .get_api_config("census", config = config)
  if (is.null(config$api_key) || !nzchar(config$api_key)) stop("Census API key is missing. Set CENSUS_API_KEY or Census.api_key.", call. = FALSE)
  time <- if (is.null(from)) "from 2010" else paste("from", as.character(from))
  if (!is.null(to)) time <- paste(time, "to", as.character(to))
  url <- paste0(sub("/+$", "", config$url), "/", dataset)
  response <- .http_get_json(url, query = list(
    get = "cell_value,data_type_code,time_slot_id,category_code,seasonally_adj,error_data",
    time = time, key = config$api_key
  ))
  dt <- .census_response_table(response)
  if (!nrow(dt)) return(dt)
  requested_type <- as.character(data_type_code)
  requested_category <- as.character(category_code)
  requested_adjustment <- tolower(as.character(seasonally_adj))
  dt <- dt[data_type_code == requested_type & category_code == requested_category &
             tolower(seasonally_adj) == requested_adjustment]
  out <- data.table::data.table(
    source = "census", series_id = series_id, label = label %||% series_id,
    dataset = dataset, period = as.character(dt$time), date = .census_period_date(dt$time),
    value = suppressWarnings(as.numeric(gsub(",", "", dt$cell_value, fixed = TRUE))),
    data_type_code = as.character(dt$data_type_code), category_code = as.character(dt$category_code),
    seasonally_adjusted = tolower(as.character(dt$seasonally_adj)) == "yes", error_data = as.character(dt$error_data)
  )
  data.table::setorderv(out, "date"); out[]
}

.census_local_file <- function(series_id, local_path) file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]", "_", series_id), ".rds"))

#' Read Local Census Data
#' @param series_id Registered series identifier.
#' @param local_path Optional Census storage directory.
#' @return A `data.table`, or `NULL`.
#' @export
get_local_census_data <- function(series_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("census")
  .read_local_data_table(.census_local_file(series_id, local_path), sort_cols = "date")
}

#' Synchronize One Census Series
#' @inheritParams get_source_data_census
#' @param local_path Optional Census storage directory.
#' @param overlap_months Months re-fetched for revisions.
#' @return A standard synchronization result.
#' @export
sync_local_census_data <- function(series_id, dataset, data_type_code, category_code,
                                   seasonally_adj = "yes", from = NULL, to = NULL, label = NULL,
                                   config = NULL, local_path = NULL, overlap_months = 24L) {
  if (is.null(local_path)) local_path <- get_source_data_path("census", create = TRUE)
  file <- .census_local_file(series_id, local_path); old <- .safe_read_rds(file, NULL); sync_from <- from
  if (!is.null(old) && nrow(old)) sync_from <- format(seq(max(old$date), by = paste0("-", overlap_months, " months"), length.out = 2L)[[2L]], "%Y-%m")
  new <- get_source_data_census(series_id, dataset, data_type_code, category_code, seasonally_adj, sync_from, to, label, config)
  sync_local_data(new, file, c("series_id", "period"), "date", if (nrow(new)) max(new$date) else NULL)
}

#' Get Census Registry File Path
#' @param config_dir Optional configuration directory.
#' @return Character scalar path.
#' @export
get_census_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("census"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) return(.normalize_scalar_path(cfg$registry_file, getOption("investdatar.config_dir")))
  config_dir <- config_dir %||% getOption("investdatar.config_dir"); if (is.null(config_dir)) stop("No Census registry path is configured.", call. = FALSE)
  file.path(config_dir, "census_series_registry.json")
}

#' Get Census Series Registry
#' @param registry_path Optional registry path.
#' @return A registry `data.table`.
#' @export
get_census_registry <- function(registry_path = get_census_registry_file_path()) .read_json_registry(registry_path, c("series_id", "dataset", "data_type_code", "category_code", "seasonally_adj", "start", "label", "active"))

#' Synchronize Registered Census Series
#' @param registry Optional Census registry.
#' @param config Optional Census configuration.
#' @param local_path Optional storage directory.
#' @return A batch summary.
#' @export
sync_all_census_registry_data <- function(registry = get_census_registry(), config = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("census", create = TRUE)
  started <- Sys.time(); registry <- registry[tolower(as.character(registry$active)) %in% c("true", "1", "yes", "y")]
  rows <- lapply(seq_len(nrow(registry)), function(i) tryCatch({
    res <- sync_local_census_data(registry$series_id[[i]], registry$dataset[[i]], registry$data_type_code[[i]], registry$category_code[[i]], registry$seasonally_adj[[i]], registry$start[[i]], label = registry$label[[i]], config = config, local_path = local_path)
    data.table::data.table(series_id = registry$series_id[[i]], status = "success", updated = isTRUE(res$updated), n_rows = res$n_rows, n_new_rows = res$n_new_rows)
  }, error = function(e) data.table::data.table(series_id = registry$series_id[[i]], status = "error", updated = FALSE, error = conditionMessage(e))))
  finished <- Sys.time(); summary <- .normalize_sync_summary(data.table::rbindlist(rows, fill = TRUE), "census", started, finished)
  .write_sync_run_log("census", summary, local_path, list(), started, finished); summary
}

#' Describe Local Census Data
#' @inheritParams get_local_census_data
#' @return Character scalar narrative.
#' @export
describe_census_data <- function(series_id, local_path = NULL) {
  dt <- get_local_census_data(series_id, local_path)
  if (is.null(dt) || !nrow(dt)) stop("Local Census data not found: ", series_id, call. = FALSE)
  paste(sprintf("Census Economic Indicators series %s contains %s observations.", series_id, nrow(dt)), .describe_time_coverage(dt$date), .describe_value_summary(dt$value))
}
