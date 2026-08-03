.eia_series_url <- function(series_id, config) {
  paste0(sub("/+$", "", config$url), "/", utils::URLencode(series_id, reserved = TRUE))
}

.eia_period_date <- function(period) {
  period <- as.character(period)
  out <- rep(as.Date(NA), length(period))
  full_date <- grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", period)
  if (any(full_date)) out[full_date] <- as.Date(substr(period[full_date], 1L, 10L))
  monthly <- grepl("^[0-9]{4}-[0-9]{2}$", period)
  if (any(monthly)) out[monthly] <- as.Date(paste0(period[monthly], "-01"))
  quarterly <- grepl("^[0-9]{4}-Q[1-4]$", period)
  if (any(quarterly)) {
    year <- as.integer(substr(period[quarterly], 1L, 4L))
    quarter <- as.integer(substr(period[quarterly], 7L, 7L))
    out[quarterly] <- as.Date(sprintf("%04d-%02d-01", year, (quarter - 1L) * 3L + 1L))
  }
  annual <- grepl("^[0-9]{4}$", period)
  if (any(annual)) out[annual] <- as.Date(paste0(period[annual], "-01-01"))
  out
}

.standardize_eia_data <- function(data, series_id, response_meta = list(), label = NULL) {
  dt <- data.table::as.data.table(data)
  if (nrow(dt) == 0L) {
    return(data.table::data.table(
      source = character(), series_id = character(), label = character(),
      frequency = character(), period = character(), date = as.Date(character()),
      datetime = .empty_posixct(), value = numeric(), unit = character(),
      description = character()
    ))
  }
  if (!all(c("period", "value") %in% names(dt))) {
    stop("EIA response must contain period and value columns.", call. = FALSE)
  }
  frequency <- response_meta$frequency
  if (is.null(frequency)) frequency <- if ("frequency" %in% names(dt)) dt$frequency[[1L]] else NA_character_
  description <- if ("seriesDescription" %in% names(dt)) dt$seriesDescription else if ("series_description" %in% names(dt)) dt$series_description else NA_character_
  unit <- if ("unit" %in% names(dt)) dt$unit else rep(NA_character_, nrow(dt))
  datetime <- .empty_posixct(nrow(dt))
  hourly <- grepl("T[0-9]{2}", dt$period)
  if (any(hourly)) {
    datetime[hourly] <- as.POSIXct(strptime(dt$period[hourly], format = "%Y-%m-%dT%H", tz = "UTC"))
  }
  out <- data.table::data.table(
    source = "eia",
    series_id = as.character(series_id),
    label = if (is.null(label) || !nzchar(label)) description else as.character(label),
    frequency = as.character(frequency),
    period = as.character(dt$period),
    date = .eia_period_date(dt$period),
    datetime = datetime,
    value = suppressWarnings(as.numeric(dt$value)),
    unit = as.character(unit),
    description = as.character(description)
  )
  data.table::setorderv(out, "period")
  unique(out, by = c("series_id", "period"))
}

.eia_check_response <- function(response) {
  if (!is.null(response$error)) {
    message <- response$error$message
    if (is.null(message)) message <- jsonlite::toJSON(response$error, auto_unbox = TRUE)
    stop("EIA API error: ", message, call. = FALSE)
  }
  if (is.null(response$response)) stop("EIA response does not contain a response object.", call. = FALSE)
  response$response
}

#' Retrieve One EIA Series
#'
#' @param series_id EIA series identifier.
#' @param label Optional local label.
#' @param config Optional EIA API configuration.
#' @param from,to Optional inclusive period bounds.
#' @param page_size Number of observations requested per page.
#' @param max_pages Optional page limit for diagnostics.
#'
#' @return A standardized long `data.table`.
#' @export
get_source_data_eia <- function(series_id, label = NULL, config = NULL,
                                from = NULL, to = NULL, page_size = 5000L,
                                max_pages = Inf) {
  config <- .get_api_config("eia", config = config)
  if (is.null(config$api_key) || !nzchar(config$api_key)) {
    stop("EIA API key is missing. Set EIA_API_KEY or EIA.api_key in the package config.", call. = FALSE)
  }
  page_size <- max(1L, min(as.integer(page_size), 5000L))
  offset <- 0L
  page <- 1L
  pages <- list()
  response_meta <- list()
  repeat {
    query <- list(
      api_key = config$api_key,
      offset = offset,
      length = page_size,
      `sort[0][column]` = "period",
      `sort[0][direction]` = "asc"
    )
    if (!is.null(from)) query$start <- as.character(from)
    if (!is.null(to)) query$end <- as.character(to)
    response <- .eia_check_response(.http_get_json(.eia_series_url(series_id, config), query = query))
    page_dt <- data.table::as.data.table(response$data)
    if (length(response_meta) == 0L) response_meta <- response
    if (nrow(page_dt) == 0L) break
    pages[[length(pages) + 1L]] <- page_dt
    total <- suppressWarnings(as.integer(response$total))
    if (is.na(total) || offset + nrow(page_dt) >= total || page >= max_pages) break
    offset <- offset + nrow(page_dt)
    page <- page + 1L
  }
  combined <- if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
  .standardize_eia_data(combined, series_id = series_id, response_meta = response_meta, label = label)
}

#' Get Latest EIA Series Update Date
#'
#' @param series_id EIA series identifier.
#' @param config Optional EIA API configuration.
#'
#' @return A UTC `POSIXct`, or `NULL`.
#' @export
get_source_utime_eia <- function(series_id, config = NULL) {
  config <- .get_api_config("eia", config = config)
  if (is.null(config$api_key) || !nzchar(config$api_key)) stop("EIA API key is missing. Set EIA_API_KEY or EIA.api_key in the package config.", call. = FALSE)
  response <- .eia_check_response(.http_get_json(
    .eia_series_url(series_id, config),
    query = list(api_key = config$api_key, offset = 0L, length = 1L, `sort[0][column]` = "period", `sort[0][direction]` = "desc")
  ))
  if (is.null(response$data) || length(response$data) == 0L) return(NULL)
  period <- data.table::as.data.table(response$data)$period[[1L]]
  as.POSIXct(.eia_period_date(period), tz = "UTC")
}

.eia_local_file <- function(series_id, local_path) {
  file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]+", "_", series_id), ".rds"))
}

#' Read Local EIA Data
#'
#' @param series_id EIA series identifier.
#' @param local_path Optional EIA storage directory.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_eia_data <- function(series_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("eia")
  .read_local_data_table(.eia_local_file(series_id, local_path), sort_cols = "period")
}

#' Synchronize One EIA Series
#'
#' @inheritParams get_source_data_eia
#' @param local_path Optional EIA storage directory.
#' @param overlap_days Days re-fetched around the latest local observation.
#'
#' @return A local synchronization result list.
#' @export
sync_local_eia_data <- function(series_id, label = NULL, config = NULL,
                                from = NULL, to = NULL, local_path = NULL,
                                overlap_days = 31L, page_size = 5000L) {
  if (is.null(local_path)) local_path <- get_source_data_path("eia", create = TRUE)
  local_file <- .eia_local_file(series_id, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- if (is.null(from)) NULL else as.Date(from)
  if (!is.null(local_dt) && nrow(local_dt) > 0L && "date" %in% names(local_dt)) {
    overlap_from <- max(local_dt$date, na.rm = TRUE) - as.integer(overlap_days)
    sync_from <- if (is.null(sync_from)) overlap_from else max(sync_from, overlap_from)
  }
  new_dt <- get_source_data_eia(
    series_id = series_id, label = label, config = config,
    from = sync_from, to = to, page_size = page_size
  )
  source_utime <- tryCatch(get_source_utime_eia(series_id, config = config), error = function(e) NULL)
  sync_local_data(
    new_data = new_dt, local_file_path = local_file,
    key_cols = c("series_id", "period"), order_cols = "period",
    source_utime = source_utime
  )
}

#' Get EIA Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback.
#'
#' @return Character scalar path.
#' @export
get_eia_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("eia"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) return(.normalize_scalar_path(cfg$registry_file, config_dir = getOption("investdatar.config_dir")))
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) stop("No EIA registry path is configured. Set EIA.registry_file in your config.", call. = FALSE)
  file.path(config_dir, "eia_series_registry.json")
}

#' Get EIA Series Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return A registry `data.table`.
#' @export
get_eia_registry <- function(registry_path = get_eia_registry_file_path()) {
  .read_json_registry(registry_path, empty_cols = c("series_id", "label", "main_group", "frequency", "active"))
}

#' Synchronize All Registered EIA Series
#'
#' @param registry Optional EIA registry table.
#' @param config Optional EIA API configuration.
#' @param local_path Optional EIA storage directory.
#' @param ... Passed to `sync_local_eia_data()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_eia_registry_data <- function(registry = get_eia_registry(), config = NULL, local_path = NULL, ...) {
  stopifnot("series_id" %in% names(registry))
  if (is.null(local_path)) local_path <- get_source_data_path("eia", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry$active))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }
  rows <- lapply(seq_len(nrow(registry)), function(i) {
    series_id <- registry$series_id[[i]]
    label <- if ("label" %in% names(registry)) registry$label[[i]] else NULL
    tryCatch({
      res <- sync_local_eia_data(series_id = series_id, label = label, config = config, local_path = local_path, ...)
      data.table::data.table(
        series_id = series_id, status = "success", updated = isTRUE(res$updated),
        n_rows = if (is.null(res$n_rows)) NA_integer_ else res$n_rows,
        n_new_rows = if (is.null(res$n_new_rows)) NA_integer_ else res$n_new_rows,
        error = NA_character_
      )
    }, error = function(e) data.table::data.table(
      series_id = series_id, status = "error", updated = FALSE,
      n_rows = NA_integer_, n_new_rows = NA_integer_, error = conditionMessage(e),
      error_class = class(e)[[1L]], http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
    ))
  })
  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(rows, use.names = TRUE, fill = TRUE),
    source_id = "eia", run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "eia", summary = summary_dt, local_path = local_path,
    params = list(), run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  summary_dt
}

#' Describe Local EIA Data
#'
#' @param series_id EIA series identifier.
#' @param local_path Optional EIA storage directory.
#'
#' @return Character scalar narrative.
#' @export
describe_eia_data <- function(series_id, local_path = NULL) {
  dt <- get_local_eia_data(series_id, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local EIA data not found for series_id: ", series_id, call. = FALSE)
  paste(
    sprintf("This object is a data.table for EIA series %s.", series_id),
    sprintf("Series label: %s.", unique(stats::na.omit(dt$label))[[1L]]),
    sprintf("Frequency: %s. Unit: %s.", unique(stats::na.omit(dt$frequency))[[1L]], unique(stats::na.omit(dt$unit))[[1L]]),
    sprintf("The table contains %s rows and %s missing values.", nrow(dt), sum(is.na(dt$value))),
    .describe_time_coverage(dt$date),
    .describe_value_summary(dt$value)
  )
}
