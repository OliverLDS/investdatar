.sdmx_registry_vector <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) return(NULL)
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  x <- trimws(unlist(strsplit(as.character(x), ",", fixed = TRUE), use.names = FALSE))
  x[nzchar(x)]
}

.sdmx_build_url <- function(provider, base_url, agency = NULL, dataflow,
                            version = "latest", key = NULL, flow_ref = NULL) {
  provider <- tolower(provider)
  base_url <- sub("/+$", "", base_url)
  key <- if (is.null(key) || !nzchar(key)) "" else key
  if (provider == "bis") {
    if (is.null(agency) || !nzchar(agency)) stop("BIS SDMX entries require agency.", call. = FALSE)
    return(paste(base_url, "data", "dataflow", agency, dataflow, version, key, sep = "/"))
  }
  if (is.null(flow_ref) || !nzchar(flow_ref)) {
    flow_ref <- if (!is.null(agency) && nzchar(agency)) {
      paste(agency, dataflow, version, sep = ",")
    } else {
      dataflow
    }
  }
  paste(base_url, "data", flow_ref, key, sep = "/")
}

.sdmx_period_date <- function(period) {
  period <- as.character(period)
  out <- rep(as.Date(NA), length(period))
  daily <- grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", period)
  out[daily] <- as.Date(period[daily])
  monthly <- grepl("^[0-9]{4}-[0-9]{2}$", period)
  if (any(monthly)) out[monthly] <- as.Date(paste0(period[monthly], "-01"))
  quarterly <- grepl("^[0-9]{4}-Q[1-4]$", period)
  if (any(quarterly)) {
    year <- as.integer(substr(period[quarterly], 1L, 4L))
    quarter <- as.integer(substr(period[quarterly], 7L, 7L))
    out[quarterly] <- as.Date(sprintf("%04d-%02d-01", year, 3L * quarter - 2L))
  }
  semiannual <- grepl("^[0-9]{4}-S[1-2]$", period)
  if (any(semiannual)) {
    year <- as.integer(substr(period[semiannual], 1L, 4L))
    half <- as.integer(substr(period[semiannual], 7L, 7L))
    out[semiannual] <- as.Date(sprintf("%04d-%02d-01", year, 6L * half - 5L))
  }
  annual <- grepl("^[0-9]{4}$", period)
  if (any(annual)) out[annual] <- as.Date(paste0(period[annual], "-01-01"))
  weekly <- grepl("^[0-9]{4}-W[0-9]{2}$", period)
  if (any(weekly)) {
    out[weekly] <- as.Date(strptime(paste0(period[weekly], "-1"), "%G-W%V-%u", tz = "UTC"))
  }
  out
}

.read_sdmx_csv <- function(response) {
  text <- .http_response_text(response)
  if (!nzchar(trimws(text))) return(data.table::data.table())
  dt <- data.table::as.data.table(utils::read.csv(
    text = text, check.names = FALSE, stringsAsFactors = FALSE,
    na.strings = c("", "NA", "NaN")
  ))
  if (length(names(dt)) > 0L) data.table::setnames(dt, sub("^\\ufeff", "", names(dt)))
  dt
}

.standardize_sdmx_data <- function(data, series_id, provider, key,
                                   time_col = "TIME_PERIOD", value_col = "OBS_VALUE",
                                   dimension_cols = NULL, label = NULL, frequency = NULL) {
  dt <- data.table::as.data.table(data)
  empty <- data.table::data.table(
    source = character(), provider = character(), series_id = character(),
    label = character(), frequency = character(), period = character(),
    date = as.Date(character()), value = numeric(), dimension_key = character()
  )
  if (nrow(dt) == 0L) return(empty)
  required <- c(time_col, value_col)
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols) > 0L) {
    stop("SDMX response is missing column(s): ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }
  dimension_cols <- .sdmx_registry_vector(dimension_cols)
  missing_dimensions <- setdiff(dimension_cols, names(dt))
  if (length(missing_dimensions) > 0L) {
    stop("SDMX response is missing dimension column(s): ", paste(missing_dimensions, collapse = ", "), call. = FALSE)
  }
  period <- as.character(dt[[time_col]])
  dimension_key <- if (is.null(dimension_cols)) {
    rep(as.character(key), nrow(dt))
  } else {
    apply(dt[, dimension_cols, with = FALSE], 1L, function(row) {
      paste(paste0(dimension_cols, "=", row), collapse = "|")
    })
  }
  frequency_values <- if (!is.null(frequency) && nzchar(frequency)) {
    rep(as.character(frequency), nrow(dt))
  } else if ("FREQ" %in% names(dt)) {
    as.character(dt$FREQ)
  } else {
    rep(NA_character_, nrow(dt))
  }
  dt[, `:=`(
    source = "sdmx", provider = tolower(provider), series_id = as.character(series_id),
    label = if (is.null(label)) NA_character_ else as.character(label),
    frequency = frequency_values, period = period, date = .sdmx_period_date(period),
    value = suppressWarnings(as.numeric(get(value_col))), dimension_key = dimension_key
  )]
  data.table::setcolorder(dt, c(names(empty), setdiff(names(dt), names(empty))))
  data.table::setorderv(dt, c("date", "period", "dimension_key"), na.last = TRUE)
  unique(dt, by = c("series_id", "dimension_key", "period"))
}

#' Retrieve Data From An SDMX Provider
#'
#' @param series_id Stable local series identifier.
#' @param provider SDMX provider dialect: `oecd`, `ecb`, or `bis`.
#' @param base_url Provider REST base URL.
#' @param agency Optional SDMX agency identifier.
#' @param dataflow SDMX dataflow identifier.
#' @param version SDMX dataflow version.
#' @param key SDMX series key.
#' @param flow_ref Optional provider-specific flow reference.
#' @param format Provider query format.
#' @param accept HTTP response media type.
#' @param time_col,value_col Source observation columns.
#' @param dimension_cols Columns identifying distinct series in the response.
#' @param label,frequency Optional local metadata.
#' @param from,to Optional inclusive SDMX periods.
#' @param last_n_observations Optional number of latest observations requested
#'   per matching SDMX series.
#'
#' @return A standardized long `data.table` retaining original SDMX columns.
#' @export
get_source_data_sdmx <- function(series_id, provider, base_url, agency = NULL,
                                 dataflow, version = "latest", key = "",
                                 flow_ref = NULL, format = NULL, accept = "text/csv",
                                 time_col = "TIME_PERIOD", value_col = "OBS_VALUE",
                                 dimension_cols = NULL, label = NULL, frequency = NULL,
                                 from = NULL, to = NULL,
                                 last_n_observations = NULL) {
  url <- .sdmx_build_url(provider, base_url, agency, dataflow, version, key, flow_ref)
  query <- list()
  if (!is.null(format) && nzchar(format)) query$format <- format
  if (!is.null(from)) query$startPeriod <- as.character(from)
  if (!is.null(to)) query$endPeriod <- as.character(to)
  if (!is.null(last_n_observations)) {
    query$lastNObservations <- max(1L, as.integer(last_n_observations))
  }
  if (tolower(provider) == "oecd") query$dimensionAtObservation <- "AllDimensions"
  response <- .http_request("GET", url, query = query, headers = c(Accept = accept))
  .standardize_sdmx_data(
    .read_sdmx_csv(response), series_id = series_id, provider = provider, key = key,
    time_col = time_col, value_col = value_col, dimension_cols = dimension_cols,
    label = label, frequency = frequency
  )
}

#' Get Latest SDMX Observation Time
#'
#' @inheritParams get_source_data_sdmx
#'
#' @return A UTC `POSIXct`, or `NULL`.
#' @export
get_source_utime_sdmx <- function(series_id, provider, base_url, agency = NULL,
                                  dataflow, version = "latest", key = "",
                                  flow_ref = NULL, format = NULL, accept = "text/csv",
                                  time_col = "TIME_PERIOD", value_col = "OBS_VALUE",
                                  dimension_cols = NULL, label = NULL, frequency = NULL) {
  dt <- get_source_data_sdmx(
    series_id = series_id, provider = provider, base_url = base_url,
    agency = agency, dataflow = dataflow, version = version, key = key,
    flow_ref = flow_ref, format = format, accept = accept,
    time_col = time_col, value_col = value_col,
    dimension_cols = dimension_cols, label = label, frequency = frequency,
    last_n_observations = 1L
  )
  if (nrow(dt) == 0L || all(is.na(dt$date))) return(NULL)
  as.POSIXct(max(dt$date, na.rm = TRUE), tz = "UTC")
}

.sdmx_local_file <- function(series_id, local_path) {
  file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]+", "_", series_id), ".rds"))
}

#' Read Local SDMX Data
#'
#' @param series_id Registry series identifier.
#' @param local_path Optional SDMX storage directory.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_sdmx_data <- function(series_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("sdmx")
  .read_local_data_table(.sdmx_local_file(series_id, local_path), sort_cols = c("date", "period", "dimension_key"))
}

#' Synchronize One SDMX Registry Entry
#'
#' @inheritParams get_source_data_sdmx
#' @param local_path Optional SDMX storage directory.
#' @param overlap_days Days re-fetched around the latest local period.
#'
#' @return A local synchronization result list.
#' @export
sync_local_sdmx_data <- function(series_id, provider, base_url, agency = NULL,
                                 dataflow, version = "latest", key = "",
                                 flow_ref = NULL, format = NULL, accept = "text/csv",
                                 time_col = "TIME_PERIOD", value_col = "OBS_VALUE",
                                 dimension_cols = NULL, label = NULL, frequency = NULL,
                                 from = NULL, to = NULL, local_path = NULL,
                                 overlap_days = 62L) {
  if (is.null(local_path)) local_path <- get_source_data_path("sdmx", create = TRUE)
  local_file <- .sdmx_local_file(series_id, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- from
  if (!is.null(local_dt) && nrow(local_dt) > 0L && "date" %in% names(local_dt) && !all(is.na(local_dt$date))) {
    overlap_from <- max(local_dt$date, na.rm = TRUE) - as.integer(overlap_days)
    sync_from <- if (is.null(sync_from)) overlap_from else max(as.Date(sync_from), overlap_from)
  }
  new_dt <- get_source_data_sdmx(
    series_id, provider, base_url, agency, dataflow, version, key, flow_ref,
    format, accept, time_col, value_col, dimension_cols, label, frequency,
    from = sync_from, to = to
  )
  source_utime <- if (nrow(new_dt) == 0L || all(is.na(new_dt$date))) NULL else as.POSIXct(max(new_dt$date, na.rm = TRUE), tz = "UTC")
  sync_local_data(
    new_data = new_dt, local_file_path = local_file,
    key_cols = c("series_id", "dimension_key", "period"),
    order_cols = c("date", "period", "dimension_key"), source_utime = source_utime
  )
}

#' Get SDMX Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback.
#'
#' @return Character scalar path.
#' @export
get_sdmx_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("sdmx"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) return(.normalize_scalar_path(cfg$registry_file, config_dir = getOption("investdatar.config_dir")))
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) stop("No SDMX registry path is configured. Set SDMX.registry_file in your config.", call. = FALSE)
  file.path(config_dir, "sdmx_series_registry.json")
}

#' Get SDMX Series Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return A registry `data.table`.
#' @export
get_sdmx_registry <- function(registry_path = get_sdmx_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("series_id", "provider", "base_url", "agency", "dataflow", "version", "key", "flow_ref", "format", "accept", "time_col", "value_col", "dimension_cols", "frequency", "label", "start", "active")
  )
}

.sdmx_registry_arg <- function(registry, name, i, default = NULL) {
  if (!name %in% names(registry)) return(default)
  value <- registry[[name]][[i]]
  if (length(value) == 0L || is.null(value) || (length(value) == 1L && (is.na(value) || !nzchar(as.character(value))))) default else value
}

#' Synchronize All Registered SDMX Series
#'
#' @param registry Optional SDMX series registry.
#' @param local_path Optional SDMX storage directory.
#' @param ... Passed to `sync_local_sdmx_data()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_sdmx_registry_data <- function(registry = get_sdmx_registry(), local_path = NULL, ...) {
  stopifnot(all(c("series_id", "provider", "base_url", "dataflow") %in% names(registry)))
  if (is.null(local_path)) local_path <- get_source_data_path("sdmx", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry$active))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }
  rows <- lapply(seq_len(nrow(registry)), function(i) {
    series_id <- registry$series_id[[i]]
    provider <- registry$provider[[i]]
    tryCatch({
      res <- sync_local_sdmx_data(
        series_id = series_id, provider = provider, base_url = registry$base_url[[i]],
        agency = .sdmx_registry_arg(registry, "agency", i), dataflow = registry$dataflow[[i]],
        version = .sdmx_registry_arg(registry, "version", i, "latest"),
        key = .sdmx_registry_arg(registry, "key", i, ""), flow_ref = .sdmx_registry_arg(registry, "flow_ref", i),
        format = .sdmx_registry_arg(registry, "format", i), accept = .sdmx_registry_arg(registry, "accept", i, "text/csv"),
        time_col = .sdmx_registry_arg(registry, "time_col", i, "TIME_PERIOD"),
        value_col = .sdmx_registry_arg(registry, "value_col", i, "OBS_VALUE"),
        dimension_cols = .sdmx_registry_arg(registry, "dimension_cols", i),
        label = .sdmx_registry_arg(registry, "label", i), frequency = .sdmx_registry_arg(registry, "frequency", i),
        from = .sdmx_registry_arg(registry, "start", i), local_path = local_path, ...
      )
      data.table::data.table(
        series_id = series_id, provider = provider, status = "success", updated = isTRUE(res$updated),
        n_rows = res$n_rows %||% NA_integer_, n_new_rows = res$n_new_rows %||% NA_integer_, error = NA_character_
      )
    }, error = function(e) data.table::data.table(
      series_id = series_id, provider = provider, status = "error", updated = FALSE,
      n_rows = NA_integer_, n_new_rows = NA_integer_, error = conditionMessage(e),
      error_class = class(e)[[1L]], http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
    ))
  })
  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(rows, use.names = TRUE, fill = TRUE),
    source_id = "sdmx", run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  .write_sync_run_log("sdmx", summary_dt, local_path, params = list(), run_started_at, run_finished_at)
  summary_dt
}

#' Describe Local SDMX Data
#'
#' @param series_id Registry series identifier.
#' @param local_path Optional SDMX storage directory.
#'
#' @return Character scalar narrative.
#' @export
describe_sdmx_data <- function(series_id, local_path = NULL) {
  dt <- get_local_sdmx_data(series_id, local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local SDMX data not found for series: ", series_id, call. = FALSE)
  paste(
    sprintf("This object is a long SDMX data.table for %s from %s.", series_id, paste(unique(dt$provider), collapse = ", ")),
    sprintf("It contains %s observations across %s dimension keys.", nrow(dt), data.table::uniqueN(dt$dimension_key)),
    .describe_time_coverage(dt$date),
    "Canonical observation fields are followed by the original provider columns for auditability."
  )
}
