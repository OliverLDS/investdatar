.standardize_bls_series <- function(series, label = NULL) {
  dt <- data.table::as.data.table(series$data)
  if (!nrow(dt)) return(data.table::data.table())
  month <- suppressWarnings(as.integer(sub("^M", "", dt$period)))
  keep <- !is.na(month) & month >= 1L & month <= 12L
  dt <- dt[keep]
  footnote <- vapply(dt$footnotes, function(x) {
    if (is.null(x) || !length(x)) return(NA_character_)
    paste(stats::na.omit(unlist(x[, intersect(c("code", "text"), names(x)), drop = FALSE])), collapse = "; ")
  }, character(1))
  out <- data.table::data.table(
    source = "bls", series_id = series$seriesID, label = as.character(label %||% NA_character_),
    period = sprintf("%s-%02d", dt$year, month), date = as.Date(sprintf("%s-%02d-01", dt$year, month)),
    value = suppressWarnings(as.numeric(dt$value)), latest = tolower(as.character(dt$latest %||% NA_character_)) == "true",
    footnote = footnote
  )
  data.table::setorderv(out, "date")
  out[]
}

#' Retrieve One BLS Series
#' @param series_id BLS series identifier.
#' @param label Optional local label.
#' @param from,to Optional year or date bounds.
#' @param config Optional BLS configuration.
#' @return A standardized monthly `data.table`.
#' @export
get_source_data_bls <- function(series_id, label = NULL, from = NULL, to = NULL, config = NULL) {
  config <- .get_api_config("bls", config = config)
  start_year <- as.integer(substr(as.character(from %||% (as.integer(format(Sys.Date(), "%Y")) - 9L)), 1L, 4L))
  end_year <- as.integer(substr(as.character(to %||% Sys.Date()), 1L, 4L))
  window <- if (!is.null(config$api_key) && nzchar(config$api_key)) 20L else 10L
  starts <- seq(start_year, end_year, by = window)
  rows <- lapply(starts, function(start) {
    body <- list(seriesid = list(series_id), startyear = as.character(start), endyear = as.character(min(end_year, start + window - 1L)))
    if (!is.null(config$api_key) && nzchar(config$api_key)) body$registrationkey <- config$api_key
    response <- .http_post_json(config$url, body = body)
    if (!identical(response$status, "REQUEST_SUCCEEDED")) stop("BLS API error: ", paste(response$message, collapse = "; "), call. = FALSE)
    series <- response$Results$series
    if (is.data.frame(series)) series <- split(series, seq_len(nrow(series)))
    if (!length(series)) return(data.table::data.table())
    .standardize_bls_series(series[[1L]], label = label)
  })
  unique(data.table::rbindlist(rows, use.names = TRUE, fill = TRUE), by = c("series_id", "period"))
}

.bls_local_file <- function(series_id, local_path) file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]", "_", series_id), ".rds"))

#' Read Local BLS Data
#' @param series_id BLS series identifier.
#' @param local_path Optional BLS storage directory.
#' @return A `data.table`, or `NULL`.
#' @export
get_local_bls_data <- function(series_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("bls")
  .read_local_data_table(.bls_local_file(series_id, local_path), sort_cols = "date")
}

#' Synchronize One BLS Series
#' @inheritParams get_source_data_bls
#' @param local_path Optional BLS storage directory.
#' @param overlap_years Years re-fetched for revisions.
#' @return A standard synchronization result.
#' @export
sync_local_bls_data <- function(series_id, label = NULL, from = NULL, to = NULL, config = NULL,
                                local_path = NULL, overlap_years = 2L) {
  if (is.null(local_path)) local_path <- get_source_data_path("bls", create = TRUE)
  file <- .bls_local_file(series_id, local_path)
  old <- .safe_read_rds(file, NULL)
  sync_from <- from
  if (!is.null(old) && nrow(old)) sync_from <- max(as.integer(format(max(old$date), "%Y")) - overlap_years, as.integer(substr(as.character(from %||% "0001"), 1L, 4L)))
  new <- get_source_data_bls(series_id, label, sync_from, to, config)
  sync_local_data(new, file, c("series_id", "period"), "date", if (nrow(new)) max(new$date) else NULL)
}

#' Get BLS Registry
#' @param registry_path Optional registry path.
#' @return A registry `data.table`.
#' @export
get_bls_registry <- function(registry_path = get_bls_registry_file_path()) .read_json_registry(registry_path, c("series_id", "label", "start", "frequency", "active"))

#' Get BLS Registry File Path
#' @param config_dir Optional configuration directory.
#' @return Character scalar path.
#' @export
get_bls_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("bls"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) return(.normalize_scalar_path(cfg$registry_file, getOption("investdatar.config_dir")))
  config_dir <- config_dir %||% getOption("investdatar.config_dir")
  if (is.null(config_dir)) stop("No BLS registry path is configured.", call. = FALSE)
  file.path(config_dir, "bls_series_registry.json")
}

#' Synchronize Registered BLS Series
#' @param registry Optional BLS registry.
#' @param config Optional BLS configuration.
#' @param local_path Optional BLS storage directory.
#' @param ... Passed to `sync_local_bls_data()`.
#' @return A batch summary.
#' @export
sync_all_bls_registry_data <- function(registry = get_bls_registry(), config = NULL, local_path = NULL, ...) {
  if (is.null(local_path)) local_path <- get_source_data_path("bls", create = TRUE)
  started <- Sys.time(); registry <- registry[tolower(as.character(registry$active)) %in% c("true", "1", "yes", "y")]
  rows <- lapply(seq_len(nrow(registry)), function(i) tryCatch({
    res <- sync_local_bls_data(registry$series_id[[i]], registry$label[[i]], registry$start[[i]], config = config, local_path = local_path, ...)
    data.table::data.table(series_id = registry$series_id[[i]], status = "success", updated = isTRUE(res$updated), n_rows = res$n_rows, n_new_rows = res$n_new_rows)
  }, error = function(e) data.table::data.table(series_id = registry$series_id[[i]], status = "error", updated = FALSE, error = conditionMessage(e))))
  finished <- Sys.time(); summary <- .normalize_sync_summary(data.table::rbindlist(rows, fill = TRUE), "bls", started, finished)
  .write_sync_run_log("bls", summary, local_path, list(), started, finished); summary
}

#' Describe Local BLS Data
#' @inheritParams get_local_bls_data
#' @return Character scalar narrative.
#' @export
describe_bls_data <- function(series_id, local_path = NULL) {
  dt <- get_local_bls_data(series_id, local_path)
  if (is.null(dt) || !nrow(dt)) stop("Local BLS data not found: ", series_id, call. = FALSE)
  paste(sprintf("BLS series %s contains %s monthly observations.", series_id, nrow(dt)), .describe_time_coverage(dt$date), .describe_value_summary(dt$value))
}
