.fiscaldata_base_url <- function(endpoint) {
  endpoint <- sub("^/+", "", endpoint)
  paste0("https://api.fiscaldata.treasury.gov/services/api/fiscal_service/", endpoint)
}

.fiscaldata_registry_vector <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) return(NULL)
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  x <- trimws(unlist(strsplit(as.character(x), ",", fixed = TRUE), use.names = FALSE))
  x[nzchar(x)]
}

.fiscaldata_filter <- function(date_col, from = NULL, to = NULL) {
  filters <- character()
  if (!is.null(from)) filters <- c(filters, sprintf("%s:gte:%s", date_col, as.Date(from)))
  if (!is.null(to)) filters <- c(filters, sprintf("%s:lte:%s", date_col, as.Date(to)))
  if (length(filters) == 0L) NULL else paste(filters, collapse = ",")
}

.standardize_fiscaldata <- function(data, dataset_id, data_types = list(),
                                    date_col = "record_date", key_cols = date_col) {
  dt <- data.table::as.data.table(data)
  required <- unique(c(date_col, key_cols))
  if (nrow(dt) == 0L) {
    out <- data.table::data.table(source = character(), dataset_id = character())
    for (nm in required) out[, (nm) := character()]
    out[, (date_col) := as.Date(get(date_col))]
    return(out[])
  }
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols) > 0L) {
    stop("Fiscal Data response is missing required column(s): ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  for (nm in names(dt)) {
    if (is.character(dt[[nm]])) {
      data.table::set(dt, i = which(dt[[nm]] %in% c("null", "")), j = nm, value = NA_character_)
    }
    type <- toupper(as.character(data_types[[nm]]))
    if (length(type) == 0L || is.na(type)) next
    if (type == "DATE") {
      data.table::set(dt, j = nm, value = as.Date(dt[[nm]]))
    } else if (type %in% c("CURRENCY", "CURRENCY0", "NUMBER", "PERCENTAGE", "DECIMAL")) {
      data.table::set(dt, j = nm, value = suppressWarnings(as.numeric(dt[[nm]])))
    } else if (type %in% c("INTEGER", "YEAR", "QUARTER", "MONTH", "DAY")) {
      data.table::set(dt, j = nm, value = suppressWarnings(as.integer(dt[[nm]])))
    }
  }
  if (!inherits(dt[[date_col]], "Date")) data.table::set(dt, j = date_col, value = as.Date(dt[[date_col]]))
  dt[, `:=`(source = "treasury_fiscaldata", dataset_id = as.character(dataset_id))]
  data.table::setcolorder(dt, c("source", "dataset_id", date_col, setdiff(names(dt), c("source", "dataset_id", date_col))))
  data.table::setorderv(dt, unique(c(date_col, key_cols)))
  dt[]
}

#' Retrieve A U.S. Treasury Fiscal Data Table
#'
#' @param dataset_id Stable local dataset identifier.
#' @param endpoint Fiscal Data endpoint relative to the fiscal-service base URL.
#' @param date_col Source date column.
#' @param key_cols Source columns that uniquely identify observations per date.
#' @param fields Optional source fields to request.
#' @param from,to Optional inclusive date bounds.
#' @param page_size Number of source rows requested per page.
#' @param max_pages Optional page limit for diagnostics.
#'
#' @return A typed, table-shaped `data.table` with source and dataset identity.
#' @export
get_source_data_fiscaldata <- function(dataset_id, endpoint,
                                       date_col = "record_date", key_cols = date_col,
                                       fields = NULL, from = NULL, to = NULL,
                                       page_size = 10000L, max_pages = Inf) {
  key_cols <- .fiscaldata_registry_vector(key_cols)
  fields <- .fiscaldata_registry_vector(fields)
  page_size <- max(1L, min(as.integer(page_size), 10000L))
  page_number <- 1L
  pages <- list()
  data_types <- list()

  repeat {
    query <- list(
      sort = paste0(date_col, ",", paste(setdiff(key_cols, date_col), collapse = ",")),
      format = "json",
      `page[number]` = page_number,
      `page[size]` = page_size
    )
    query$sort <- sub(",$", "", query$sort)
    filter <- .fiscaldata_filter(date_col, from = from, to = to)
    if (!is.null(filter)) query$filter <- filter
    if (!is.null(fields)) query$fields <- paste(unique(c(date_col, key_cols, fields)), collapse = ",")

    response <- .http_get_json(.fiscaldata_base_url(endpoint), query = query)
    page_dt <- data.table::as.data.table(response$data)
    if (length(data_types) == 0L && !is.null(response$meta$dataTypes)) data_types <- response$meta$dataTypes
    if (nrow(page_dt) == 0L) break
    pages[[length(pages) + 1L]] <- page_dt
    total_pages <- suppressWarnings(as.integer(response$meta[["total-pages"]]))
    if (is.na(total_pages) || page_number >= total_pages || page_number >= max_pages) break
    page_number <- page_number + 1L
  }

  combined <- if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
  .standardize_fiscaldata(combined, dataset_id = dataset_id, data_types = data_types, date_col = date_col, key_cols = key_cols)
}

#' Get The Latest Treasury Fiscal Data Date
#'
#' @inheritParams get_source_data_fiscaldata
#'
#' @return A UTC `POSIXct` inferred from the newest source record, or `NULL`.
#' @export
get_source_utime_fiscaldata <- function(dataset_id, endpoint, date_col = "record_date") {
  response <- .http_get_json(
    .fiscaldata_base_url(endpoint),
    query = list(fields = date_col, sort = paste0("-", date_col), format = "json", `page[number]` = 1L, `page[size]` = 1L)
  )
  if (is.null(response$data) || length(response$data) == 0L) return(NULL)
  value <- data.table::as.data.table(response$data)[[date_col]][[1L]]
  as.POSIXct(as.Date(value), tz = "UTC")
}

.fiscaldata_local_file <- function(dataset_id, local_path) {
  file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]+", "_", dataset_id), ".rds"))
}

#' Read Local Treasury Fiscal Data
#'
#' @param dataset_id Registry dataset identifier.
#' @param local_path Optional Fiscal Data storage directory.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_fiscaldata <- function(dataset_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("fiscaldata")
  .read_local_data_table(.fiscaldata_local_file(dataset_id, local_path))
}

#' Synchronize One Treasury Fiscal Data Table
#'
#' @inheritParams get_source_data_fiscaldata
#' @param local_path Optional Fiscal Data storage directory.
#' @param overlap_days Days re-fetched around the latest local date.
#'
#' @return A local synchronization result list.
#' @export
sync_local_fiscaldata <- function(dataset_id, endpoint,
                                  date_col = "record_date", key_cols = date_col,
                                  fields = NULL, from = NULL, to = NULL,
                                  local_path = NULL, overlap_days = 14L,
                                  page_size = 10000L) {
  key_cols <- .fiscaldata_registry_vector(key_cols)
  if (is.null(local_path)) local_path <- get_source_data_path("fiscaldata", create = TRUE)
  local_file <- .fiscaldata_local_file(dataset_id, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- if (is.null(from)) NULL else as.Date(from)
  if (!is.null(local_dt) && nrow(local_dt) > 0L && date_col %in% names(local_dt)) {
    overlap_from <- max(local_dt[[date_col]], na.rm = TRUE) - as.integer(overlap_days)
    sync_from <- if (is.null(sync_from)) overlap_from else max(sync_from, overlap_from)
  }
  new_dt <- get_source_data_fiscaldata(
    dataset_id = dataset_id, endpoint = endpoint, date_col = date_col,
    key_cols = key_cols, fields = fields, from = sync_from, to = to,
    page_size = page_size
  )
  source_utime <- tryCatch(
    get_source_utime_fiscaldata(dataset_id = dataset_id, endpoint = endpoint, date_col = date_col),
    error = function(e) NULL
  )
  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file,
    key_cols = c("dataset_id", key_cols),
    order_cols = unique(c(date_col, key_cols)),
    source_utime = source_utime
  )
}

#' Get Treasury Fiscal Data Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback.
#'
#' @return Character scalar path.
#' @export
get_fiscaldata_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("fiscaldata"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) {
    return(.normalize_scalar_path(cfg$registry_file, config_dir = getOption("investdatar.config_dir")))
  }
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) stop("No Fiscal Data registry path is configured. Set FiscalData.registry_file in your config.", call. = FALSE)
  file.path(config_dir, "fiscaldata_registry.json")
}

#' Get Treasury Fiscal Data Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return A registry `data.table`.
#' @export
get_fiscaldata_registry <- function(registry_path = get_fiscaldata_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("dataset_id", "endpoint", "date_col", "key_cols", "fields", "frequency", "start", "label", "active")
  )
}

#' Synchronize All Registered Treasury Fiscal Data
#'
#' @param registry Optional Fiscal Data registry table.
#' @param local_path Optional Fiscal Data storage directory.
#' @param ... Passed to `sync_local_fiscaldata()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_fiscaldata_registry_data <- function(registry = get_fiscaldata_registry(), local_path = NULL, ...) {
  stopifnot(all(c("dataset_id", "endpoint", "date_col", "key_cols") %in% names(registry)))
  if (is.null(local_path)) local_path <- get_source_data_path("fiscaldata", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry$active))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }
  rows <- lapply(seq_len(nrow(registry)), function(i) {
    dataset_id <- registry$dataset_id[[i]]
    endpoint <- registry$endpoint[[i]]
    date_col <- registry$date_col[[i]]
    key_cols <- .fiscaldata_registry_vector(registry$key_cols[[i]])
    fields <- if ("fields" %in% names(registry)) .fiscaldata_registry_vector(registry$fields[[i]]) else NULL
    start <- if ("start" %in% names(registry) && !is.na(registry$start[[i]]) && nzchar(registry$start[[i]])) registry$start[[i]] else NULL
    tryCatch({
      res <- sync_local_fiscaldata(
        dataset_id = dataset_id, endpoint = endpoint, date_col = date_col,
        key_cols = key_cols, fields = fields, from = start,
        local_path = local_path, ...
      )
      data.table::data.table(
        dataset_id = dataset_id, endpoint = endpoint, status = "success",
        updated = isTRUE(res$updated), n_rows = if (is.null(res$n_rows)) NA_integer_ else res$n_rows,
        n_new_rows = if (is.null(res$n_new_rows)) NA_integer_ else res$n_new_rows,
        error = NA_character_
      )
    }, error = function(e) data.table::data.table(
      dataset_id = dataset_id, endpoint = endpoint, status = "error", updated = FALSE,
      n_rows = NA_integer_, n_new_rows = NA_integer_, error = conditionMessage(e),
      error_class = class(e)[[1L]], http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
    ))
  })
  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(rows, use.names = TRUE, fill = TRUE),
    source_id = "fiscaldata", run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "fiscaldata", summary = summary_dt, local_path = local_path,
    params = list(), run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  summary_dt
}

#' Describe Local Treasury Fiscal Data
#'
#' @param dataset_id Registry dataset identifier.
#' @param local_path Optional Fiscal Data storage directory.
#'
#' @return Character scalar narrative.
#' @export
describe_fiscaldata <- function(dataset_id, local_path = NULL) {
  dt <- get_local_fiscaldata(dataset_id, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local Fiscal Data not found for dataset_id: ", dataset_id, call. = FALSE)
  date_col <- if ("record_date" %in% names(dt)) "record_date" else names(dt)[vapply(dt, inherits, logical(1), "Date")][[1L]]
  paste(
    sprintf("This object is a data.table for U.S. Treasury Fiscal Data dataset %s.", dataset_id),
    "The table preserves source-specific fields and adds source and dataset_id identity columns.",
    sprintf("The table contains %s rows and %s columns.", nrow(dt), ncol(dt)),
    .describe_time_coverage(dt[[date_col]]),
    "Registry-declared key columns determine deterministic local upserts."
  )
}
