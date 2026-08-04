.cftc_cot_dataset_map <- function(report_type = "tff") {
  maps <- list(
    tff = c(futures_only = "gpe5-46if", combined = "yw9f-hn96"),
    disaggregated = c(futures_only = "72hh-3qpy", combined = "kh3c-gbw2"),
    legacy = c(futures_only = "6dca-aqww", combined = "jun7-fc8e")
  )
  maps[[.normalize_cftc_report_type(report_type)]]
}

.normalize_cftc_report_type <- function(report_type) {
  value <- tolower(trimws(as.character(report_type)))
  aliases <- c(tff = "tff", financial = "tff", disaggregated = "disaggregated", dcot = "disaggregated", legacy = "legacy")
  out <- unname(aliases[[value]])
  if (is.null(out)) stop("Unsupported CFTC COT report type: ", report_type, call. = FALSE)
  out
}

.normalize_cftc_report_variant <- function(report_variant) {
  aliases <- c(
    futures_only = "futures_only",
    futures = "futures_only",
    futonly = "futures_only",
    combined = "combined",
    futures_and_options = "combined"
  )
  key <- tolower(gsub("[^a-z0-9]+", "_", trimws(report_variant)))
  out <- unname(aliases[[key]])
  if (is.null(out)) {
    stop("Unsupported CFTC TFF report variant: ", report_variant, call. = FALSE)
  }
  out
}

.cftc_cot_base_url <- function(dataset_id) {
  sprintf("https://publicreporting.cftc.gov/resource/%s.json", dataset_id)
}

.cftc_cot_metadata_url <- function(dataset_id) {
  sprintf("https://publicreporting.cftc.gov/api/views/%s.json", dataset_id)
}

.cftc_escape_soql_string <- function(x) {
  gsub("'", "''", x, fixed = TRUE)
}

.cftc_cot_where <- function(market_codes = NULL, from = NULL, to = NULL) {
  clauses <- character()
  if (!is.null(market_codes)) {
    market_codes <- unique(trimws(as.character(market_codes)))
    market_codes <- market_codes[nzchar(market_codes)]
    if (length(market_codes) > 0L) {
      values <- sprintf("'%s'", .cftc_escape_soql_string(market_codes))
      clauses <- c(clauses, sprintf("cftc_contract_market_code in (%s)", paste(values, collapse = ",")))
    }
  }
  if (!is.null(from)) {
    clauses <- c(clauses, sprintf("report_date_as_yyyy_mm_dd >= '%sT00:00:00.000'", as.Date(from)))
  }
  if (!is.null(to)) {
    clauses <- c(clauses, sprintf("report_date_as_yyyy_mm_dd <= '%sT23:59:59.999'", as.Date(to)))
  }
  if (length(clauses) == 0L) NULL else paste(clauses, collapse = " AND ")
}

.standardize_cftc_cot <- function(data, report_id, report_variant, dataset_id, report_type = "tff") {
  dt <- data.table::as.data.table(data)
  if (nrow(dt) == 0L) {
    return(data.table::data.table(
      source = character(), report_id = character(), report_type = character(),
      report_variant = character(), dataset_id = character(), id = character(),
      report_date = as.Date(character()), cftc_contract_market_code = character()
    ))
  }
  required <- c("id", "report_date_as_yyyy_mm_dd", "cftc_contract_market_code")
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols) > 0L) {
    stop("CFTC response is missing required column(s): ", paste(missing_cols, collapse = ", "), call. = FALSE)
  }

  dt[, report_date := as.Date(substr(report_date_as_yyyy_mm_dd, 1L, 10L))]
  dt[, cftc_contract_market_code := trimws(as.character(cftc_contract_market_code))]
  for (nm in intersect(c("cftc_market_code", "cftc_region_code", "cftc_commodity_code"), names(dt))) {
    data.table::set(dt, j = nm, value = trimws(as.character(dt[[nm]])))
  }

  numeric_cols <- grep(
    "^(open_interest|dealer_|asset_mgr_|lev_money_|other_rept_|tot_rept_|nonrept_|prod_merc_|swap_|m_money_|noncomm_|comm_|change_in_|pct_of_|traders_|conc_)",
    names(dt),
    value = TRUE
  )
  for (nm in numeric_cols) {
    data.table::set(dt, j = nm, value = suppressWarnings(as.numeric(dt[[nm]])))
  }

  dt[, `:=`(
    source = "cftc",
    report_id = as.character(report_id),
    report_type = report_type,
    report_variant = report_variant,
    dataset_id = dataset_id
  )]
  leading <- c(
    "source", "report_id", "report_type", "report_variant", "dataset_id",
    "id", "report_date", "market_and_exchange_names", "contract_market_name",
    "cftc_contract_market_code"
  )
  data.table::setcolorder(dt, c(intersect(leading, names(dt)), setdiff(names(dt), leading)))
  data.table::setorderv(dt, c("report_date", "cftc_contract_market_code", "id"))
  dt[]
}

#' Retrieve CFTC Commitments Of Traders Data
#'
#' Retrieves TFF, Disaggregated, or Legacy reports from the CFTC Public
#' Reporting Environment.
#'
#' @param report_type Report family: `"tff"`, `"disaggregated"`, or `"legacy"`.
#' @param report_variant Report variant: `"futures_only"` or `"combined"`.
#' @param report_id Stable local report identifier.
#' @param dataset_id Optional official Socrata dataset identifier.
#' @param market_codes Optional CFTC contract-market codes.
#' @param from,to Optional inclusive report-date bounds.
#' @param page_size Number of source rows requested per page.
#' @param max_pages Optional page limit, primarily useful for diagnostics.
#'
#' @return A standardized wide `data.table`, one row per market and report date.
#' @export
get_source_data_cftc_cot <- function(report_variant = c("futures_only", "combined"),
                                     report_type = "tff",
                                     report_id = NULL, dataset_id = NULL,
                                     market_codes = NULL, from = NULL, to = NULL,
                                     page_size = 5000L, max_pages = Inf) {
  report_variant <- .normalize_cftc_report_variant(match.arg(report_variant))
  report_type <- .normalize_cftc_report_type(report_type)
  if (is.null(dataset_id) || !nzchar(dataset_id)) {
    dataset_id <- .cftc_cot_dataset_map(report_type)[[report_variant]]
  }
  if (is.null(report_id) || !nzchar(report_id)) {
    report_id <- paste0(report_type, "_", report_variant)
  }
  page_size <- max(1L, min(as.integer(page_size), 50000L))
  where <- .cftc_cot_where(market_codes = market_codes, from = from, to = to)
  pages <- list()
  offset <- 0L
  page <- 1L

  repeat {
    query <- list(
      `$limit` = page_size,
      `$offset` = offset,
      `$order` = "report_date_as_yyyy_mm_dd ASC,cftc_contract_market_code ASC,id ASC"
    )
    if (!is.null(where)) query[["$where"]] <- where
    raw <- .http_get_json(.cftc_cot_base_url(dataset_id), query = query)
    page_dt <- data.table::as.data.table(raw)
    if (nrow(page_dt) == 0L) break
    pages[[length(pages) + 1L]] <- page_dt
    if (nrow(page_dt) < page_size || page >= max_pages) break
    offset <- offset + page_size
    page <- page + 1L
  }

  combined <- if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
  .standardize_cftc_cot(combined, report_id = report_id, report_type = report_type, report_variant = report_variant, dataset_id = dataset_id)
}

#' Get CFTC Dataset Update Time
#'
#' @inheritParams get_source_data_cftc_cot
#'
#' @return A UTC `POSIXct` update time, or `NULL` when unavailable.
#' @export
get_source_utime_cftc_cot <- function(report_variant = c("futures_only", "combined"), report_type = "tff", dataset_id = NULL) {
  report_variant <- .normalize_cftc_report_variant(match.arg(report_variant))
  report_type <- .normalize_cftc_report_type(report_type)
  if (is.null(dataset_id) || !nzchar(dataset_id)) {
    dataset_id <- .cftc_cot_dataset_map(report_type)[[report_variant]]
  }
  metadata <- .http_get_json(.cftc_cot_metadata_url(dataset_id))
  stamp <- metadata$rowsUpdatedAt
  if (is.null(stamp) || length(stamp) == 0L || is.na(stamp)) return(NULL)
  as.POSIXct(as.numeric(stamp), origin = "1970-01-01", tz = "UTC")
}

.cftc_cot_local_file <- function(report_id, local_path) {
  safe_id <- gsub("[^A-Za-z0-9._-]+", "_", report_id)
  file.path(local_path, paste0(safe_id, ".rds"))
}

#' Read Local CFTC COT Data
#'
#' @param report_id Registry report identifier.
#' @param local_path Optional CFTC storage directory.
#'
#' @return A `data.table`, or `NULL` when no local file exists.
#' @export
get_local_cftc_cot <- function(report_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("cftc")
  .read_local_data_table(.cftc_cot_local_file(report_id, local_path), sort_cols = c("report_date", "cftc_contract_market_code", "id"))
}

#' Synchronize One CFTC COT Report
#'
#' @inheritParams get_source_data_cftc_cot
#' @param local_path Optional CFTC storage directory.
#' @param overlap_days Number of days re-fetched around the latest local report.
#'
#' @return A local synchronization result list.
#' @export
sync_local_cftc_cot <- function(report_variant = c("futures_only", "combined"),
                                report_type = "tff",
                                report_id = NULL, dataset_id = NULL,
                                market_codes = NULL, from = NULL, to = NULL,
                                local_path = NULL, overlap_days = 14L,
                                page_size = 5000L) {
  report_variant <- .normalize_cftc_report_variant(match.arg(report_variant))
  report_type <- .normalize_cftc_report_type(report_type)
  if (is.null(report_id) || !nzchar(report_id)) report_id <- paste0(report_type, "_", report_variant)
  if (is.null(dataset_id) || !nzchar(dataset_id)) dataset_id <- .cftc_cot_dataset_map(report_type)[[report_variant]]
  if (is.null(local_path)) local_path <- get_source_data_path("cftc", create = TRUE)
  local_file <- .cftc_cot_local_file(report_id, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- if (is.null(from)) NULL else as.Date(from)
  if (!is.null(local_dt) && nrow(local_dt) > 0L && "report_date" %in% names(local_dt)) {
    overlap_from <- max(local_dt$report_date, na.rm = TRUE) - as.integer(overlap_days)
    sync_from <- if (is.null(sync_from)) overlap_from else max(sync_from, overlap_from)
  }

  new_dt <- get_source_data_cftc_cot(
    report_variant = report_variant,
    report_type = report_type,
    report_id = report_id,
    dataset_id = dataset_id,
    market_codes = market_codes,
    from = sync_from,
    to = to,
    page_size = page_size
  )
  source_utime <- tryCatch(
    get_source_utime_cftc_cot(report_variant = report_variant, report_type = report_type, dataset_id = dataset_id),
    error = function(e) NULL
  )
  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file,
    key_cols = c("report_id", "id"),
    order_cols = c("report_date", "cftc_contract_market_code", "id"),
    source_utime = source_utime
  )
}

#' Get CFTC COT Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback.
#'
#' @return Character scalar path.
#' @export
get_cftc_cot_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("cftc"), error = function(e) list())
  registry_file <- cfg$registry_file
  if (!is.null(registry_file) && nzchar(registry_file)) {
    return(.normalize_scalar_path(registry_file, config_dir = getOption("investdatar.config_dir")))
  }
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) {
    stop("No CFTC registry path is configured. Set CFTC.registry_file in your config.", call. = FALSE)
  }
  file.path(config_dir, "cftc_cot_registry.json")
}

#' Get CFTC COT Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return A registry `data.table`.
#' @export
get_cftc_cot_registry <- function(registry_path = get_cftc_cot_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("report_id", "report_type", "report_variant", "dataset_id", "market_codes", "start", "active")
  )
}

.cftc_registry_market_codes <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) return(NULL)
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  x <- trimws(unlist(strsplit(as.character(x), ",", fixed = TRUE), use.names = FALSE))
  x[nzchar(x)]
}

#' Synchronize All Registered CFTC COT Reports
#'
#' @param registry Optional CFTC registry table.
#' @param local_path Optional CFTC storage directory.
#' @param ... Passed to `sync_local_cftc_cot()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_cftc_cot_registry_data <- function(registry = get_cftc_cot_registry(), local_path = NULL, ...) {
  stopifnot(all(c("report_id", "report_variant", "dataset_id") %in% names(registry)))
  if (is.null(local_path)) local_path <- get_source_data_path("cftc", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry$active))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }

  rows <- lapply(seq_len(nrow(registry)), function(i) {
    report_id <- registry$report_id[[i]]
    report_type <- if ("report_type" %in% names(registry)) registry$report_type[[i]] else "tff"
    report_variant <- registry$report_variant[[i]]
    dataset_id <- registry$dataset_id[[i]]
    market_codes <- if ("market_codes" %in% names(registry)) .cftc_registry_market_codes(registry$market_codes[[i]]) else NULL
    start <- if ("start" %in% names(registry) && !is.na(registry$start[[i]]) && nzchar(registry$start[[i]])) registry$start[[i]] else NULL
    tryCatch(
      {
        res <- sync_local_cftc_cot(
          report_variant = report_variant,
          report_type = report_type,
          report_id = report_id,
          dataset_id = dataset_id,
          market_codes = market_codes,
          from = start,
          local_path = local_path,
          ...
        )
        data.table::data.table(
          report_id = report_id, report_type = report_type, report_variant = report_variant, dataset_id = dataset_id,
          status = "success", updated = isTRUE(res$updated),
          n_rows = if (is.null(res$n_rows)) NA_integer_ else res$n_rows,
          n_new_rows = if (is.null(res$n_new_rows)) NA_integer_ else res$n_new_rows,
          error = NA_character_
        )
      },
      error = function(e) data.table::data.table(
        report_id = report_id, report_type = report_type, report_variant = report_variant, dataset_id = dataset_id,
        status = "error", updated = FALSE, n_rows = NA_integer_, n_new_rows = NA_integer_,
        error = conditionMessage(e),
        error_class = class(e)[[1L]],
        http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
      )
    )
  })
  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(rows, use.names = TRUE, fill = TRUE),
    source_id = "cftc",
    run_started_at = run_started_at,
    run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "cftc", summary = summary_dt, local_path = local_path,
    params = list(), run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  summary_dt
}

#' Describe Local CFTC COT Data
#'
#' @param report_id Registry report identifier.
#' @param local_path Optional CFTC storage directory.
#'
#' @return Character scalar narrative.
#' @export
describe_cftc_cot_data <- function(report_id, local_path = NULL) {
  dt <- get_local_cftc_cot(report_id = report_id, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local CFTC COT data not found for report_id: ", report_id, call. = FALSE)
  paste(
    sprintf("This object is a data.table for CFTC Commitments of Traders report %s.", report_id),
    "Each row represents one contract market on one weekly report date; position, change, percentage, trader-count, and concentration measures remain in fixed source-aligned columns.",
    sprintf("The table contains %s rows, %s contract markets, and %s columns.", nrow(dt), data.table::uniqueN(dt$cftc_contract_market_code), ncol(dt)),
    .describe_time_coverage(dt$report_date),
    "The compound local key is report_id plus the source row id. CFTC contract-market code is the stable analytical market identifier."
  )
}
