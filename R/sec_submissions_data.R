.sec_submissions_url <- function(cik, config) {
  sprintf("%s/submissions/CIK%s.json", sub("/+$", "", config$data_url), .normalize_sec_cik(cik, padded = TRUE))
}

.sec_submission_file_url <- function(name, config) {
  paste0(sub("/+$", "", config$data_url), "/submissions/", name)
}

.sec_columnar_table <- function(x) {
  if (is.null(x) || length(x) == 0L) return(data.table::data.table())
  data.table::as.data.table(x)
}

.standardize_sec_submissions <- function(data, cik, ticker = NULL, company_name = NULL) {
  cik_value <- .normalize_sec_cik(cik)
  ticker_value <- if (is.null(ticker)) NA_character_ else toupper(ticker)
  company_name_value <- if (is.null(company_name)) NA_character_ else as.character(company_name)
  dt <- data.table::as.data.table(data)
  empty <- data.table::data.table(
    source = character(), cik = character(), ticker = character(), company_name = character(),
    accession_number = character(), filing_date = as.Date(character()),
    report_date = as.Date(character()), acceptance_datetime = .empty_posixct(),
    act = character(), form = character(), file_number = character(),
    film_number = character(), items = character(), size = numeric(),
    is_xbrl = logical(), is_inline_xbrl = logical(), primary_document = character(),
    primary_doc_description = character()
  )
  if (nrow(dt) == 0L) return(empty)

  rename <- c(
    accessionNumber = "accession_number", filingDate = "filing_date",
    reportDate = "report_date", acceptanceDateTime = "acceptance_datetime",
    fileNumber = "file_number", filmNumber = "film_number",
    isXBRL = "is_xbrl", isInlineXBRL = "is_inline_xbrl",
    primaryDocument = "primary_document",
    primaryDocDescription = "primary_doc_description"
  )
  for (old in intersect(names(rename), names(dt))) data.table::setnames(dt, old, rename[[old]])
  required <- c("accession_number", "filing_date", "form")
  missing_cols <- setdiff(required, names(dt))
  if (length(missing_cols) > 0L) stop("SEC submissions response is missing column(s): ", paste(missing_cols, collapse = ", "), call. = FALSE)
  for (nm in setdiff(names(empty), names(dt))) {
    data.table::set(dt, j = nm, value = .sec_missing_column(empty[[nm]], nrow(dt)))
  }

  dt[, filing_date := as.Date(filing_date)]
  dt[, report_date := as.Date(report_date)]
  acceptance_raw <- as.character(dt$acceptance_datetime)
  parsed <- as.POSIXct(strptime(acceptance_raw, format = "%Y%m%d%H%M%S", tz = "America/New_York"))
  fallback <- is.na(parsed) & nzchar(acceptance_raw)
  if (any(fallback)) parsed[fallback] <- as.POSIXct(acceptance_raw[fallback], tz = "America/New_York")
  dt[, acceptance_datetime := parsed]
  dt[, `:=`(
    source = "sec", cik = cik_value,
    ticker = ticker_value,
    company_name = company_name_value,
    size = suppressWarnings(as.numeric(size)),
    is_xbrl = as.character(is_xbrl) %in% c("1", "TRUE", "true"),
    is_inline_xbrl = as.character(is_inline_xbrl) %in% c("1", "TRUE", "true")
  )]
  data.table::setcolorder(dt, c(names(empty), setdiff(names(dt), names(empty))))
  data.table::setorderv(dt, c("filing_date", "acceptance_datetime", "accession_number"))
  unique(dt, by = c("cik", "accession_number"))
}

#' Retrieve SEC Filing Submissions
#'
#' @param cik SEC Central Index Key.
#' @param ticker Optional ticker label.
#' @param company_name Optional company name label.
#' @param forms Optional filing forms to retain.
#' @param from,to Optional inclusive filing-date bounds.
#' @param include_history Logical; retrieve older SEC submission files in addition to recent filings.
#' @param config Optional SEC configuration.
#'
#' @return A standardized filing-event `data.table`.
#' @export
get_source_data_sec_submissions <- function(cik, ticker = NULL, company_name = NULL,
                                            forms = NULL, from = NULL, to = NULL,
                                            include_history = TRUE, config = NULL) {
  config <- .sec_api_config(config)
  response <- .sec_get_json(.sec_submissions_url(cik, config), config = config)
  recent <- .sec_columnar_table(response$filings$recent)
  batches <- list(recent)
  if (isTRUE(include_history) && !is.null(response$filings$files) && length(response$filings$files) > 0L) {
    files <- data.table::as.data.table(response$filings$files)
    if ("name" %in% names(files)) {
      for (name in files$name) {
        historical <- .sec_get_json(.sec_submission_file_url(name, config), config = config)
        batches[[length(batches) + 1L]] <- .sec_columnar_table(historical)
      }
    }
  }
  combined <- data.table::rbindlist(batches, use.names = TRUE, fill = TRUE)
  out <- .standardize_sec_submissions(
    combined, cik = cik,
    ticker = if (is.null(ticker)) response$tickers[[1L]] else ticker,
    company_name = if (is.null(company_name)) response$name else company_name
  )
  forms <- .sec_registry_values(forms)
  if (!is.null(forms)) out <- out[form %in% forms]
  if (!is.null(from)) out <- out[filing_date >= as.Date(from)]
  if (!is.null(to)) out <- out[filing_date <= as.Date(to)]
  out[]
}

#' Get Latest SEC Submission Time
#'
#' @param cik SEC Central Index Key.
#' @param config Optional SEC configuration.
#'
#' @return A UTC-compatible `POSIXct`, or `NULL`.
#' @export
get_source_utime_sec_submissions <- function(cik, config = NULL) {
  dt <- get_source_data_sec_submissions(cik, include_history = FALSE, config = config)
  if (nrow(dt) == 0L || all(is.na(dt$acceptance_datetime))) return(NULL)
  max(dt$acceptance_datetime, na.rm = TRUE)
}

.sec_submissions_local_file <- function(cik, local_path) {
  file.path(local_path, paste0("CIK", .normalize_sec_cik(cik, padded = TRUE), ".rds"))
}

#' Read Local SEC Submissions
#'
#' @param cik SEC Central Index Key.
#' @param local_path Optional SEC submissions storage directory.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_sec_submissions <- function(cik, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "submissions")
  .read_local_data_table(.sec_submissions_local_file(cik, local_path), sort_cols = c("filing_date", "acceptance_datetime", "accession_number"))
}

#' Synchronize SEC Submissions For One Company
#'
#' @inheritParams get_source_data_sec_submissions
#' @param local_path Optional SEC submissions storage directory.
#' @param overlap_days Filing-date overlap for incremental synchronization.
#'
#' @return A local synchronization result list.
#' @export
sync_local_sec_submissions <- function(cik, ticker = NULL, company_name = NULL,
                                       forms = NULL, from = NULL, to = NULL,
                                       include_history = TRUE, config = NULL,
                                       local_path = NULL, overlap_days = 14L) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "submissions", create = TRUE)
  local_file <- .sec_submissions_local_file(cik, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- if (is.null(from)) NULL else as.Date(from)
  first_sync <- is.null(local_dt) || nrow(local_dt) == 0L
  if (!first_sync) {
    overlap_from <- max(local_dt$filing_date, na.rm = TRUE) - as.integer(overlap_days)
    sync_from <- if (is.null(sync_from)) overlap_from else max(sync_from, overlap_from)
  }
  new_dt <- get_source_data_sec_submissions(
    cik = cik, ticker = ticker, company_name = company_name, forms = forms,
    from = sync_from, to = to,
    include_history = isTRUE(include_history) && first_sync,
    config = config
  )
  source_utime <- if (nrow(new_dt) == 0L || all(is.na(new_dt$acceptance_datetime))) NULL else max(new_dt$acceptance_datetime, na.rm = TRUE)
  sync_local_data(
    new_data = new_dt, local_file_path = local_file,
    key_cols = c("cik", "accession_number"),
    order_cols = c("filing_date", "acceptance_datetime", "accession_number"),
    source_utime = source_utime
  )
}

#' Synchronize Registered SEC Submissions
#'
#' @param registry Optional SEC company registry.
#' @param config Optional SEC configuration.
#' @param local_path Optional SEC submissions storage directory.
#' @param ... Passed to `sync_local_sec_submissions()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_sec_submissions_registry_data <- function(registry = get_sec_registry(), config = NULL, local_path = NULL, ...) {
  stopifnot(all(c("ticker", "cik") %in% names(registry)))
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "submissions", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry$active))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }
  rows <- lapply(seq_len(nrow(registry)), function(i) {
    ticker <- registry$ticker[[i]]
    cik <- registry$cik[[i]]
    company_name <- if ("company_name" %in% names(registry)) registry$company_name[[i]] else NULL
    forms <- if ("forms" %in% names(registry)) .sec_registry_values(registry$forms[[i]]) else NULL
    tryCatch({
      res <- sync_local_sec_submissions(
        cik = cik, ticker = ticker, company_name = company_name, forms = forms,
        config = config, local_path = local_path, ...
      )
      data.table::data.table(
        ticker = ticker, cik = as.character(cik), status = "success", updated = isTRUE(res$updated),
        n_rows = if (is.null(res$n_rows)) NA_integer_ else res$n_rows,
        n_new_rows = if (is.null(res$n_new_rows)) NA_integer_ else res$n_new_rows,
        error = NA_character_
      )
    }, error = function(e) data.table::data.table(
      ticker = ticker, cik = as.character(cik), status = "error", updated = FALSE,
      n_rows = NA_integer_, n_new_rows = NA_integer_, error = conditionMessage(e),
      error_class = class(e)[[1L]], http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
    ))
  })
  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(rows, use.names = TRUE, fill = TRUE),
    source_id = "sec_submissions", run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "sec_submissions", summary = summary_dt, local_path = local_path,
    params = list(), run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  summary_dt
}

#' Describe Local SEC Submissions
#'
#' @param cik SEC Central Index Key.
#' @param local_path Optional SEC submissions storage directory.
#'
#' @return Character scalar narrative.
#' @export
describe_sec_submissions <- function(cik, local_path = NULL) {
  dt <- get_local_sec_submissions(cik, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local SEC submissions not found for CIK: ", cik, call. = FALSE)
  paste(
    sprintf("This object is a data.table of SEC filing events for CIK %s.", .normalize_sec_cik(cik)),
    sprintf("The table contains %s filings across forms: %s.", nrow(dt), paste(sort(unique(dt$form)), collapse = ", ")),
    .describe_time_coverage(dt$filing_date),
    "Each filing is keyed by CIK and accession number; primary_document identifies the filed document without downloading its contents."
  )
}
