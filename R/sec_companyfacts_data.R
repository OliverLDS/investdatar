.sec_companyfacts_url <- function(cik, config) {
  sprintf("%s/api/xbrl/companyfacts/CIK%s.json", sub("/+$", "", config$data_url), .normalize_sec_cik(cik, padded = TRUE))
}

.sec_fact_key <- function(dt) {
  fields <- c("taxonomy", "concept", "unit", "accession_number", "start", "end", "frame", "filed", "form", "fiscal_year", "fiscal_period")
  values <- lapply(fields, function(nm) {
    x <- as.character(dt[[nm]])
    x[is.na(x)] <- "<NA>"
    x
  })
  do.call(paste, c(values, sep = "|"))
}

.standardize_sec_companyfacts <- function(response, cik, ticker = NULL, company_name = NULL,
                                          concepts = NULL, forms = NULL,
                                          from = NULL, to = NULL) {
  cik_value <- .normalize_sec_cik(cik)
  ticker_value <- if (is.null(ticker)) NA_character_ else toupper(ticker)
  company_name_value <- if (is.null(company_name)) NA_character_ else as.character(company_name)
  rows <- list()
  facts <- response$facts
  concepts <- .sec_registry_values(concepts)
  forms <- .sec_registry_values(forms)
  if (!is.null(facts) && length(facts) > 0L) {
    for (taxonomy in names(facts)) {
      taxonomy_facts <- facts[[taxonomy]]
      for (concept in names(taxonomy_facts)) {
        if (!is.null(concepts) && !concept %in% concepts && !paste(taxonomy, concept, sep = ":") %in% concepts) next
        fact <- taxonomy_facts[[concept]]
        for (unit in names(fact$units)) {
          unit_dt <- data.table::as.data.table(fact$units[[unit]])
          if (nrow(unit_dt) == 0L) next
          unit_dt[, `:=`(
            taxonomy = taxonomy, concept = concept,
            concept_label = as.character(fact$label),
            concept_description = as.character(fact$description),
            unit = unit
          )]
          rows[[length(rows) + 1L]] <- unit_dt
        }
      }
    }
  }
  empty <- data.table::data.table(
    source = character(), cik = character(), ticker = character(), company_name = character(),
    taxonomy = character(), concept = character(), concept_label = character(),
    concept_description = character(), unit = character(), start = as.Date(character()),
    end = as.Date(character()), value = numeric(), accession_number = character(),
    fiscal_year = integer(), fiscal_period = character(), form = character(),
    filed = as.Date(character()), frame = character(), fact_key = character()
  )
  if (length(rows) == 0L) return(empty)
  dt <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  rename <- c(val = "value", accn = "accession_number", fy = "fiscal_year", fp = "fiscal_period")
  for (old in intersect(names(rename), names(dt))) data.table::setnames(dt, old, rename[[old]])
  for (nm in setdiff(names(empty), names(dt))) {
    data.table::set(dt, j = nm, value = .sec_missing_column(empty[[nm]], nrow(dt)))
  }
  dt[, `:=`(
    source = "sec", cik = cik_value,
    ticker = ticker_value,
    company_name = company_name_value,
    start = as.Date(start), end = as.Date(end), filed = as.Date(filed),
    value = suppressWarnings(as.numeric(value)),
    fiscal_year = suppressWarnings(as.integer(fiscal_year))
  )]
  if (!is.null(forms)) dt <- dt[form %in% forms]
  if (!is.null(from)) dt <- dt[filed >= as.Date(from)]
  if (!is.null(to)) dt <- dt[filed <= as.Date(to)]
  dt[, fact_key := .sec_fact_key(.SD)]
  data.table::setcolorder(dt, c(names(empty), setdiff(names(dt), names(empty))))
  data.table::setorderv(dt, c("filed", "taxonomy", "concept", "unit", "fact_key"))
  unique(dt, by = c("cik", "fact_key"))
}

#' Retrieve SEC Company Facts
#'
#' @param cik SEC Central Index Key.
#' @param ticker Optional ticker label.
#' @param company_name Optional company name label.
#' @param concepts Optional concept names or `taxonomy:concept` identifiers.
#' @param forms Optional filing forms to retain.
#' @param from,to Optional inclusive filed-date bounds.
#' @param config Optional SEC configuration.
#'
#' @return A standardized long XBRL fact `data.table`.
#' @export
get_source_data_sec_companyfacts <- function(cik, ticker = NULL, company_name = NULL,
                                             concepts = NULL, forms = NULL,
                                             from = NULL, to = NULL, config = NULL) {
  config <- .sec_api_config(config)
  response <- .sec_get_json(.sec_companyfacts_url(cik, config), config = config)
  .standardize_sec_companyfacts(
    response, cik = cik,
    ticker = if (is.null(ticker)) response$tickers[[1L]] else ticker,
    company_name = if (is.null(company_name)) response$entityName else company_name,
    concepts = concepts, forms = forms, from = from, to = to
  )
}

#' Get Latest SEC Company Facts Filing Date
#'
#' @param cik SEC Central Index Key.
#' @param config Optional SEC configuration.
#'
#' @return A UTC `POSIXct`, or `NULL`.
#' @export
get_source_utime_sec_companyfacts <- function(cik, config = NULL) {
  dt <- get_source_data_sec_companyfacts(cik, config = config)
  if (nrow(dt) == 0L || all(is.na(dt$filed))) return(NULL)
  as.POSIXct(max(dt$filed, na.rm = TRUE), tz = "UTC")
}

.sec_companyfacts_local_file <- function(cik, local_path) {
  file.path(local_path, paste0("CIK", .normalize_sec_cik(cik, padded = TRUE), ".rds"))
}

#' Read Local SEC Company Facts
#'
#' @param cik SEC Central Index Key.
#' @param local_path Optional SEC Company Facts storage directory.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_sec_companyfacts <- function(cik, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "companyfacts")
  .read_local_data_table(.sec_companyfacts_local_file(cik, local_path), sort_cols = c("filed", "taxonomy", "concept", "unit", "fact_key"))
}

#' Synchronize SEC Company Facts For One Company
#'
#' @inheritParams get_source_data_sec_companyfacts
#' @param local_path Optional SEC Company Facts storage directory.
#' @param overlap_days Filed-date overlap used for local upserts.
#'
#' @return A local synchronization result list.
#' @export
sync_local_sec_companyfacts <- function(cik, ticker = NULL, company_name = NULL,
                                        concepts = NULL, forms = NULL,
                                        from = NULL, to = NULL, config = NULL,
                                        local_path = NULL, overlap_days = 31L) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "companyfacts", create = TRUE)
  local_file <- .sec_companyfacts_local_file(cik, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- if (is.null(from)) NULL else as.Date(from)
  if (!is.null(local_dt) && nrow(local_dt) > 0L && "filed" %in% names(local_dt)) {
    overlap_from <- max(local_dt$filed, na.rm = TRUE) - as.integer(overlap_days)
    sync_from <- if (is.null(sync_from)) overlap_from else max(sync_from, overlap_from)
  }
  new_dt <- get_source_data_sec_companyfacts(
    cik = cik, ticker = ticker, company_name = company_name,
    concepts = concepts, forms = forms, from = sync_from, to = to, config = config
  )
  source_utime <- if (nrow(new_dt) == 0L || all(is.na(new_dt$filed))) NULL else as.POSIXct(max(new_dt$filed, na.rm = TRUE), tz = "UTC")
  sync_local_data(
    new_data = new_dt, local_file_path = local_file,
    key_cols = c("cik", "fact_key"),
    order_cols = c("filed", "taxonomy", "concept", "unit", "fact_key"),
    source_utime = source_utime
  )
}

#' Synchronize Registered SEC Company Facts
#'
#' @param registry Optional SEC company registry.
#' @param config Optional SEC configuration.
#' @param local_path Optional SEC Company Facts storage directory.
#' @param ... Passed to `sync_local_sec_companyfacts()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_sec_companyfacts_registry_data <- function(registry = get_sec_registry(), config = NULL, local_path = NULL, ...) {
  stopifnot(all(c("ticker", "cik") %in% names(registry)))
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "companyfacts", create = TRUE)
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
    concepts <- if ("concepts" %in% names(registry)) .sec_registry_values(registry$concepts[[i]]) else NULL
    tryCatch({
      res <- sync_local_sec_companyfacts(
        cik = cik, ticker = ticker, company_name = company_name,
        concepts = concepts, forms = forms, config = config, local_path = local_path, ...
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
    source_id = "sec_companyfacts", run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  .write_sync_run_log(
    source_id = "sec_companyfacts", summary = summary_dt, local_path = local_path,
    params = list(), run_started_at = run_started_at, run_finished_at = run_finished_at
  )
  summary_dt
}

#' Describe Local SEC Company Facts
#'
#' @param cik SEC Central Index Key.
#' @param local_path Optional SEC Company Facts storage directory.
#'
#' @return Character scalar narrative.
#' @export
describe_sec_companyfacts <- function(cik, local_path = NULL) {
  dt <- get_local_sec_companyfacts(cik, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local SEC Company Facts not found for CIK: ", cik, call. = FALSE)
  paste(
    sprintf("This object is a long data.table of SEC XBRL Company Facts for CIK %s.", .normalize_sec_cik(cik)),
    sprintf("The table contains %s facts across %s concepts and %s units.", nrow(dt), data.table::uniqueN(dt$concept), data.table::uniqueN(dt$unit)),
    .describe_time_coverage(dt$filed),
    "Each fact retains taxonomy, concept, unit, reporting context, accession number, filing form, fiscal labels, and frame. fact_key distinguishes amendments and repeated contexts."
  )
}
