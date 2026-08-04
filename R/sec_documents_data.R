.sec_filing_document_url <- function(cik, accession_number, primary_document, config) {
  accession <- gsub("-", "", accession_number, fixed = TRUE)
  sprintf(
    "%s/Archives/edgar/data/%s/%s/%s",
    sub("/+$", "", config$website_url), .normalize_sec_cik(cik), accession,
    utils::URLencode(basename(primary_document), reserved = TRUE)
  )
}

#' Download A Selected SEC Filing Document
#'
#' @param cik SEC Central Index Key.
#' @param accession_number Filing accession number.
#' @param primary_document Primary filing document name from submissions data.
#' @param local_path Optional document cache directory.
#' @param config Optional SEC configuration.
#' @param overwrite Replace an existing local document.
#' @return A one-row metadata `data.table`.
#' @export
sync_local_sec_filing_document <- function(cik, accession_number, primary_document,
                                           local_path = NULL, config = NULL,
                                           overwrite = FALSE) {
  config <- .sec_api_config(config)
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "documents", create = TRUE)
  accession <- gsub("-", "", accession_number, fixed = TRUE)
  file_path <- file.path(local_path, paste0("CIK", .normalize_sec_cik(cik, TRUE)), accession, basename(primary_document))
  updated <- FALSE
  if (!file.exists(file_path) || isTRUE(overwrite)) {
    raw <- .sec_get_raw(.sec_filing_document_url(cik, accession_number, primary_document, config), config = config)
    .safe_write_raw(raw, file_path)
    updated <- TRUE
  }
  info <- file.info(file_path)
  data.table::data.table(
    cik = .normalize_sec_cik(cik), accession_number = as.character(accession_number),
    primary_document = basename(primary_document), file_path = normalizePath(file_path, mustWork = FALSE),
    bytes = as.numeric(info$size), updated = updated, local_updated_at = as.POSIXct(info$mtime, tz = "UTC")
  )
}

#' Download Selected Documents From Local SEC Submissions
#'
#' @param cik SEC Central Index Key.
#' @param forms Optional filing-form filter.
#' @param from,to Optional filing-date bounds.
#' @param limit Maximum number of newest documents.
#' @param submissions_path Optional submissions cache directory.
#' @param local_path Optional document cache directory.
#' @param config Optional SEC configuration.
#' @return One metadata row per selected document.
#' @export
sync_sec_filing_documents <- function(cik, forms = NULL, from = NULL, to = NULL, limit = 20L,
                                      submissions_path = NULL, local_path = NULL, config = NULL) {
  dt <- get_local_sec_submissions(cik, local_path = submissions_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local SEC submissions are required before document retrieval.", call. = FALSE)
  if (!is.null(forms)) dt <- dt[form %in% forms]
  if (!is.null(from)) dt <- dt[filing_date >= as.Date(from)]
  if (!is.null(to)) dt <- dt[filing_date <= as.Date(to)]
  dt <- dt[!is.na(dt$primary_document) & nzchar(dt$primary_document)]
  data.table::setorderv(dt, c("filing_date", "acceptance_datetime"), order = -1L, na.last = TRUE)
  dt <- utils::head(dt, max(0L, as.integer(limit)))
  rows <- lapply(seq_len(nrow(dt)), function(i) {
    sync_local_sec_filing_document(
      cik, dt$accession_number[[i]], dt$primary_document[[i]],
      local_path = local_path, config = config
    )
  })
  data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
}
