.sec_bulk_archives <- c(
  companyfacts = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip",
  submissions = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
)

#' Download An Optional SEC Bulk Archive
#'
#' @param archive One of `companyfacts` or `submissions`.
#' @param local_path Optional SEC bulk archive directory.
#' @param config Optional SEC configuration.
#' @param overwrite Replace the existing ZIP.
#' @param extract Extract the ZIP into a same-named directory.
#' @return Download metadata.
#' @export
sync_local_sec_bulk_archive <- function(archive = c("companyfacts", "submissions"),
                                        local_path = NULL, config = NULL,
                                        overwrite = FALSE, extract = FALSE) {
  archive <- match.arg(archive)
  config <- .sec_api_config(config)
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "bulk", create = TRUE)
  file_path <- file.path(local_path, paste0(archive, ".zip"))
  updated <- FALSE
  if (!file.exists(file_path) || isTRUE(overwrite)) {
    .safe_write_raw(.sec_get_raw(.sec_bulk_archives[[archive]], config = config, accept = "application/zip"), file_path)
    updated <- TRUE
  }
  extract_path <- NA_character_
  if (isTRUE(extract)) {
    extract_path <- file.path(local_path, archive)
    dir.create(extract_path, recursive = TRUE, showWarnings = FALSE)
    utils::unzip(file_path, exdir = extract_path)
  }
  info <- file.info(file_path)
  data.table::data.table(
    archive = archive, source_url = .sec_bulk_archives[[archive]], file_path = normalizePath(file_path, mustWork = FALSE),
    extract_path = extract_path, bytes = as.numeric(info$size), updated = updated,
    local_updated_at = as.POSIXct(info$mtime, tz = "UTC")
  )
}
