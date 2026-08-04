.sec_frame_url <- function(taxonomy, tag, unit, period, config) {
  sprintf(
    "%s/api/xbrl/frames/%s/%s/%s/%s.json",
    sub("/+$", "", config$data_url), taxonomy, tag, unit, period
  )
}

.standardize_sec_frame <- function(response, taxonomy, tag, unit, period) {
  rows <- response$data
  if (is.null(rows) || length(rows) == 0L) return(data.table::data.table())
  dt <- data.table::as.data.table(rows)
  aliases <- c(accn = "accession_number", entityName = "entity_name", loc = "location", val = "value")
  for (old in names(aliases)) {
    if (old %in% names(dt) && !aliases[[old]] %in% names(dt)) data.table::setnames(dt, old, aliases[[old]])
  }
  defaults <- list(
    accession_number = NA_character_, cik = NA_character_, entity_name = NA_character_,
    location = NA_character_, start = NA_character_, end = NA_character_, value = NA_real_,
    fy = NA_integer_, fp = NA_character_, form = NA_character_, filed = NA_character_
  )
  for (nm in names(defaults)) if (!nm %in% names(dt)) dt[, (nm) := defaults[[nm]]]
  dt[, `:=`(
    taxonomy = as.character(taxonomy), concept = as.character(tag), unit = as.character(unit),
    frame = as.character(period), cik = as.character(cik),
    start = as.Date(start), end = as.Date(end), filed = as.Date(filed),
    value = suppressWarnings(as.numeric(value)), retrieved_at = as.POSIXct(Sys.time(), tz = "UTC")
  )]
  cols <- c("taxonomy", "concept", "unit", "frame", "cik", "entity_name", "location",
            "start", "end", "value", "accession_number", "fy", "fp", "form", "filed", "retrieved_at")
  data.table::setcolorder(dt, c(cols, setdiff(names(dt), cols)))
  data.table::setorderv(dt, c("end", "cik", "accession_number"), na.last = TRUE)
  dt[]
}

#' Retrieve An SEC XBRL Frame
#'
#' @param taxonomy XBRL taxonomy, such as `us-gaap`.
#' @param tag XBRL concept tag.
#' @param unit XBRL unit, such as `USD`.
#' @param period SEC frame period, such as `CY2025Q4I`.
#' @param config Optional SEC configuration.
#' @return A normalized cross-company `data.table`.
#' @export
get_source_data_sec_frame <- function(taxonomy, tag, unit, period, config = NULL) {
  config <- .sec_api_config(config)
  response <- .sec_get_json(.sec_frame_url(taxonomy, tag, unit, period, config), config = config)
  .standardize_sec_frame(response, taxonomy, tag, unit, period)
}

.sec_frame_id <- function(taxonomy, tag, unit, period) {
  paste(taxonomy, tag, unit, period, sep = "__")
}

.sec_frame_local_file <- function(taxonomy, tag, unit, period, local_path) {
  file.path(local_path, paste0(gsub("[^A-Za-z0-9_.-]", "_", .sec_frame_id(taxonomy, tag, unit, period)), ".rds"))
}

#' Read A Local SEC XBRL Frame
#' @inheritParams get_source_data_sec_frame
#' @param local_path Optional SEC frame cache directory.
#' @return A cached `data.table`, or `NULL`.
#' @export
get_local_sec_frame <- function(taxonomy, tag, unit, period, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "frames")
  .read_local_data_table(.sec_frame_local_file(taxonomy, tag, unit, period, local_path), sort_cols = c("end", "cik"))
}

#' Synchronize One SEC XBRL Frame
#' @inheritParams get_source_data_sec_frame
#' @param local_path Optional SEC frame cache directory.
#' @return A standard synchronization result.
#' @export
sync_local_sec_frame <- function(taxonomy, tag, unit, period, config = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "frames", create = TRUE)
  new_data <- get_source_data_sec_frame(taxonomy, tag, unit, period, config = config)
  sync_local_data(
    new_data = new_data,
    local_file_path = .sec_frame_local_file(taxonomy, tag, unit, period, local_path),
    key_cols = c("taxonomy", "concept", "unit", "frame", "cik", "accession_number"),
    order_cols = c("end", "cik", "accession_number"),
    source_utime = if (nrow(new_data) && any(!is.na(new_data$filed))) max(new_data$filed, na.rm = TRUE) else Sys.time()
  )
}

#' Get SEC Frames Registry File Path
#' @param config_dir Optional configuration directory.
#' @return Character scalar path.
#' @export
get_sec_frames_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("sec"), error = function(e) list())
  if (!is.null(cfg$frames_registry_file) && nzchar(cfg$frames_registry_file)) return(.normalize_scalar_path(cfg$frames_registry_file, getOption("investdatar.config_dir")))
  config_dir <- config_dir %||% getOption("investdatar.config_dir")
  if (is.null(config_dir)) stop("No SEC Frames registry path is configured.", call. = FALSE)
  file.path(config_dir, "sec_frames_registry.json")
}

#' Get SEC Frames Registry
#' @param registry_path Optional registry path.
#' @return A registry `data.table`.
#' @export
get_sec_frames_registry <- function(registry_path = get_sec_frames_registry_file_path()) {
  .read_json_registry(registry_path, c("taxonomy", "tag", "unit", "period", "label", "active"))
}

#' Synchronize Registered SEC XBRL Frames
#' @param registry Optional Frames registry.
#' @param config Optional SEC configuration.
#' @param local_path Optional frame storage directory.
#' @return A standardized batch summary.
#' @export
sync_all_sec_frames_registry_data <- function(registry = get_sec_frames_registry(), config = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("sec", subdir = "frames", create = TRUE)
  started <- Sys.time(); registry <- registry[tolower(as.character(registry$active)) %in% c("true", "1", "yes", "y")]
  rows <- lapply(seq_len(nrow(registry)), function(i) tryCatch({
    res <- sync_local_sec_frame(registry$taxonomy[[i]], registry$tag[[i]], registry$unit[[i]], registry$period[[i]], config, local_path)
    data.table::data.table(frame_id = .sec_frame_id(registry$taxonomy[[i]], registry$tag[[i]], registry$unit[[i]], registry$period[[i]]),
                           status = "success", updated = isTRUE(res$updated), n_rows = res$n_rows, n_new_rows = res$n_new_rows)
  }, error = function(e) data.table::data.table(
    frame_id = .sec_frame_id(registry$taxonomy[[i]], registry$tag[[i]], registry$unit[[i]], registry$period[[i]]),
    status = "error", updated = FALSE, error = conditionMessage(e)
  )))
  finished <- Sys.time(); summary <- .normalize_sync_summary(data.table::rbindlist(rows, fill = TRUE), "sec_frames", started, finished)
  .write_sync_run_log("sec_frames", summary, local_path, list(), started, finished); summary
}

#' Describe A Local SEC XBRL Frame
#' @inheritParams get_local_sec_frame
#' @return Character scalar narrative.
#' @export
describe_sec_frame <- function(taxonomy, tag, unit, period, local_path = NULL) {
  dt <- get_local_sec_frame(taxonomy, tag, unit, period, local_path)
  if (is.null(dt) || !nrow(dt)) stop("Local SEC Frame not found.", call. = FALSE)
  paste(sprintf("SEC XBRL Frame %s contains %s cross-company facts.", .sec_frame_id(taxonomy, tag, unit, period), nrow(dt)), .describe_time_coverage(dt$end), .describe_value_summary(dt$value))
}
