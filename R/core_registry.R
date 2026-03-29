.read_json_registry <- function(path, empty_cols = character()) {
  if (!file.exists(path)) {
    out <- data.table::data.table()
    for (nm in empty_cols) {
      out[, (nm) := character()]
    }
    return(out[])
  }

  dt <- data.table::as.data.table(jsonlite::fromJSON(path, simplifyDataFrame = TRUE))
  if (length(empty_cols) > 0L) {
    for (nm in empty_cols) {
      if (!nm %in% names(dt)) {
        dt[, (nm) := character()]
      }
    }
  }
  dt[]
}

.write_json_registry <- function(dt, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  jsonlite::write_json(as.data.frame(dt), path = path, pretty = TRUE, auto_unbox = TRUE, null = "null")
  invisible(path)
}

.align_registry_schema <- function(dt, template_names) {
  dt <- data.table::as.data.table(dt)
  for (nm in template_names) {
    if (!nm %in% names(dt)) {
      dt[, (nm) := NA_character_]
    }
  }
  data.table::setcolorder(dt, c(template_names, setdiff(names(dt), template_names)))
  dt[]
}

.prompt_stdin_value <- function(prompt_text, hints = NULL) {
  if (!is.null(hints) && length(hints) > 0L) {
    cat("Hints:", paste(sort(unique(hints)), collapse = ", "), "\n")
  }
  cat(prompt_text)
  trimws(readLines(stdin(), n = 1L))
}

.confirm_stdin <- function(prompt_text) {
  cat(prompt_text)
  answer <- tolower(trimws(readLines(stdin(), n = 1L)))
  answer %in% c("y", "yes")
}
