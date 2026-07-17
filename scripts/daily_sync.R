#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)

has_flag <- function(...) {
  any(unlist(list(...)) %in% args)
}

arg_value <- function(flag, default = NULL) {
  idx <- match(flag, args)
  if (is.na(idx) || idx == length(args)) {
    return(default)
  }
  args[[idx + 1L]]
}

script_path <- function() {
  file_arg <- grep("^--file=", commandArgs(FALSE), value = TRUE)
  if (length(file_arg) == 0L) {
    return(normalizePath("scripts/daily_sync.R", mustWork = FALSE))
  }
  normalizePath(sub("^--file=", "", file_arg[[1L]]), mustWork = FALSE)
}

print_help <- function() {
  cat("Run investdatar local sync nodes in the standard daily workflow.\n\n")
  cat("Usage: Rscript scripts/daily_sync.R [--config PATH] [--force] [--compact]\n\n")
  cat("Flags:\n")
  cat("  -h, --help     Show this help text.\n")
  cat("  --config PATH  Pass this investdatar YAML config to each sync node.\n")
  cat("  --force        Ask every node to sync even when its run log is fresh.\n")
  cat("  --compact      Emit compact JSON instead of pretty JSON.\n\n")
  cat("Side effects:\n")
  cat("  Calls the provider-specific sync nodes in scripts/sync_*.R. Each node may\n")
  cat("  update its configured local cache and write a provider _sync_runs log.\n")
}

if (has_flag("-h", "--help")) {
  print_help()
  quit(save = "no", status = 0L)
}

if (!requireNamespace("jsonlite", quietly = TRUE)) {
  stop("Package 'jsonlite' is required for node JSON output.", call. = FALSE)
}

script_dir <- dirname(script_path())
node_files <- c(
  "sync_ishare_holdings.R",
  "sync_rss.R",
  "sync_treasury.R",
  "sync_yahoofinance.R",
  "sync_fred.R",
  "sync_ishare.R",
  "sync_wbstats.R"
)

config_path <- arg_value("--config", Sys.getenv("INVESTDATAR_CONFIG", unset = ""))
node_args <- character()
if (nzchar(config_path)) {
  node_args <- c(node_args, "--config", config_path)
}
if (has_flag("--force")) {
  node_args <- c(node_args, "--force")
}
node_args <- c(node_args, "--compact")

run_node <- function(node_file) {
  node_path <- file.path(script_dir, node_file)
  started_at <- Sys.time()
  stderr_path <- tempfile("investdatar-sync-node-", fileext = ".stderr")
  on.exit(unlink(stderr_path), add = TRUE)

  stdout <- tryCatch(
    system2("Rscript", c(node_path, node_args), stdout = TRUE, stderr = stderr_path),
    error = function(e) {
      structure(character(), status = 1L, error = conditionMessage(e))
    }
  )
  status <- attr(stdout, "status")
  if (is.null(status)) {
    status <- 0L
  }
  stderr <- if (file.exists(stderr_path)) readLines(stderr_path, warn = FALSE) else character()
  payload <- tryCatch(
    jsonlite::fromJSON(paste(stdout, collapse = "\n"), simplifyVector = FALSE),
    error = function(e) NULL
  )

  list(
    node = sub("\\.R$", "", node_file),
    script = normalizePath(node_path, winslash = "/", mustWork = FALSE),
    success = identical(as.integer(status), 0L) && !is.null(payload) && isTRUE(payload$success),
    exit_status = as.integer(status),
    payload = payload,
    stderr = stderr,
    started_at = format(as.POSIXct(started_at, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    finished_at = format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  )
}

started_at <- Sys.time()
results <- lapply(node_files, run_node)
success <- all(vapply(results, function(x) isTRUE(x$success), logical(1)))

out <- list(
  success = success,
  node = "daily_sync",
  results = results,
  error = if (success) NULL else "one_or_more_sync_nodes_failed",
  started_at = format(as.POSIXct(started_at, tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
  finished_at = format(as.POSIXct(Sys.time(), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
)

cat(jsonlite::toJSON(out, auto_unbox = TRUE, pretty = !has_flag("--compact"), null = "null", na = "null"))
cat("\n")
quit(save = "no", status = if (success) 0L else 1L)
