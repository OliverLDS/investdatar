.crypto_derivatives_registry_vector <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) return(NULL)
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  x <- trimws(as.character(x))
  x[nzchar(x)]
}

.crypto_time_ms <- function(x) {
  if (is.null(x)) return(NULL)
  floor(as.numeric(as.POSIXct(x, tz = "UTC")) * 1000)
}

.check_binance_derivatives_response <- function(response) {
  if (is.list(response) && !is.data.frame(response) && !is.null(response$code)) {
    stop("Binance derivatives API error ", response$code, ": ", response$msg, call. = FALSE)
  }
  response
}

.crypto_derivatives_empty <- function() {
  data.table::data.table(
    source = character(), provider = character(), dataset_type = character(),
    symbol = character(), interval = character(), datetime = .empty_posixct(),
    date = as.Date(character()), value = numeric(), funding_rate = numeric(),
    mark_price = numeric(), open_interest = numeric(), open_interest_value = numeric()
  )
}

.standardize_crypto_derivatives <- function(data, provider, dataset_type, symbol, interval = NULL) {
  dt <- data.table::as.data.table(data)
  if (nrow(dt) == 0L) return(.crypto_derivatives_empty())
  provider <- tolower(provider)
  dataset_type <- tolower(dataset_type)
  if (provider == "binance" && dataset_type == "funding_rate") {
    required <- c("fundingTime", "fundingRate")
    if (!all(required %in% names(dt))) stop("Binance funding response is missing required columns.", call. = FALSE)
    datetime <- as.POSIXct(as.numeric(dt$fundingTime) / 1000, origin = "1970-01-01", tz = "UTC")
    funding_rate <- suppressWarnings(as.numeric(dt$fundingRate))
    mark_price <- if ("markPrice" %in% names(dt)) suppressWarnings(as.numeric(dt$markPrice)) else rep(NA_real_, nrow(dt))
    open_interest <- open_interest_value <- rep(NA_real_, nrow(dt))
    interval_value <- if (is.null(interval)) "funding" else interval
  } else if (provider == "binance" && dataset_type == "open_interest") {
    required <- c("timestamp", "sumOpenInterest", "sumOpenInterestValue")
    if (!all(required %in% names(dt))) stop("Binance open-interest response is missing required columns.", call. = FALSE)
    datetime <- as.POSIXct(as.numeric(dt$timestamp) / 1000, origin = "1970-01-01", tz = "UTC")
    funding_rate <- mark_price <- rep(NA_real_, nrow(dt))
    open_interest <- suppressWarnings(as.numeric(dt$sumOpenInterest))
    open_interest_value <- suppressWarnings(as.numeric(dt$sumOpenInterestValue))
    interval_value <- interval
  } else if (provider == "okx" && dataset_type == "funding_rate") {
    required <- c("fundingTime", "fundingRate")
    if (!all(required %in% names(dt))) stop("OKX funding response is missing required columns.", call. = FALSE)
    datetime <- as.POSIXct(as.numeric(dt$fundingTime) / 1000, origin = "1970-01-01", tz = "UTC")
    funding_rate <- suppressWarnings(as.numeric(dt$fundingRate))
    mark_price <- open_interest <- open_interest_value <- rep(NA_real_, nrow(dt))
    interval_value <- if (is.null(interval)) "funding" else interval
  } else {
    stop("Unsupported crypto derivatives provider/dataset_type: ", provider, "/", dataset_type, call. = FALSE)
  }
  value <- if (dataset_type == "funding_rate") funding_rate else open_interest
  dt[, `:=`(
    source = "crypto_derivatives", provider = provider, dataset_type = dataset_type,
    symbol = as.character(symbol), interval = as.character(interval_value),
    datetime = datetime, date = as.Date(datetime, tz = "UTC"), value = value,
    funding_rate = funding_rate, mark_price = mark_price,
    open_interest = open_interest, open_interest_value = open_interest_value
  )]
  canonical <- names(.crypto_derivatives_empty())
  data.table::setcolorder(dt, c(canonical, setdiff(names(dt), canonical)))
  data.table::setorderv(dt, "datetime")
  unique(dt, by = c("provider", "dataset_type", "symbol", "interval", "datetime"))
}

.get_binance_funding_history <- function(symbol, from = NULL, to = NULL, limit = 1000L) {
  limit <- max(1L, min(as.integer(limit), 1000L))
  start_ms <- .crypto_time_ms(from)
  end_ms <- .crypto_time_ms(to)
  pages <- list()
  repeat {
    query <- list(symbol = symbol, startTime = start_ms, endTime = end_ms, limit = limit)
    query <- query[!vapply(query, is.null, logical(1))]
    response <- .check_binance_derivatives_response(.http_get_json("https://fapi.binance.com/fapi/v1/fundingRate", query = query))
    page <- data.table::as.data.table(response)
    if (nrow(page) == 0L) break
    pages[[length(pages) + 1L]] <- page
    if (nrow(page) < limit || is.null(start_ms)) break
    next_ms <- max(as.numeric(page$fundingTime), na.rm = TRUE) + 1
    if (!is.null(end_ms) && next_ms > end_ms) break
    start_ms <- next_ms
  }
  if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
}

.get_binance_open_interest_history <- function(symbol, interval, from = NULL, to = NULL, limit = 500L) {
  limit <- max(1L, min(as.integer(limit), 500L))
  query <- list(
    symbol = symbol, period = interval, startTime = .crypto_time_ms(from),
    endTime = .crypto_time_ms(to), limit = limit
  )
  query <- query[!vapply(query, is.null, logical(1))]
  response <- .check_binance_derivatives_response(.http_get_json("https://fapi.binance.com/futures/data/openInterestHist", query = query))
  data.table::as.data.table(response)
}

.get_okx_funding_history <- function(symbol, from = NULL, to = NULL, limit = 400L) {
  limit <- max(1L, min(as.integer(limit), 400L))
  from_ms <- .crypto_time_ms(from)
  to_ms <- .crypto_time_ms(to)
  cursor <- NULL
  pages <- list()
  repeat {
    query <- list(instId = symbol, after = cursor, limit = limit)
    query <- query[!vapply(query, is.null, logical(1))]
    response <- .http_get_json("https://www.okx.com/api/v5/public/funding-rate-history", query = query)
    if (!identical(as.character(response$code), "0")) stop("OKX funding API error: ", response$msg, call. = FALSE)
    page <- data.table::as.data.table(response$data)
    if (nrow(page) == 0L) break
    times <- suppressWarnings(as.numeric(page$fundingTime))
    if (!is.null(to_ms)) page <- page[times <= to_ms]
    if (nrow(page) > 0L) pages[[length(pages) + 1L]] <- page
    oldest <- min(times, na.rm = TRUE)
    if (nrow(page) < limit || (!is.null(from_ms) && oldest <= from_ms)) break
    cursor <- as.character(oldest)
  }
  out <- if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
  if (!is.null(from_ms) && nrow(out) > 0L) out <- out[as.numeric(fundingTime) >= from_ms]
  out
}

#' Retrieve Historical Crypto Derivatives Data
#'
#' @param provider `binance` or `okx`.
#' @param dataset_type `funding_rate` or `open_interest`.
#' @param symbol Provider instrument identifier.
#' @param interval Required Binance open-interest period; otherwise a local label.
#' @param from,to Optional inclusive UTC time bounds.
#' @param limit Provider page size.
#'
#' @return A standardized `data.table` retaining provider fields.
#' @export
get_source_data_crypto_derivatives <- function(provider, dataset_type, symbol,
                                               interval = NULL, from = NULL,
                                               to = NULL, limit = NULL) {
  provider <- tolower(provider)
  dataset_type <- tolower(dataset_type)
  raw <- if (provider == "binance" && dataset_type == "funding_rate") {
    .get_binance_funding_history(symbol, from, to, limit %||% 1000L)
  } else if (provider == "binance" && dataset_type == "open_interest") {
    if (is.null(interval) || !nzchar(interval)) stop("Binance open-interest history requires interval.", call. = FALSE)
    .get_binance_open_interest_history(symbol, interval, from, to, limit %||% 500L)
  } else if (provider == "okx" && dataset_type == "funding_rate") {
    .get_okx_funding_history(symbol, from, to, limit %||% 400L)
  } else {
    stop("Unsupported crypto derivatives provider/dataset_type: ", provider, "/", dataset_type, call. = FALSE)
  }
  .standardize_crypto_derivatives(raw, provider, dataset_type, symbol, interval)
}

#' Get Latest Crypto Derivatives Observation Time
#'
#' @inheritParams get_source_data_crypto_derivatives
#'
#' @return UTC `POSIXct`, or `NULL`.
#' @export
get_source_utime_crypto_derivatives <- function(provider, dataset_type, symbol, interval = NULL) {
  provider <- tolower(provider)
  dataset_type <- tolower(dataset_type)
  if (provider == "okx" && dataset_type == "funding_rate") {
    response <- .http_get_json(
      "https://www.okx.com/api/v5/public/funding-rate-history",
      query = list(instId = symbol, limit = 1L)
    )
    if (!identical(as.character(response$code), "0")) stop("OKX funding API error: ", response$msg, call. = FALSE)
    dt <- .standardize_crypto_derivatives(response$data, provider, dataset_type, symbol, interval)
  } else {
    dt <- get_source_data_crypto_derivatives(provider, dataset_type, symbol, interval, limit = 1L)
  }
  if (nrow(dt) == 0L) return(NULL)
  max(dt$datetime, na.rm = TRUE)
}

.crypto_derivatives_local_file <- function(provider, dataset_type, symbol, interval, local_path) {
  id <- paste(provider, dataset_type, symbol, interval %||% "funding", sep = "__")
  file.path(local_path, paste0(gsub("[^A-Za-z0-9._-]+", "_", id), ".rds"))
}

#' Read Local Crypto Derivatives Data
#'
#' @inheritParams get_source_data_crypto_derivatives
#' @param local_path Optional derivatives storage directory.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_crypto_derivatives <- function(provider, dataset_type, symbol, interval = NULL, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("crypto", subdir = "derivatives")
  .read_local_data_table(.crypto_derivatives_local_file(provider, dataset_type, symbol, interval, local_path), sort_cols = "datetime")
}

#' Synchronize One Crypto Derivatives Dataset
#'
#' @inheritParams get_source_data_crypto_derivatives
#' @param local_path Optional derivatives storage directory.
#' @param overlap_days Days re-fetched before the latest local observation.
#'
#' @return A local synchronization result list.
#' @export
sync_local_crypto_derivatives <- function(provider, dataset_type, symbol,
                                          interval = NULL, from = NULL, to = NULL,
                                          limit = NULL, local_path = NULL,
                                          overlap_days = 2L) {
  if (is.null(local_path)) local_path <- get_source_data_path("crypto", subdir = "derivatives", create = TRUE)
  local_file <- .crypto_derivatives_local_file(provider, dataset_type, symbol, interval, local_path)
  local_dt <- .safe_read_rds(local_file, default = NULL)
  sync_from <- from
  if (!is.null(local_dt) && nrow(local_dt) > 0L) {
    overlap_from <- max(local_dt$datetime, na.rm = TRUE) - as.difftime(overlap_days, units = "days")
    sync_from <- if (is.null(sync_from)) overlap_from else max(as.POSIXct(sync_from, tz = "UTC"), overlap_from)
  }
  new_dt <- get_source_data_crypto_derivatives(provider, dataset_type, symbol, interval, sync_from, to, limit)
  source_utime <- if (nrow(new_dt) == 0L) NULL else max(new_dt$datetime, na.rm = TRUE)
  sync_local_data(
    new_dt, local_file,
    key_cols = c("provider", "dataset_type", "symbol", "interval", "datetime"),
    order_cols = "datetime", source_utime = source_utime
  )
}

#' Get Crypto Derivatives Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback.
#'
#' @return Character scalar path.
#' @export
get_crypto_derivatives_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("crypto"), error = function(e) list())
  if (!is.null(cfg$derivatives_registry_file) && nzchar(cfg$derivatives_registry_file)) return(.normalize_scalar_path(cfg$derivatives_registry_file, config_dir = getOption("investdatar.config_dir")))
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) stop("No crypto derivatives registry path is configured. Set Crypto.derivatives_registry_file.", call. = FALSE)
  file.path(config_dir, "crypto_derivatives_registry.json")
}

#' Get Crypto Derivatives Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return A registry `data.table`.
#' @export
get_crypto_derivatives_registry <- function(registry_path = get_crypto_derivatives_registry_file_path()) {
  .read_json_registry(registry_path, empty_cols = c("provider", "dataset_type", "symbol", "interval", "start", "label", "active"))
}

#' Synchronize All Registered Crypto Derivatives Data
#'
#' @param registry Optional derivatives registry.
#' @param local_path Optional derivatives storage directory.
#' @param ... Passed to `sync_local_crypto_derivatives()`.
#'
#' @return A standardized batch summary `data.table`.
#' @export
sync_all_crypto_derivatives_registry_data <- function(registry = get_crypto_derivatives_registry(), local_path = NULL, ...) {
  stopifnot(all(c("provider", "dataset_type", "symbol") %in% names(registry)))
  if (is.null(local_path)) local_path <- get_source_data_path("crypto", subdir = "derivatives", create = TRUE)
  run_started_at <- Sys.time()
  if ("active" %in% names(registry)) {
    active_flag <- tolower(as.character(registry$active))
    registry <- registry[is.na(active_flag) | active_flag %in% c("true", "1", "yes", "y")]
  }
  rows <- lapply(seq_len(nrow(registry)), function(i) {
    provider <- registry$provider[[i]]
    dataset_type <- registry$dataset_type[[i]]
    symbol <- registry$symbol[[i]]
    interval <- if ("interval" %in% names(registry) && !is.na(registry$interval[[i]]) && nzchar(registry$interval[[i]])) registry$interval[[i]] else NULL
    start <- if ("start" %in% names(registry) && !is.na(registry$start[[i]]) && nzchar(registry$start[[i]])) registry$start[[i]] else NULL
    tryCatch({
      res <- sync_local_crypto_derivatives(provider, dataset_type, symbol, interval, from = start, local_path = local_path, ...)
      data.table::data.table(
        provider = provider, dataset_type = dataset_type, symbol = symbol, interval = interval %||% "funding",
        status = "success", updated = isTRUE(res$updated), n_rows = res$n_rows %||% NA_integer_,
        n_new_rows = res$n_new_rows %||% NA_integer_, error = NA_character_
      )
    }, error = function(e) data.table::data.table(
      provider = provider, dataset_type = dataset_type, symbol = symbol, interval = interval %||% "funding",
      status = "error", updated = FALSE, n_rows = NA_integer_, n_new_rows = NA_integer_,
      error = conditionMessage(e), error_class = class(e)[[1L]],
      http_status = if (inherits(e, "investdatar_http_error")) e$status_code else NA_integer_
    ))
  })
  run_finished_at <- Sys.time()
  summary_dt <- .normalize_sync_summary(
    data.table::rbindlist(rows, use.names = TRUE, fill = TRUE), "crypto_derivatives",
    run_started_at, run_finished_at
  )
  .write_sync_run_log("crypto_derivatives", summary_dt, local_path, params = list(), run_started_at, run_finished_at)
  summary_dt
}

#' Describe Local Crypto Derivatives Data
#'
#' @inheritParams get_local_crypto_derivatives
#'
#' @return Character scalar narrative.
#' @export
describe_crypto_derivatives <- function(provider, dataset_type, symbol, interval = NULL, local_path = NULL) {
  dt <- get_local_crypto_derivatives(provider, dataset_type, symbol, interval, local_path)
  if (is.null(dt) || nrow(dt) == 0L) stop("Local crypto derivatives data not found.", call. = FALSE)
  paste(
    sprintf("This object is a %s %s data.table for %s.", provider, dataset_type, symbol),
    sprintf("It contains %s observations at interval %s.", nrow(dt), paste(unique(dt$interval), collapse = ", ")),
    .describe_time_coverage(dt$datetime),
    "The canonical value is accompanied by dataset-specific funding-rate, mark-price, or open-interest fields and retained provider columns."
  )
}
