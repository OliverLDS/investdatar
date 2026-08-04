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
    mark_price = numeric(), index_price = numeric(), basis = numeric(),
    basis_rate = numeric(), annualized_basis_rate = numeric(),
    open_interest = numeric(), open_interest_value = numeric(),
    long_short_ratio = numeric(), long_account = numeric(), short_account = numeric(),
    liquidation_price = numeric(), liquidation_quantity = numeric()
  )
}

.crypto_datetime <- function(x) {
  if (inherits(x, "POSIXt")) return(as.POSIXct(x, tz = "UTC"))
  value <- suppressWarnings(as.numeric(x))
  if (length(value) > 0L && any(value > 1e11, na.rm = TRUE)) value <- value / 1000
  as.POSIXct(value, origin = "1970-01-01", tz = "UTC")
}

.crypto_numeric <- function(dt, name) {
  if (!name %in% names(dt)) return(rep(NA_real_, nrow(dt)))
  suppressWarnings(as.numeric(dt[[name]]))
}

.standardize_crypto_derivatives <- function(data, provider, dataset_type, symbol, interval = NULL) {
  dt <- data.table::as.data.table(data)
  if (nrow(dt) == 0L) return(.crypto_derivatives_empty())
  provider <- tolower(provider)
  dataset_type <- tolower(dataset_type)
  funding_rate <- mark_price <- index_price <- basis <- basis_rate <-
    annualized_basis_rate <- open_interest <- open_interest_value <-
    long_short_ratio <- long_account <- short_account <- liquidation_price <-
    liquidation_quantity <- rep(NA_real_, nrow(dt))
  if (dataset_type == "funding_rate") {
    required <- c("fundingTime", "fundingRate")
    if (!all(required %in% names(dt))) stop("Funding response is missing required columns.", call. = FALSE)
    datetime <- .crypto_datetime(dt$fundingTime)
    funding_rate <- .crypto_numeric(dt, "fundingRate")
    mark_price <- .crypto_numeric(dt, "markPrice")
    interval_value <- if (is.null(interval)) "funding" else interval
    value <- funding_rate
  } else if (dataset_type == "open_interest") {
    required <- c("timestamp", "sumOpenInterest", "sumOpenInterestValue")
    if (!all(required %in% names(dt))) stop("Open-interest response is missing required columns.", call. = FALSE)
    datetime <- .crypto_datetime(dt$timestamp)
    open_interest <- .crypto_numeric(dt, "sumOpenInterest")
    open_interest_value <- .crypto_numeric(dt, "sumOpenInterestValue")
    interval_value <- interval
    value <- open_interest
  } else if (dataset_type %in% c("mark_price", "index_price")) {
    if (!"datetime" %in% names(dt) || !"close" %in% names(dt)) stop("Price history is missing datetime/close columns.", call. = FALSE)
    datetime <- .crypto_datetime(dt$datetime)
    if (dataset_type == "mark_price") mark_price <- .crypto_numeric(dt, "close") else index_price <- .crypto_numeric(dt, "close")
    interval_value <- interval
    value <- if (dataset_type == "mark_price") mark_price else index_price
  } else if (dataset_type == "basis") {
    if (!"timestamp" %in% names(dt) || !"basis" %in% names(dt)) stop("Basis response is missing timestamp/basis columns.", call. = FALSE)
    datetime <- .crypto_datetime(dt$timestamp)
    basis <- .crypto_numeric(dt, "basis")
    basis_rate <- .crypto_numeric(dt, "basisRate")
    annualized_basis_rate <- .crypto_numeric(dt, "annualizedBasisRate")
    index_price <- .crypto_numeric(dt, "indexPrice")
    mark_price <- .crypto_numeric(dt, "futuresPrice")
    interval_value <- interval
    value <- basis_rate
  } else if (dataset_type %in% c("global_long_short_ratio", "top_long_short_account_ratio", "top_long_short_position_ratio")) {
    if (!"timestamp" %in% names(dt) || !"longShortRatio" %in% names(dt)) stop("Long-short response is missing timestamp/ratio columns.", call. = FALSE)
    datetime <- .crypto_datetime(dt$timestamp)
    long_short_ratio <- .crypto_numeric(dt, "longShortRatio")
    long_account <- .crypto_numeric(dt, "longAccount")
    short_account <- .crypto_numeric(dt, "shortAccount")
    interval_value <- interval
    value <- long_short_ratio
  } else if (dataset_type == "liquidation") {
    time_cols <- intersect(c("datetime", "time", "timestamp"), names(dt))
    if (length(time_cols) == 0L) stop("Liquidation events require a datetime, time, or timestamp column.", call. = FALSE)
    time_col <- time_cols[[1L]]
    datetime <- .crypto_datetime(dt[[time_col]])
    liquidation_price <- if ("averagePrice" %in% names(dt)) .crypto_numeric(dt, "averagePrice") else .crypto_numeric(dt, "price")
    liquidation_quantity <- if ("executedQty" %in% names(dt)) .crypto_numeric(dt, "executedQty") else .crypto_numeric(dt, "quantity")
    interval_value <- if (is.null(interval)) "event" else interval
    value <- liquidation_quantity
  } else {
    stop("Unsupported crypto derivatives provider/dataset_type: ", provider, "/", dataset_type, call. = FALSE)
  }
  dt[, `:=`(
    source = "crypto_derivatives", provider = provider, dataset_type = dataset_type,
    symbol = as.character(symbol), interval = as.character(interval_value),
    datetime = datetime, date = as.Date(datetime, tz = "UTC"), value = value,
    funding_rate = funding_rate, mark_price = mark_price, index_price = index_price,
    basis = basis, basis_rate = basis_rate, annualized_basis_rate = annualized_basis_rate,
    open_interest = open_interest, open_interest_value = open_interest_value,
    long_short_ratio = long_short_ratio, long_account = long_account,
    short_account = short_account, liquidation_price = liquidation_price,
    liquidation_quantity = liquidation_quantity
  )]
  canonical <- names(.crypto_derivatives_empty())
  data.table::setcolorder(dt, c(canonical, setdiff(names(dt), canonical)))
  data.table::setorderv(dt, "datetime")
  unique(dt, by = c("provider", "dataset_type", "symbol", "interval", "datetime"))
}

.get_binance_funding_history <- function(symbol, from = NULL, to = NULL, limit = 1000L) {
  .require_suggested_package("binxr", "to retrieve Binance derivatives history.")
  limit <- max(1L, min(as.integer(limit), 1000L))
  start_ms <- .crypto_time_ms(from)
  end_ms <- .crypto_time_ms(to)
  pages <- list()
  repeat {
    page <- binxr::futures_get_funding_rate_history(
      symbol = symbol, startTime = start_ms, endTime = end_ms,
      limit = limit, config = binxr::config_futures()
    )
    if (nrow(page) == 0L) break
    pages[[length(pages) + 1L]] <- page
    if (nrow(page) < limit || is.null(start_ms)) break
    next_ms <- .crypto_time_ms(max(page$fundingTime, na.rm = TRUE)) + 1
    if (!is.null(end_ms) && next_ms > end_ms) break
    start_ms <- next_ms
  }
  if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
}

.get_binance_open_interest_history <- function(symbol, interval, from = NULL, to = NULL, limit = 500L) {
  .require_suggested_package("binxr", "to retrieve Binance derivatives history.")
  limit <- max(1L, min(as.integer(limit), 500L))
  start_ms <- .crypto_time_ms(from %||% (Sys.time() - as.difftime(29, units = "days")))
  end_ms <- .crypto_time_ms(to)
  pages <- list()
  repeat {
    page <- binxr::futures_get_open_interest_history(
      symbol, period = interval, startTime = start_ms, endTime = end_ms,
      limit = limit, config = binxr::config_futures()
    )
    if (nrow(page) == 0L) break
    pages[[length(pages) + 1L]] <- page
    if (nrow(page) < limit) break
    next_ms <- .crypto_time_ms(max(page$timestamp, na.rm = TRUE)) + 1
    if (!is.null(end_ms) && next_ms > end_ms) break
    start_ms <- next_ms
  }
  if (length(pages) == 0L) data.table::data.table() else unique(data.table::rbindlist(pages, use.names = TRUE, fill = TRUE), by = "timestamp")
}

.get_binance_periodic_history <- function(dataset_type, symbol, interval,
                                           from = NULL, to = NULL, limit = 500L) {
  .require_suggested_package("binxr", "to retrieve Binance derivatives history.")
  limit <- max(1L, min(as.integer(limit), 500L))
  start_ms <- .crypto_time_ms(from %||% (Sys.time() - as.difftime(29, units = "days")))
  end_ms <- .crypto_time_ms(to)
  fetch <- switch(
    dataset_type,
    basis = function() binxr::futures_get_basis(symbol, "PERPETUAL", interval, start_ms, end_ms, limit, config = binxr::config_futures()),
    global_long_short_ratio = function() binxr::futures_get_global_long_short_ratio(symbol, interval, start_ms, end_ms, limit, config = binxr::config_futures()),
    top_long_short_account_ratio = function() binxr::futures_get_top_long_short_account_ratio(symbol, interval, start_ms, end_ms, limit, config = binxr::config_futures()),
    top_long_short_position_ratio = function() binxr::futures_get_top_long_short_position_ratio(symbol, interval, start_ms, end_ms, limit, config = binxr::config_futures()),
    stop("Unsupported Binance periodic dataset: ", dataset_type, call. = FALSE)
  )
  pages <- list()
  repeat {
    page <- fetch()
    if (nrow(page) == 0L) break
    pages[[length(pages) + 1L]] <- page
    if (nrow(page) < limit) break
    next_ms <- .crypto_time_ms(max(page$timestamp, na.rm = TRUE)) + 1
    if (!is.null(end_ms) && next_ms > end_ms) break
    start_ms <- next_ms
  }
  if (length(pages) == 0L) data.table::data.table() else unique(data.table::rbindlist(pages, use.names = TRUE, fill = TRUE), by = "timestamp")
}

.get_binance_derived_price_history <- function(dataset_type, symbol, interval,
                                                from = NULL, to = NULL, limit = 1500L) {
  .require_suggested_package("binxr", "to retrieve Binance derivatives history.")
  limit <- max(1L, min(as.integer(limit), 1500L))
  start_ms <- .crypto_time_ms(from)
  end_ms <- .crypto_time_ms(to)
  pages <- list()
  repeat {
    page <- if (dataset_type == "mark_price") {
      binxr::futures_get_mark_price_klines(symbol, interval, start_ms, end_ms, limit, config = binxr::config_futures())
    } else {
      binxr::futures_get_index_price_klines(symbol, interval, start_ms, end_ms, limit, config = binxr::config_futures())
    }
    if (nrow(page) == 0L) break
    pages[[length(pages) + 1L]] <- page
    if (nrow(page) < limit || is.null(start_ms)) break
    next_ms <- .crypto_time_ms(max(page$datetime, na.rm = TRUE)) + 1
    if (!is.null(end_ms) && next_ms > end_ms) break
    start_ms <- next_ms
  }
  if (length(pages) == 0L) data.table::data.table() else unique(data.table::rbindlist(pages, use.names = TRUE, fill = TRUE), by = "datetime")
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

.get_okx_derived_price_history <- function(dataset_type, symbol, interval,
                                           from = NULL, to = NULL, limit = 100L,
                                           config = NULL) {
  .require_suggested_package("okxr", "to retrieve OKX derivatives history.")
  limit <- max(1L, min(as.integer(limit), 100L))
  from_ms <- .crypto_time_ms(from)
  to_ms <- .crypto_time_ms(to)
  cursor <- if (is.null(to_ms)) NULL else as.character(to_ms)
  pages <- list()
  repeat {
    page <- if (dataset_type == "mark_price") {
      okxr::get_market_history_mark_price_candles(
        symbol, bar = interval, after = cursor, limit = limit,
        config = config, tz = "UTC"
      )
    } else {
      okxr::get_market_history_index_candles(
        symbol, bar = interval, after = cursor, limit = limit,
        config = config, tz = "UTC"
      )
    }
    page <- data.table::as.data.table(page)
    if (nrow(page) == 0L) break
    if ("timestamp" %in% names(page) && !"datetime" %in% names(page)) data.table::setnames(page, "timestamp", "datetime")
    times_ms <- .crypto_time_ms(page$datetime)
    if (!is.null(to_ms)) page <- page[times_ms <= to_ms]
    if (nrow(page) > 0L) pages[[length(pages) + 1L]] <- page
    oldest <- min(times_ms, na.rm = TRUE)
    if (nrow(page) < limit || (!is.null(from_ms) && oldest <= from_ms)) break
    cursor <- as.character(oldest)
  }
  out <- if (length(pages) == 0L) data.table::data.table() else data.table::rbindlist(pages, use.names = TRUE, fill = TRUE)
  if (!is.null(from_ms) && nrow(out) > 0L) out <- out[.crypto_time_ms(datetime) >= from_ms]
  if (nrow(out) > 0L) out <- unique(out, by = "datetime")
  out
}

#' Retrieve Historical Crypto Derivatives Data
#'
#' @param provider `binance` or `okx`.
#' @param dataset_type Funding, open-interest, mark/index price, basis, or
#'   Binance long-short-ratio dataset identifier.
#' @param symbol Provider instrument identifier.
#' @param interval Required Binance open-interest period; otherwise a local label.
#' @param from,to Optional inclusive UTC time bounds.
#' @param limit Provider page size.
#' @param config Optional exchange configuration.
#'
#' @return A standardized `data.table` retaining provider fields.
#' @export
get_source_data_crypto_derivatives <- function(provider, dataset_type, symbol,
                                               interval = NULL, from = NULL,
                                               to = NULL, limit = NULL,
                                               config = NULL) {
  provider <- tolower(provider)
  dataset_type <- tolower(dataset_type)
  raw <- if (provider == "binance" && dataset_type == "funding_rate") {
    .get_binance_funding_history(symbol, from, to, limit %||% 1000L)
  } else if (provider == "binance" && dataset_type == "open_interest") {
    if (is.null(interval) || !nzchar(interval)) stop("Binance open-interest history requires interval.", call. = FALSE)
    .get_binance_open_interest_history(symbol, interval, from, to, limit %||% 500L)
  } else if (provider == "binance" && dataset_type %in% c("mark_price", "index_price")) {
    if (is.null(interval) || !nzchar(interval)) stop("Binance derived-price history requires interval.", call. = FALSE)
    .get_binance_derived_price_history(dataset_type, symbol, interval, from, to, limit %||% 1500L)
  } else if (provider == "binance" && dataset_type %in% c(
    "basis", "global_long_short_ratio", "top_long_short_account_ratio",
    "top_long_short_position_ratio"
  )) {
    if (is.null(interval) || !nzchar(interval)) stop("Binance periodic derivatives history requires interval.", call. = FALSE)
    .get_binance_periodic_history(dataset_type, symbol, interval, from, to, limit %||% 500L)
  } else if (provider == "okx" && dataset_type == "funding_rate") {
    .get_okx_funding_history(symbol, from, to, limit %||% 400L)
  } else if (provider == "okx" && dataset_type %in% c("mark_price", "index_price")) {
    if (is.null(interval) || !nzchar(interval)) stop("OKX derived-price history requires interval.", call. = FALSE)
    config <- .get_api_config("okx", config = config)
    .get_okx_derived_price_history(dataset_type, symbol, interval, from, to, limit %||% 100L, config)
  } else if (dataset_type == "liquidation") {
    stop(
      "Historical market-wide liquidation events are not available from these REST APIs. Use sync_local_crypto_liquidations() with events captured from an exchange liquidation stream.",
      call. = FALSE
    )
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
get_source_utime_crypto_derivatives <- function(provider, dataset_type, symbol, interval = NULL, config = NULL) {
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
    dt <- get_source_data_crypto_derivatives(provider, dataset_type, symbol, interval, limit = 1L, config = config)
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
#' @param storage Local storage mode: monolithic `"single"` or monthly
#'   partitioned `"monthly"`.
#'
#' @return A `data.table`, or `NULL`.
#' @export
get_local_crypto_derivatives <- function(provider, dataset_type, symbol, interval = NULL, local_path = NULL,
                                         storage = c("single", "monthly"), from = NULL, to = NULL) {
  storage <- match.arg(storage)
  if (is.null(local_path)) local_path <- get_source_data_path("crypto", subdir = "derivatives")
  local_file <- .crypto_derivatives_local_file(provider, dataset_type, symbol, interval, local_path)
  if (storage == "monthly") return(get_local_data_partitioned(local_file, "datetime", from, to, "datetime"))
  .read_local_data_table(local_file, sort_cols = "datetime")
}

#' Synchronize One Crypto Derivatives Dataset
#'
#' @inheritParams get_source_data_crypto_derivatives
#' @param local_path Optional derivatives storage directory.
#' @param overlap_days Days re-fetched before the latest local observation.
#' @param storage Local storage mode: monolithic `"single"` or monthly
#'   partitioned `"monthly"`.
#'
#' @return A local synchronization result list.
#' @export
sync_local_crypto_derivatives <- function(provider, dataset_type, symbol,
                                          interval = NULL, from = NULL, to = NULL,
                                          limit = NULL, local_path = NULL,
                                          overlap_days = 2L, config = NULL,
                                          storage = c("single", "monthly")) {
  storage <- match.arg(storage)
  if (is.null(local_path)) local_path <- get_source_data_path("crypto", subdir = "derivatives", create = TRUE)
  local_file <- .crypto_derivatives_local_file(provider, dataset_type, symbol, interval, local_path)
  local_dt <- if (storage == "monthly") get_local_data_partitioned(local_file, "datetime") else .safe_read_rds(local_file, default = NULL)
  sync_from <- from
  if (!is.null(local_dt) && nrow(local_dt) > 0L) {
    overlap_from <- max(local_dt$datetime, na.rm = TRUE) - as.difftime(overlap_days, units = "days")
    sync_from <- if (is.null(sync_from)) overlap_from else max(as.POSIXct(sync_from, tz = "UTC"), overlap_from)
  }
  fetch_args <- list(provider, dataset_type, symbol, interval, sync_from, to, limit)
  if (!is.null(config)) fetch_args$config <- config
  new_dt <- do.call(get_source_data_crypto_derivatives, fetch_args)
  source_utime <- if (nrow(new_dt) == 0L) NULL else max(new_dt$datetime, na.rm = TRUE)
  sync_fun <- if (storage == "monthly") sync_local_data_partitioned else sync_local_data
  args <- list(new_data = new_dt, local_file_path = local_file,
    key_cols = c("provider", "dataset_type", "symbol", "interval", "datetime"),
    order_cols = "datetime", source_utime = source_utime
  )
  if (storage == "monthly") args$time_col <- "datetime"
  do.call(sync_fun, args)
}

#' Store Captured Crypto Liquidation Events
#'
#' Exchange REST APIs do not provide trustworthy market-wide liquidation
#' history. This function persists events captured from Binance or OKX public
#' liquidation streams without presenting private force-order history as market
#' data.
#'
#' @param events Event rows containing a time column and price/quantity fields.
#' @param provider Exchange identifier.
#' @param symbol Exchange instrument identifier.
#' @param local_path Optional derivatives storage directory.
#' @param storage Local storage mode.
#'
#' @return A local synchronization result list.
#' @export
sync_local_crypto_liquidations <- function(events, provider, symbol,
                                           local_path = NULL,
                                           storage = c("single", "monthly")) {
  storage <- match.arg(storage)
  if (is.null(local_path)) local_path <- get_source_data_path("crypto", subdir = "derivatives", create = TRUE)
  dt <- .standardize_crypto_derivatives(events, provider, "liquidation", symbol, "event")
  local_file <- .crypto_derivatives_local_file(provider, "liquidation", symbol, "event", local_path)
  key_cols <- c("provider", "dataset_type", "symbol", "interval", "datetime")
  if ("orderId" %in% names(dt)) key_cols <- c(key_cols, "orderId")
  if (storage == "monthly") {
    sync_local_data_partitioned(dt, local_file, "datetime", key_cols, "datetime", max(dt$datetime, na.rm = TRUE))
  } else {
    sync_local_data(dt, local_file, key_cols, "datetime", max(dt$datetime, na.rm = TRUE))
  }
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
    "The canonical value is accompanied by dataset-specific funding, open-interest, mark/index price, basis, long-short ratio, or liquidation fields and retained provider columns."
  )
}
