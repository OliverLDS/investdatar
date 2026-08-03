.sec_api_config <- function(config = NULL) {
  config <- .get_api_config("sec", config = config)
  if (is.null(config$user_agent) || !nzchar(trimws(config$user_agent))) {
    stop(
      "SEC user agent is missing. Set SEC_USER_AGENT to an identifiable value such as 'Name email@example.com', or configure SEC.user_agent.",
      call. = FALSE
    )
  }
  config
}

.sec_get_json <- function(url, config = NULL) {
  config <- .sec_api_config(config)
  delay <- suppressWarnings(as.numeric(config$request_delay))
  if (!is.na(delay) && delay > 0) Sys.sleep(delay)
  .http_get_json(
    url,
    headers = c(
      `User-Agent` = config$user_agent,
      Accept = "application/json",
      `Accept-Encoding` = "gzip, deflate"
    )
  )
}

.normalize_sec_cik <- function(cik, padded = FALSE) {
  if (length(cik) != 1L || is.na(cik)) {
    stop("cik must be one non-missing value.", call. = FALSE)
  }
  cik <- gsub("[^0-9]", "", as.character(cik))
  if (!nzchar(cik)) stop("cik must contain at least one digit.", call. = FALSE)
  if (isTRUE(padded)) sprintf("%010d", as.integer(cik)) else as.character(as.integer(cik))
}

.sec_missing_column <- function(template, n) {
  if (inherits(template, "Date")) return(as.Date(rep(NA_real_, n), origin = "1970-01-01"))
  if (inherits(template, "POSIXct")) return(as.POSIXct(rep(NA_real_, n), origin = "1970-01-01", tz = attr(template, "tzone") %||% "UTC"))
  if (is.logical(template)) return(rep(NA, n))
  if (is.integer(template)) return(rep(NA_integer_, n))
  if (is.numeric(template)) return(rep(NA_real_, n))
  rep(NA_character_, n)
}

#' Retrieve SEC Ticker-CIK Mappings
#'
#' @param config Optional SEC configuration containing an identifiable user agent.
#'
#' @return A `data.table` with `cik`, `company_name`, `ticker`, and `exchange`.
#' @export
get_sec_company_tickers <- function(config = NULL) {
  config <- .sec_api_config(config)
  response <- .sec_get_json(
    paste0(sub("/+$", "", config$website_url), "/files/company_tickers_exchange.json"),
    config = config
  )
  if (is.null(response$fields) || is.null(response$data)) {
    stop("SEC ticker mapping response is missing fields or data.", call. = FALSE)
  }
  rows <- data.table::as.data.table(do.call(rbind, response$data))
  data.table::setnames(rows, as.character(response$fields))
  data.table::setnames(rows, old = intersect(c("name"), names(rows)), new = "company_name")
  rows[, cik := as.character(as.integer(cik))]
  rows[, ticker := toupper(as.character(ticker))]
  data.table::setorderv(rows, c("ticker", "cik"))
  rows[]
}

#' Resolve An SEC CIK From A Ticker
#'
#' @param ticker Security ticker.
#' @param mappings Optional table returned by `get_sec_company_tickers()`.
#' @param config Optional SEC configuration used when mappings are not supplied.
#'
#' @return Unpadded CIK character scalar.
#' @export
resolve_sec_cik <- function(ticker, mappings = NULL, config = NULL) {
  if (is.null(mappings)) mappings <- get_sec_company_tickers(config = config)
  ticker_value <- toupper(trimws(ticker))
  matches <- data.table::as.data.table(mappings)[toupper(ticker) == ticker_value]
  if (nrow(matches) == 0L) stop("SEC CIK mapping not found for ticker: ", ticker, call. = FALSE)
  if (data.table::uniqueN(matches$cik) > 1L) stop("Ticker maps to multiple SEC CIK values: ", ticker, call. = FALSE)
  as.character(matches$cik[[1L]])
}

#' Get SEC Company Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback.
#'
#' @return Character scalar path.
#' @export
get_sec_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("sec"), error = function(e) list())
  if (!is.null(cfg$registry_file) && nzchar(cfg$registry_file)) return(.normalize_scalar_path(cfg$registry_file, config_dir = getOption("investdatar.config_dir")))
  if (is.null(config_dir)) config_dir <- getOption("investdatar.config_dir")
  if (is.null(config_dir) || !nzchar(config_dir)) stop("No SEC registry path is configured. Set SEC.registry_file in your config.", call. = FALSE)
  file.path(config_dir, "sec_company_registry.json")
}

#' Get SEC Company Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return A registry `data.table`.
#' @export
get_sec_registry <- function(registry_path = get_sec_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("ticker", "cik", "company_name", "forms", "concepts", "active")
  )
}

#' Add Or Update An SEC Registry Company
#'
#' @param ticker Security ticker.
#' @param cik Optional CIK; resolved from the SEC ticker mapping when omitted.
#' @param company_name Optional company name.
#' @param forms Optional filing forms retained by submissions sync.
#' @param concepts Optional XBRL concepts retained by Company Facts sync.
#' @param active Logical registry flag.
#' @param registry_path Optional registry path.
#' @param config Optional SEC configuration.
#'
#' @return The stored registry row.
#' @export
add_sec_registry_company <- function(ticker, cik = NULL, company_name = NULL,
                                     forms = c("10-K", "10-Q", "8-K"), concepts = NULL,
                                     active = TRUE,
                                     registry_path = get_sec_registry_file_path(),
                                     config = NULL) {
  mappings <- NULL
  if (is.null(cik) || is.null(company_name)) mappings <- get_sec_company_tickers(config = config)
  if (is.null(cik)) cik <- resolve_sec_cik(ticker, mappings = mappings)
  cik <- .normalize_sec_cik(cik)
  ticker <- toupper(trimws(ticker))
  if (is.null(company_name)) {
    row <- mappings[toupper(mappings$ticker) == ticker & mappings$cik == cik]
    company_name <- if (nrow(row) > 0L) row$company_name[[1L]] else NA_character_
  }
  registry <- get_sec_registry(registry_path)
  new_row <- data.table::data.table(
    ticker = ticker, cik = cik, company_name = as.character(company_name),
    forms = list(as.character(forms)), concepts = list(as.character(concepts)), active = isTRUE(active)
  )
  ticker_value <- ticker
  cik_value <- cik
  registry <- registry[!(toupper(ticker) == ticker_value | cik == cik_value)]
  registry <- data.table::rbindlist(list(registry, new_row), use.names = TRUE, fill = TRUE)
  data.table::setorderv(registry, "ticker")
  .write_json_registry(registry, registry_path)
  registry[ticker == ticker_value]
}

.sec_registry_values <- function(x) {
  if (is.null(x) || length(x) == 0L || all(is.na(x))) return(NULL)
  if (is.list(x)) x <- unlist(x, use.names = FALSE)
  x <- trimws(unlist(strsplit(as.character(x), ",", fixed = TRUE), use.names = FALSE))
  x[nzchar(x)]
}
