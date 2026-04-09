.to_num <- function(x, NA_to_zero = FALSE) {
  x <- trimws(as.character(x))
  x[x %in% c("-", "--", "")] <- NA_character_
  if (NA_to_zero) x[is.na(x)] <- '0'
  as.numeric(gsub(",", "", x, fixed = TRUE))
}
  
.to_date <- function(x, fmt = "%Y-%m-%d") {
  x <- trimws(as.character(x))
  x[x %in% c("-", "--", "")] <- NA_character_
  data.table::as.IDate(x, format = fmt) # It is said data.table::as.IDate is much faster than as.Date
}

.sanitize_xml_text <- function(txt) {
  txt <- gsub("&(?!#\\d+;|#x[0-9A-Fa-f]+;|[A-Za-z][A-Za-z0-9]+;)", "&amp;", txt, perl=TRUE)
  txt <- gsub("[\\x00-\\x08\\x0B\\x0C\\x0E-\\x1F]", "", txt, perl=TRUE)
  txt
}

.get_worksheets_from_xml <- function(xml_path, sheet_name) {
  raw <- readBin(xml_path, "raw", n = file.info(xml_path)$size)
  i <- which(raw == as.raw(0x3C))[1]; if (!is.na(i) && i > 1L) raw <- raw[i:length(raw)] # remove UTF-8 tags
  xml_text_raw <- try(rawToChar(raw, multiple = FALSE), silent = TRUE)
  if (inherits(xml_text_raw, "try-error")) { stop("error in downloading") }
  txt <- .sanitize_xml_text(xml_text_raw)
  doc <- xml2::read_xml(txt, options=c("RECOVER","NOERROR","NOWARNING","HUGE"))
  ns  <- xml2::xml_ns(doc); if (is.null(ns[["ss"]])) ns <- c(ns, ss="urn:schemas-microsoft-com:office:spreadsheet")
  
  wss   <- xml2::xml_find_all(doc, ".//ss:Worksheet", ns=ns)
  names <- xml2::xml_attr(wss, "Name"); names[is.na(names)] <- paste0("Sheet", seq_along(wss))
  
  idx   <- which(grepl(sheet_name, names, ignore.case=TRUE))[1]
  if (is.na(idx)) stop("No sheet matches: ", sheet_name)
  list(ws = wss[[idx]], ns = ns)
}

.get_xml_xls <- function(path, sheet_name, n_headrows, n_bottoms, col_labels, herf_dict_dt = NULL, use_head_match = FALSE) {
  res <- .get_worksheets_from_xml(path, sheet_name)
  ws <- res$ws
  ns <- res$ns
  rows <- xml2::xml_find_all(ws, ".//ss:Table/ss:Row", ns=ns)
  
  if (use_head_match) {
    row_heads <- sapply(xml2::xml_find_all(rows[n_headrows], "./ss:Cell", ns=ns), function(x) {
      dat <- xml2::xml_find_first(x, "./ss:Data", ns=ns)
      xml2::xml_text(dat)
    })
    selected_Js <- (1:length(row_heads))[row_heads %in% col_labels]
  } else {
    cells <- xml2::xml_find_all(rows[n_headrows+1], "./ss:Cell", ns=ns) # because metadata's head is merged; we couldn't get correct column namber from head rows
    selected_Js <- 1:length(cells)
  }
  stopifnot(length(selected_Js) == length(col_labels))

  out <- lapply(
    rows[(n_headrows+1):(length(rows)-n_bottoms)], 
    function (row) {
      cells <- xml2::xml_find_all(row, "./ss:Cell", ns=ns)
      vals <- list()
      k <- 1
      for (j in selected_Js){
        dat <- xml2::xml_find_first(cells[j], "./ss:Data", ns=ns)
        v <- xml2::xml_text(dat)
        col_name <- col_labels[k]
        vals[[col_name]] <- v
        k <- k + 1
      }
      if (!is.null(herf_dict_dt)) {
        for (h in 1:nrow(herf_dict_dt)) {
          herf <- xml2::xml_attr(cells[herf_dict_dt$col_idx[h]], "HRef")
          vals[[herf_dict_dt$col_label[h]]] <- herf
        }
      }
      vals
    }
  )

  data.table::rbindlist(out)
}

.get_ishare_xls_file_name <- function(prod_url, user_agent = FALSE) {
  path <- sprintf("%s/1521942788811.ajax?fileType=xls", prod_url)

  if (user_agent) {
    ua <- httr::user_agent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0"
    )
    head_res <- httr::HEAD(path, ua, httr::timeout(20))  
  } else {
    head_res <- httr::HEAD(path, httr::timeout(20))
  }
  httr::stop_for_status(head_res)
  cd <- head_res$headers[["content-disposition"]]
  sub('.*filename="?([^";]+).*', "\\1", cd)
}

.download_ishare_xls_file <- function(prod_url, cache_dir, file_name = NULL, user_agent = FALSE) {
  path <- sprintf("%s/1521942788811.ajax?fileType=xls", prod_url)

  if (user_agent) {
    ua <- httr::user_agent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0"
    )
    head_res <- httr::HEAD(path, ua, httr::timeout(20))  
  } else {
    head_res <- httr::HEAD(path, httr::timeout(20))
  }
  httr::stop_for_status(head_res)

  if (is.null(file_name)) {
    cd <- head_res$headers[["content-disposition"]]
    file_name <- sub('.*filename="?([^";]+).*', "\\1", cd)
  }

  file_path <- file.path(cache_dir, file_name)

  get_res <- httr::RETRY(
    "GET",
    path,
    httr::write_disk(file_path, overwrite = TRUE),
    httr::timeout(300), # 300 seconds is really long enough
    times = 3,
    pause_min = 1,
    pause_cap = 10
  )
  httr::stop_for_status(get_res)

  return(file_path)
}
.wraggle_mega_data <- function(file_path) {
  sheet_name <- 'etf'
  n_headrows <- 2
  n_bottoms <- 3
  col_labels <- c('Ticker','Name','SEDOL','ISIN','CUSIP','Incept. Date','Gross Expense Ratio (%)','Net Expense Ratio (%)','Net Assets (USD)','Net Assets as of','Asset Class','Sub Asset Class','Region','Market','Location','Investment Style', 'Key 12m Trailing Yield (%)','Key As of','Key YTD Return (%)','Key Per. As of','NAV Q YTD (%)','NAV Q 1Y (%)','NAV Q 3Y (%)','NAV Q 5Y (%)','NAV Q 10Y (%)','NAV Q Incept. (%)','NAV Q Per. As of','PRICE Q YTD (%)','PRICE Q 1Y (%)','PRICE Q 3Y (%)','PRICE Q 5Y (%)','PRICE Q 10Y (%)','PRICE Q Incept. (%)','PRICE Q Per. As of','NAV M YTD (%)','NAV M 1Y (%)','NAV M 3Y (%)','NAV M 5Y (%)','NAV M 10Y (%)','NAV M Incept. (%)','NAV M Per. As of','Price M YTD (%)','Price M 1Y (%)','Price M 3Y (%)','Price M 5Y (%)','Price M 10Y (%)','Price M Incept. (%)','Price M Per. As of','Yield 12m Trailing Yield (%)','Yield As of','Yield 30-Day SEC Yield (%)','Yield Unsubsidized 30-day SEC Yield (%)','Yield As of','FI Duration (yrs)','FI Option Adjusted Spread','FI Avg. Yield (%)','FI Avg. Yield as of Date')
  herf_dict_dt <- data.table::data.table(col_idx = 1L, col_label = 'etf_href')
  
  out <- .get_xml_xls(file_path, sheet_name, n_headrows, n_bottoms, col_labels = col_labels, herf_dict_dt = herf_dict_dt)
  
  num_cols <- c(
    "Net Assets (USD)",
    "Gross Expense Ratio (%)","Net Expense Ratio (%)",
    "Key 12m Trailing Yield (%)","Key YTD Return (%)",
    "NAV Q YTD (%)","NAV Q 1Y (%)","NAV Q 3Y (%)","NAV Q 5Y (%)",
    "NAV Q 10Y (%)","NAV Q Incept. (%)",
    "PRICE Q YTD (%)","PRICE Q 1Y (%)","PRICE Q 3Y (%)",
    "PRICE Q 5Y (%)","PRICE Q 10Y (%)","PRICE Q Incept. (%)",
    "NAV M YTD (%)","NAV M 1Y (%)","NAV M 3Y (%)","NAV M 5Y (%)",
    "NAV M 10Y (%)","NAV M Incept. (%)",
    "Price M YTD (%)","Price M 1Y (%)","Price M 3Y (%)",
    "Price M 5Y (%)","Price M 10Y (%)","Price M Incept. (%)",
    "Yield 12m Trailing Yield (%)",
    "Yield 30-Day SEC Yield (%)",
    "Yield Unsubsidized 30-day SEC Yield (%)",
    "FI Duration (yrs)","FI Option Adjusted Spread",
    "FI Avg. Yield (%)"
  )
  
  date_cols <- c(
    "Incept. Date",
    "Net Assets as of",
    "Key As of",
    "Key Per. As of",
    "NAV Q Per. As of",
    "PRICE Q Per. As of",
    "NAV M Per. As of",
    "Price M Per. As of",
    "Yield As of",
    "FI Avg. Yield as of Date"
  )
  
  out[, (num_cols) := lapply(.SD, .to_num), .SDcols = num_cols]
  out[, (date_cols) := lapply(.SD, .to_date), .SDcols = date_cols]
  
  out[]
}

.wraggle_historical_data <- function(file_path) {
  sheet_name <- 'Historical'
  n_headrows <- 1
  n_bottoms <- 0
  col_labels <- c('As Of', 'NAV per Share', 'Ex-Dividends', 'Shares Outstanding')
  
  out <- .get_xml_xls(file_path, sheet_name, n_headrows, n_bottoms, col_labels = col_labels, use_head_match = TRUE)
  
  out <- out[, list(
    date  = .to_date(`As Of`, fmt = "%b %d, %Y"),
    nav   = .to_num(`NAV per Share`),
    ex_div = .to_num(`Ex-Dividends`, NA_to_zero = TRUE),
    N_shares = .to_num(`Shares Outstanding`)
  )]
  
  # sorting by dates
  out <- out[order(date)]
  
  # handle NAs
  stopifnot(all(!is.na(out$date)))
  if (any(is.na(out$nav))) message(sprintf('missing values of NAV: %s', file_path))
  # stopifnot(all(!is.na(out$nav))) # BINC
  out[, ex_div := data.table::nafill(ex_div, fill = 0)]
  out[, N_shares := data.table::nafill(N_shares, type = 'locf')]
  
  out[, log_ret := log(nav + ex_div) - log(data.table::shift(nav))]
  out[, log_N_shares := log(N_shares) - log(data.table::shift(N_shares))]
  out[]
}

.wraggle_holding_data <- function(file_path) {
  sheet_name <- 'Holdings'
  n_headrows <- 8
  n_bottoms <- 0
  col_labels <- c('Ticker', 'Name', 'Sector', 'Asset Class', 'Weight (%)', 'Location', 'Exchange')
  
  res <- .get_worksheets_from_xml(file_path, sheet_name)
  ws <- res$ws
  ns <- res$ns
  rows <- xml2::xml_find_all(ws, ".//ss:Table/ss:Row", ns=ns)
  rows[4]
  
  updated_date_cell <- sapply(xml2::xml_find_all(rows[4], "./ss:Cell", ns=ns), function(x) {
    dat <- xml2::xml_find_first(x, "./ss:Data", ns=ns)
    xml2::xml_text(dat)
  })
  stopifnot(identical(updated_date_cell[1], 'Fund Holdings as of'))
  
  updated_date <- as.Date(updated_date_cell[2], format = "%b %d, %Y")
  
  out <- .get_xml_xls(file_path, sheet_name, n_headrows, n_bottoms, col_labels = col_labels, use_head_match = TRUE)
  
  out <- out[, .(
    holding_ticker = Ticker,
    holding_name = Name,
    sector = Sector,
    asset_class = `Asset Class`,
    weight_pct = .to_num(`Weight (%)`),
    location = Location,
    exchange = Exchange
  )]

  out[, updated_date := updated_date]
  data.table::setcolorder(
    out,
    c("updated_date", "holding_ticker", "holding_name", "sector", "asset_class", "weight_pct", "location", "exchange")
  )

  out[]
 
}

.default_ishare_holdings_tickers <- function() {
  c("DYNF", "THRO", "BAI", "BDYN", "BDVL")
}

.looks_numericish <- function(x) {
  if (is.null(x)) {
    return(FALSE)
  }
  x <- trimws(as.character(x))
  x <- x[nzchar(x) & !is.na(x)]
  if (length(x) == 0L) {
    return(FALSE)
  }
  mean(grepl("^-?[0-9.,]+$", x)) > 0.8
}

.normalize_ishare_holdings_snapshot <- function(dt, ticker = NULL, updated_date = NULL) {
  dt <- data.table::as.data.table(dt)
  nms <- names(dt)

  get_col <- function(candidates) {
    idx <- match(candidates, nms)
    idx <- idx[!is.na(idx)]
    if (length(idx) == 0L) {
      return(rep(NA_character_, nrow(dt)))
    }
    dt[[idx[[1]]]]
  }

  weight_raw <- NULL
  if ("Weight (%)" %in% nms) {
    weight_raw <- dt[["Weight (%)"]]
  } else if ("weight_pct" %in% nms) {
    weight_raw <- dt[["weight_pct"]]
  } else if ("Location" %in% nms && .looks_numericish(dt[["Location"]])) {
    # Early legacy snapshots wrote weights into Location and exchange into an NA column.
    weight_raw <- dt[["Location"]]
  } else {
    weight_raw <- rep(NA_character_, nrow(dt))
  }

  location_raw <- NULL
  if ("location" %in% nms) {
    location_raw <- dt[["location"]]
  } else if ("Location" %in% nms && !.looks_numericish(dt[["Location"]])) {
    location_raw <- dt[["Location"]]
  } else {
    location_raw <- rep(NA_character_, nrow(dt))
  }

  exchange_raw <- NULL
  if ("exchange" %in% nms) {
    exchange_raw <- dt[["exchange"]]
  } else if ("Exchange" %in% nms) {
    exchange_raw <- dt[["Exchange"]]
  } else if ("NA" %in% nms) {
    exchange_raw <- dt[["NA"]]
  } else {
    exchange_raw <- rep(NA_character_, nrow(dt))
  }

  out <- data.table::data.table(
    ticker = if (!is.null(ticker)) rep(ticker, nrow(dt)) else get_col("ticker"),
    updated_date = if (!is.null(updated_date)) rep(as.Date(updated_date), nrow(dt)) else .to_date(get_col("updated_date")),
    holding_ticker = get_col(c("holding_ticker", "Ticker")),
    holding_name = get_col(c("holding_name", "Name")),
    sector = get_col(c("sector", "Sector")),
    asset_class = get_col(c("asset_class", "Asset Class")),
    weight_pct = .to_num(weight_raw),
    location = trimws(as.character(location_raw)),
    exchange = trimws(as.character(exchange_raw))
  )

  out[]
}

.coerce_legacy_ishare_holdings <- function(x, ticker = NULL) {
  if (is.null(x)) {
    return(NULL)
  }

  if (is.data.frame(x)) {
    out <- .normalize_ishare_holdings_snapshot(x, ticker = ticker)
    data.table::setorderv(out, c("updated_date", "holding_ticker"))
    return(out[])
  }

  if (is.list(x) && !is.null(names(x)) && all(nzchar(names(x)))) {
    snapshot_names <- names(x)
    if (all(grepl("^\\d{4}-\\d{2}-\\d{2}$", snapshot_names))) {
      out <- data.table::rbindlist(
        lapply(snapshot_names, function(d) {
          .normalize_ishare_holdings_snapshot(x[[d]], ticker = ticker, updated_date = d)
        }),
        use.names = TRUE,
        fill = TRUE
      )
      data.table::setorderv(out, c("updated_date", "holding_ticker"))
      return(out[])
    }
  }

  stop("Unsupported local iShares holdings file structure.")
}

.migrate_local_ishare_holdings_file <- function(local_file_path, ticker) {
  old_obj <- .safe_read_rds(local_file_path, default = NULL)
  if (is.null(old_obj)) {
    return(invisible(NULL))
  }

  if (is.data.frame(old_obj) && all(c("updated_date", "holding_ticker") %in% names(old_obj))) {
    return(invisible(NULL))
  }

  migrated <- .coerce_legacy_ishare_holdings(old_obj, ticker = ticker)
  .safe_save_rds(migrated, local_file_path)
  invisible(migrated)
}

.get_ishare_holdings_sync_tickers <- function(config = NULL) {
  cfg <- tryCatch(get_source_config("ishare", config = config), error = function(e) list())
  tickers <- cfg$holdings_tickers

  if (is.null(tickers)) {
    return(.default_ishare_holdings_tickers())
  }

  tickers <- unique(trimws(as.character(tickers)))
  tickers[nzchar(tickers)]
}

#' Get iShares Registry File Path
#'
#' @param config_dir Optional configuration directory used for fallback lookup.
#'
#' @return Character scalar path.
#' @export
get_ishare_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("ishare"), error = function(e) list())
  registry_file <- cfg$registry_file

  if (is.null(registry_file) || !nzchar(registry_file)) {
    if (is.null(config_dir)) {
      config_dir <- getOption("investdatar.config_dir")
    }
    if (is.null(config_dir) || !nzchar(config_dir)) {
      stop(
        "No iShares registry path is configured. Set iShare.registry_file in ",
        "your config or load a config file rooted at the desired directory."
      )
    }
    return(file.path(config_dir, "ishare_ticker_registry.json"))
  }

  .normalize_scalar_path(registry_file, config_dir = getOption("investdatar.config_dir"))
}

#' Get iShares Registry
#'
#' @param registry_path Optional registry JSON path.
#'
#' @return `data.table`.
#' @export
get_ishare_registry <- function(registry_path = get_ishare_registry_file_path()) {
  .read_json_registry(registry_path, empty_cols = c("ticker", "type"))
}

#' Add Or Update One iShares Registry Entry
#'
#' @param ticker ETF ticker.
#' @param type Optional ticker type. If `NULL`, read from stdin.
#' @param registry_path Optional registry JSON path.
#'
#' @return The added or updated row as a `data.table`.
#' @export
add_ishare_registry_ticker <- function(ticker, type = NULL,
                                       registry_path = get_ishare_registry_file_path()) {
  registry <- get_ishare_registry(registry_path = registry_path)
  template_names <- names(registry)

  if (is.null(type) || !nzchar(type)) {
    type <- .prompt_stdin_value(sprintf("Enter type for iShare ticker '%s': ", ticker))
  }
  if (!nzchar(type)) {
    stop("type must be a non-empty string.")
  }

  new_row <- data.table::data.table(ticker = ticker, type = type)
  new_row <- .align_registry_schema(new_row, template_names)

  if (nrow(registry) > 0L && any(registry$ticker == ticker)) {
    ticker_value <- ticker
    registry <- registry[ticker != ticker_value]
  }
  registry <- data.table::rbindlist(list(registry, new_row), use.names = TRUE, fill = TRUE)
  data.table::setorderv(registry, "ticker")
  .write_json_registry(registry, registry_path)

  ticker_value <- ticker
  registry[ticker == ticker_value]
}

#' Get iShares Historical Data
#'
#' Downloads and parses the historical sheet for a single iShares fund using
#' the local iShares registry metadata.
#'
#' @param ticker ETF ticker.
#' @param ishare_mega_data Optional iShares registry table.
#' @param cache_dir Optional download cache directory.
#' @param local_path Optional local storage path used to resolve defaults.
#'
#' @return `data.table`.
#' @export
get_source_data_ishare <- function(ticker, ishare_mega_data = NULL, cache_dir = NULL, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- tryCatch(get_source_data_path("ishare", create = TRUE), error = function(e) NULL)
  }
  if (is.null(cache_dir) && !is.null(local_path)) {
    cache_dir <- file.path(local_path, "_cache")
  }
  if (!is.null(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  }
  if (is.null(ishare_mega_data)) {
    ishare_mega_data <- get_local_ishare_mega_data(local_path = local_path)
  }
  prod_url <- ishare_mega_data[Ticker == ticker, etf_href]
  file_path <- .download_ishare_xls_file(prod_url, cache_dir = cache_dir)
  .wraggle_historical_data(file_path)
}

#' Get iShares Holdings Data
#'
#' Downloads and parses the holdings sheet for a single iShares fund using the
#' local iShares registry metadata.
#'
#' @param ticker ETF ticker.
#' @param ishare_mega_data Optional iShares registry table.
#' @param cache_dir Optional download cache directory.
#' @param local_path Optional local storage path used to resolve defaults.
#'
#' @return `data.table`.
#' @export
get_source_data_ishare_holdings <- function(ticker, ishare_mega_data = NULL, cache_dir = NULL, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- tryCatch(get_source_data_path("ishare", create = TRUE), error = function(e) NULL)
  }
  if (is.null(cache_dir) && !is.null(local_path)) {
    cache_dir <- file.path(local_path, "_cache")
  }
  if (!is.null(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  }
  if (is.null(ishare_mega_data)) {
    ishare_mega_data <- get_local_ishare_mega_data(local_path = local_path)
  }
  prod_url <- ishare_mega_data[Ticker == ticker, etf_href]
  file_path <- .download_ishare_xls_file(prod_url, cache_dir = cache_dir)
  out <- .wraggle_holding_data(file_path)
  out[, ticker := ticker]
  data.table::setcolorder(out, c("ticker", setdiff(names(out), "ticker")))
  out[]
}

#' Get iShares Source Update Time
#'
#' Returns a best-effort upstream update time for iShares data. When
#' `check_online = TRUE`, the function scrapes the product page; otherwise it
#' falls back to the latest midnight in `tz`.
#'
#' @param tz Time zone used for the returned timestamp.
#' @param check_online Logical. Scrape iShares online when `TRUE`.
#'
#' @return POSIXct.
#' @export
get_source_utime_ishare <- function(tz = 'America/New_York', check_online = TRUE) {
  if (check_online) {
    url <- "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf"
    html  <- readLines(url, warn = FALSE) 
    idx <- grep("NAV as of", html, ignore.case = TRUE)
    line <- html[idx]
    raw_date <- sub(".*NAV as of ([A-Za-z]+ [0-9]{1,2}, [0-9]{4}).*", "\\1", line)
    utime <- as.POSIXct(raw_date, format = "%b %d, %Y", tz = "America/New_York") + 86399 # 86399 seconds = 23:59:59
    return(utime)
  } else {
    now <- Sys.time()
    latest_midnight_in_tz_string <- format(now, "%Y-%m-%d 00:00:00", tz = tz)
    latest_midnight_in_tz <- as.POSIXct(latest_midnight_in_tz_string, tz = tz) # tz parameter is only used when we switch between POSIXct and string
    return(latest_midnight_in_tz)
  }
}

#' Get Local iShares Historical Data
#'
#' @param ticker ETF ticker.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_ishare_data <- function(ticker, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare")
  }
  dt <- .read_local_data_table(file.path(local_path, paste0(ticker, "_historical.rds")), sort_cols = "date")
  if (!is.null(dt) && !"ticker" %in% names(dt)) {
    dt[, ticker := ticker]
    data.table::setcolorder(dt, c("ticker", setdiff(names(dt), "ticker")))
  }
  dt
}

#' Get Local iShares Holdings Data
#'
#' @param ticker ETF ticker.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_ishare_holdings <- function(ticker, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare")
  }
  local_file_path <- file.path(local_path, paste0(ticker, "_holdings.rds"))
  .migrate_local_ishare_holdings_file(local_file_path, ticker = ticker)
  dt <- .read_local_data_table(local_file_path, sort_cols = c("updated_date", "holding_ticker"))
  if (!is.null(dt) && !"ticker" %in% names(dt)) {
    dt[, ticker := ticker]
    data.table::setcolorder(dt, c("ticker", setdiff(names(dt), "ticker")))
  }
  dt
}

#' Get Local iShares Metadata Snapshot
#'
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_ishare_mega_data <- function(local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare")
  }
  .read_local_data_table(file.path(local_path, "mega_data.rds"), sort_cols = "Ticker")
}

#' Synchronize Local iShares Historical Data
#'
#' @param ticker ETF ticker.
#' @param ishare_mega_data Optional iShares registry table.
#' @param local_path Optional local storage path.
#' @param cache_dir Optional XLS cache directory.
#' @param source_utime Optional upstream update time.
#'
#' @return A sync result list.
#' @export
sync_local_ishare_data <- function(ticker, ishare_mega_data = NULL, local_path = NULL,
                                   cache_dir = NULL, source_utime = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare", create = TRUE)
  }
  if (is.null(cache_dir)) {
    cache_dir <- file.path(local_path, "_cache")
  }
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

  if (is.null(source_utime)) {
    source_utime <- get_source_utime_ishare(check_online = FALSE)
  }

  new_dt <- get_source_data_ishare(
    ticker = ticker,
    ishare_mega_data = ishare_mega_data,
    cache_dir = cache_dir,
    local_path = local_path
  )
  if (!"ticker" %in% names(new_dt)) {
    new_dt[, ticker := ticker]
  }

  sync_local_data(
    new_data = new_dt,
    local_file_path = file.path(local_path, paste0(ticker, "_historical.rds")),
    key_cols = "date",
    order_cols = "date",
    source_utime = source_utime
  )
}

#' Synchronize Local iShares Holdings Data
#'
#' @param ticker ETF ticker.
#' @param ishare_mega_data Optional iShares registry table.
#' @param local_path Optional local storage path.
#' @param cache_dir Optional XLS cache directory.
#' @param source_utime Optional upstream update time.
#'
#' @return A sync result list.
#' @export
sync_local_ishare_holdings <- function(ticker, ishare_mega_data = NULL, local_path = NULL,
                                       cache_dir = NULL, source_utime = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare", create = TRUE)
  }
  if (is.null(cache_dir)) {
    cache_dir <- file.path(local_path, "_cache")
  }
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)

  if (is.null(source_utime)) {
    source_utime <- get_source_utime_ishare(check_online = FALSE)
  }

  local_file_path <- file.path(local_path, paste0(ticker, "_holdings.rds"))
  .migrate_local_ishare_holdings_file(local_file_path, ticker = ticker)

  new_dt <- get_source_data_ishare_holdings(
    ticker = ticker,
    ishare_mega_data = ishare_mega_data,
    cache_dir = cache_dir,
    local_path = local_path
  )
  if (!"ticker" %in% names(new_dt)) {
    new_dt[, ticker := ticker]
  }

  sync_local_data(
    new_data = new_dt,
    local_file_path = local_file_path,
    key_cols = c("updated_date", "holding_ticker"),
    order_cols = c("updated_date", "holding_ticker"),
    source_utime = source_utime
  )
}

#' Synchronize All iShares Tickers In The Registry
#'
#' @param registry Optional iShares registry table.
#' @param local_path Optional local storage path.
#' @param cache_dir Optional cache directory.
#' @param source_utime Optional upstream update time shared across the batch.
#' @param ishare_mega_data Optional metadata table used to resolve fund URLs.
#'
#' @return Summary `data.table`.
#' @export
sync_all_ishare_registry_data <- function(registry = get_ishare_registry(),
                                          local_path = NULL,
                                          cache_dir = NULL,
                                          source_utime = NULL,
                                          ishare_mega_data = NULL) {
  stopifnot("ticker" %in% names(registry))

  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare", create = TRUE)
  }
  if (is.null(cache_dir)) {
    cache_dir <- file.path(local_path, "_cache")
  }
  if (is.null(ishare_mega_data)) {
    ishare_mega_data <- get_local_ishare_mega_data(local_path = local_path)
  }
  if (is.null(source_utime)) {
    source_utime <- get_source_utime_ishare(check_online = FALSE)
  }
  run_started_at <- Sys.time()

  summary_list <- lapply(seq_len(nrow(registry)), function(i) {
    ticker <- registry$ticker[[i]]
    type <- if ("type" %in% names(registry)) registry$type[[i]] else NA_character_

    tryCatch(
      {
        res <- sync_local_ishare_data(
          ticker = ticker,
          ishare_mega_data = ishare_mega_data,
          local_path = local_path,
          cache_dir = cache_dir,
          source_utime = source_utime
        )
        data.table::data.table(
          ticker = ticker,
          type = type,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          ticker = ticker,
          type = type,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          error = conditionMessage(e)
        )
      }
    )
  })

  summary_dt <- data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE)
  .write_sync_run_log(
    source_id = "ishare",
    summary = summary_dt,
    local_path = local_path,
    params = list(),
    run_started_at = run_started_at,
    run_finished_at = Sys.time()
  )
  summary_dt
}

#' Synchronize All iShares Holdings In The Registry
#'
#' @param registry Optional iShares registry table. When `NULL`, the function
#'   reads the full iShares registry and filters it to the configured holdings
#'   ticker list.
#' @param tickers Optional character vector of holdings tickers to sync. When
#'   `NULL`, the function uses `iShare.holdings_tickers` from package config and
#'   falls back to `c("DYNF", "THRO", "BAI", "BDYN", "BDVL")`.
#' @param local_path Optional local storage path.
#' @param cache_dir Optional cache directory.
#' @param source_utime Optional upstream update time shared across the batch.
#' @param ishare_mega_data Optional metadata table used to resolve fund URLs.
#'
#' @return Summary `data.table`.
#' @export
sync_all_ishare_registry_holdings <- function(registry = NULL,
                                              tickers = NULL,
                                              local_path = NULL,
                                              cache_dir = NULL,
                                              source_utime = NULL,
                                              ishare_mega_data = NULL) {
  if (is.null(registry)) {
    registry <- get_ishare_registry()
  }
  stopifnot("ticker" %in% names(registry))

  if (is.null(tickers)) {
    tickers <- .get_ishare_holdings_sync_tickers()
  }
  tickers <- unique(trimws(as.character(tickers)))
  tickers <- tickers[nzchar(tickers)]

  registry <- registry[ticker %in% tickers]

  if (nrow(registry) == 0L) {
    return(
      data.table::data.table(
        ticker = character(),
        type = character(),
        status = character(),
        updated = logical(),
        n_rows = integer(),
        n_new_rows = integer(),
        error = character()
      )
    )
  }

  if (is.null(local_path)) {
    local_path <- get_source_data_path("ishare", create = TRUE)
  }
  if (is.null(cache_dir)) {
    cache_dir <- file.path(local_path, "_cache")
  }
  if (is.null(ishare_mega_data)) {
    ishare_mega_data <- get_local_ishare_mega_data(local_path = local_path)
  }
  if (is.null(source_utime)) {
    source_utime <- get_source_utime_ishare(check_online = FALSE)
  }
  run_started_at <- Sys.time()

  summary_list <- lapply(seq_len(nrow(registry)), function(i) {
    ticker <- registry$ticker[[i]]
    type <- if ("type" %in% names(registry)) registry$type[[i]] else NA_character_

    tryCatch(
      {
        res <- sync_local_ishare_holdings(
          ticker = ticker,
          ishare_mega_data = ishare_mega_data,
          local_path = local_path,
          cache_dir = cache_dir,
          source_utime = source_utime
        )
        data.table::data.table(
          ticker = ticker,
          type = type,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          ticker = ticker,
          type = type,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          error = conditionMessage(e)
        )
      }
    )
  })

  summary_dt <- data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE)
  .write_sync_run_log(
    source_id = "ishare_holdings",
    summary = summary_dt,
    local_path = local_path,
    params = list(tickers = tickers),
    run_started_at = run_started_at,
    run_finished_at = Sys.time()
  )
  summary_dt
}
