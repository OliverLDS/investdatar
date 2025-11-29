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
  
  out <- out[, .(
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
  
  list(
    updated_date = updated_date,
    holdings_dt = out[]
  )
 
}

#' @export
get_source_data_ishare <- function(ticker, ishare_mega_data, cache_dir) {
  prod_url <- ishare_mega_data[Ticker == ticker, etf_href]
  file_path <- .download_ishare_xls_file(prod_url, cache_dir = cache_dir)
  .wraggle_historical_data(file_path)
}

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



