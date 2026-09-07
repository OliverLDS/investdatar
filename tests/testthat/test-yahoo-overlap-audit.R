.yahoo_overlap_test_catalog <- function(rows) {
  data.table::data.table(
    instrument_id = vapply(rows, `[[`, character(1), "instrument_id"),
    canonical_symbol = vapply(rows, `[[`, character(1), "canonical_symbol"),
    asset_class = vapply(rows, `[[`, character(1), "asset_class"),
    instrument_type = vapply(rows, `[[`, character(1), "instrument_type"),
    market_calendar = vapply(rows, `[[`, character(1), "market_calendar"),
    provider_identifiers = lapply(rows, `[[`, "provider_identifiers")
  )
}

.yahoo_overlap_test_rows <- function(source = "quantmod_yahoo", close = 100, volume = 1000,
                                     dates = as.Date(c("2026-09-03", "2026-09-04"))) {
  data.table::data.table(
    source = source, symbol = "SPY", interval = "1d",
    datetime = as.POSIXct(dates, tz = "UTC"), date = dates,
    open = close - 1, high = close + 1, low = close - 2, close = close,
    volume = volume, adj_close = close
  )
}

test_that("Yahoo overlap audit separates audited, skipped, informational, and calendar findings", {
  local <- .yahoo_overlap_test_rows(source = "eastmoney")
  yahoo <- .yahoo_overlap_test_rows(source = "quantmod_yahoo")
  registry <- data.table::data.table(yahoo_finance_ticker = c("SPY", "QQQ"))
  catalog <- .yahoo_overlap_test_catalog(list(list(
    instrument_id = "etf.us.spy", canonical_symbol = "SPY", asset_class = "equity",
    instrument_type = "etf", market_calendar = "XNYS", provider_identifiers = list(yahoo = "SPY")
  )))

  out <- testthat::with_mocked_bindings(
    get_instrument_catalog = function(...) catalog,
    get_completed_local_quantmod_OHLC = function(label, local_path = NULL, as_of = NULL, ...) local,
    .yahoo_overlap_fetch = function(ticker, from, to, ...) yahoo,
    audit_yahoofinance_recent_overlap(
      registry = registry, local_path = withr::local_tempdir(),
      output_dir = withr::local_tempdir(), as_of = as.POSIXct("2026-09-07", tz = "UTC"),
      overlap_days = 5L
    ),
    .package = "investdatar"
  )

  expect_equal(out$summary$manifest_summary$registered, 2L)
  expect_equal(out$summary$manifest_summary$audited, 1L)
  expect_equal(out$summary$manifest_summary$skipped, 1L)
  expect_equal(out$summary$manifest_summary$informational_instruments, 1L)
  expect_equal(out$summary$manifest_summary$integrity_issue_instruments, 0L)
  expect_true(any(out$findings$issue_type == "skipped_missing_catalog_metadata"))
  expect_true(any(out$findings$issue_type == "provenance_change"))
  expect_true(all(out$findings[out$findings$issue_type == "provenance_change", severity] == "informational"))
  expect_true(file.exists(out$artifact_paths$json))
  expect_true(file.exists(out$artifact_paths$csv))
  report <- jsonlite::fromJSON(out$artifact_paths$json, simplifyVector = TRUE)
  expect_equal(report$baostock$queried, FALSE)
  expect_equal(report$manifest_summary$skipped, 1L)
})

test_that("Yahoo overlap audit excludes weekends and detects missing completed Yahoo bars", {
  local <- .yahoo_overlap_test_rows(dates = as.Date(c("2026-09-03", "2026-09-04")))
  yahoo <- .yahoo_overlap_test_rows(dates = as.Date(c("2026-09-03", "2026-09-04", "2026-09-05")))
  registry <- data.table::data.table(yahoo_finance_ticker = "SPY")
  catalog <- .yahoo_overlap_test_catalog(list(list(
    instrument_id = "etf.us.spy", canonical_symbol = "SPY", asset_class = "equity",
    instrument_type = "etf", market_calendar = "XNYS", provider_identifiers = list(yahoo = "SPY")
  )))

  out <- testthat::with_mocked_bindings(
    get_instrument_catalog = function(...) catalog,
    get_completed_local_quantmod_OHLC = function(...) local,
    .yahoo_overlap_fetch = function(...) yahoo,
    audit_yahoofinance_recent_overlap(
      registry = registry, local_path = withr::local_tempdir(), output_dir = withr::local_tempdir(),
      as_of = as.POSIXct("2026-09-07", tz = "UTC"), overlap_days = 5L
    ),
    .package = "investdatar"
  )

  expect_false(any(out$findings$issue_type == "missing_in_cache" & out$findings$date == as.Date("2026-09-05")))
})

test_that("Yahoo overlap audit uses asset-appropriate FX tolerance", {
  local <- .yahoo_overlap_test_rows(close = 1.0000)
  local[, symbol := "EURUSD=X"]
  yahoo <- data.table::copy(local)
  yahoo[, close := 1.00005]
  registry <- data.table::data.table(yahoo_finance_ticker = "EURUSD=X")
  catalog <- .yahoo_overlap_test_catalog(list(list(
    instrument_id = "fx.eur-usd", canonical_symbol = "EUR/USD", asset_class = "foreign_exchange",
    instrument_type = "spot_fx", market_calendar = "FX_24_5", provider_identifiers = list(yahoo = "EURUSD=X")
  )))

  out <- testthat::with_mocked_bindings(
    get_instrument_catalog = function(...) catalog,
    get_completed_local_quantmod_OHLC = function(...) local,
    .yahoo_overlap_fetch = function(...) yahoo,
    audit_yahoofinance_recent_overlap(
      registry = registry, local_path = withr::local_tempdir(), output_dir = withr::local_tempdir(),
      as_of = as.POSIXct("2026-09-07", tz = "UTC"), overlap_days = 5L
    ),
    .package = "investdatar"
  )
  expect_false(any(out$findings$issue_type == "ohlc_discrepancy"))
})

test_that("Yahoo overlap audit uses Eastmoney only for permitted fallback corroboration", {
  local <- .yahoo_overlap_test_rows(close = 100)
  local[, `:=`(symbol = "000300.SS", source = "eastmoney")]
  yahoo <- data.table::copy(local)
  yahoo[, `:=`(source = "quantmod_yahoo", close = 90)]
  eastmoney <- data.table::copy(local)
  calls <- character()
  registry <- data.table::data.table(
    yahoo_finance_ticker = "000300.SS", fallback_source = "eastmoney", fallback_ticker = "1.000300"
  )
  catalog <- .yahoo_overlap_test_catalog(list(list(
    instrument_id = "index.cn.csi-300", canonical_symbol = "CSI300", asset_class = "equity",
    instrument_type = "equity_index", market_calendar = "XSHG", provider_identifiers = list(yahoo = "000300.SS")
  )))

  out <- testthat::with_mocked_bindings(
    get_instrument_catalog = function(...) catalog,
    get_completed_local_quantmod_OHLC = function(...) local,
    .yahoo_overlap_fetch = function(...) yahoo,
    .fetch_eastmoney_ohlc = function(ticker, ...) {
      calls <<- c(calls, ticker)
      eastmoney
    },
    audit_yahoofinance_recent_overlap(
      registry = registry, local_path = withr::local_tempdir(), output_dir = withr::local_tempdir(),
      as_of = as.POSIXct("2026-09-07", tz = "UTC"), overlap_days = 5L
    ),
    .package = "investdatar"
  )

  expect_equal(calls, "1.000300")
  expect_equal(out$summary$manifest_summary$integrity_issue_instruments, 1L)
  expect_true(any(out$findings$issue_type == "ohlc_discrepancy"))
  expect_true(all(out$findings[out$findings$issue_type == "provenance_change", related_integrity_issue]))
})
