test_that("add_fred_registry_series updates registry schema without duplicates", {
  registry_path <- file.path(withr::local_tempdir(), "fred_registry.json")
  jsonlite::write_json(
    list(
      list(
        series_id = "FEDFUNDS",
        main_group = "monetary_policy",
        title = "Federal Funds Effective Rate",
        start = "1954-07-01",
        end = "2026-02-01",
        freq = "Monthly",
        units = "Percent",
        season = "Not Seasonally Adjusted",
        update_time = "2026-03-20 16:57:51"
      )
    ),
    registry_path,
    pretty = TRUE,
    auto_unbox = TRUE
  )

  out <- testthat::with_mocked_bindings(
    get_source_metadata_fred = function(series_id, config = NULL) {
      list(
        title = "Gross Domestic Product",
        start = "1947-01-01",
        end = "2025-10-01",
        freq = "Quarterly",
        units = "Billions of Dollars",
        season = "Seasonally Adjusted Annual Rate"
      )
    },
    .confirm_stdin = function(prompt_text) TRUE,
    investdatar::add_fred_registry_series("GDP", main_group = "growth", registry_path = registry_path),
    .package = "investdatar"
  )

  registry <- jsonlite::fromJSON(registry_path, simplifyDataFrame = TRUE)
  expect_equal(nrow(registry), 2L)
  expect_equal(sum(registry$series_id == "GDP"), 1L)
  expect_equal(out$main_group[[1]], "growth")
})

test_that("sync_all_fred_registry_data returns success and failure summary", {
  registry <- data.table::data.table(series_id = c("FEDFUNDS", "BAD"))
  local_dir <- withr::local_tempdir()

  summary_dt <- testthat::with_mocked_bindings(
    sync_local_fred_data = function(series_id, config = NULL, local_path = NULL, from_server = FALSE, tz = "America/Chicago") {
      if (series_id == "BAD") stop("broken source")
      list(updated = TRUE, n_rows = 10L, n_new_rows = 2L)
    },
    investdatar::sync_all_fred_registry_data(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt[series_id == "FEDFUNDS", status][[1]], "success")
  expect_equal(summary_dt[series_id == "BAD", status][[1]], "error")
  run_log <- investdatar::get_latest_sync_run("fred", local_path = local_dir)
  expect_equal(run_log$source_id, "fred")
  expect_equal(nrow(run_log$summary), 2L)
})

test_that("World Bank registry helpers add series and batch sync via mocked helpers", {
  registry_path <- file.path(withr::local_tempdir(), "world_bank_registry.json")
  jsonlite::write_json(
    list(
      list(
        indicator = "NY.GDP.MKTP.CD",
        country = "US",
        freq = "Y",
        main_group = "growth",
        label = "US GDP",
        notes = NA_character_,
        active = TRUE,
        update_time = "2026-04-08 10:00:00"
      )
    ),
    registry_path,
    pretty = TRUE,
    auto_unbox = TRUE,
    null = "null"
  )

  out <- investdatar::add_wbstats_registry_series(
    "FP.CPI.TOTL.ZG",
    country = NULL,
    freq = "Y",
    main_group = "inflation",
    label = "US CPI inflation",
    registry_path = registry_path
  )
  registry <- investdatar::get_wbstats_registry(registry_path = registry_path)

  expect_equal(nrow(registry), 2L)
  expect_equal(sum(registry$indicator == "FP.CPI.TOTL.ZG"), 1L)
  expect_equal(out$country[[1]], "")
  expect_equal(out$main_group[[1]], "inflation")
  local_dir <- withr::local_tempdir()

  summary_dt <- testthat::with_mocked_bindings(
    sync_local_wbstats_data = function(indicator, country, freq = "Y", local_path = NULL, ...) {
      if (indicator == "FP.CPI.TOTL.ZG") expect_equal(country, "countries_only")
      if (indicator == "FP.CPI.TOTL.ZG") stop("download failed")
      list(updated = TRUE, n_rows = 20L, n_new_rows = 1L)
    },
    investdatar::sync_all_wbstats_registry_data(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt[indicator == "NY.GDP.MKTP.CD", status][[1]], "success")
  expect_equal(summary_dt[indicator == "FP.CPI.TOTL.ZG", status][[1]], "error")
  run_log <- investdatar::get_latest_sync_run("wbstats", local_path = local_dir)
  expect_equal(run_log$source_id, "wbstats")
  expect_equal(nrow(run_log$summary), 2L)
})

test_that("iShare registry helpers add tickers and batch sync via mocked helpers", {
  registry_path <- file.path(withr::local_tempdir(), "ishare_registry.json")
  jsonlite::write_json(
    list(list(ticker = "IVV", type = "equity_core")),
    registry_path,
    pretty = TRUE,
    auto_unbox = TRUE
  )

  out <- investdatar::add_ishare_registry_ticker("IAU", type = "commodity", registry_path = registry_path)
  registry <- investdatar::get_ishare_registry(registry_path = registry_path)

  expect_equal(nrow(registry), 2L)
  expect_equal(sum(registry$ticker == "IAU"), 1L)
  expect_equal(out$type[[1]], "commodity")
  local_dir <- withr::local_tempdir()

  summary_dt <- testthat::with_mocked_bindings(
    get_local_ishare_mega_data = function(local_path = NULL) data.table::data.table(Ticker = c("IVV", "IAU"), etf_href = c("u1", "u2")),
    get_source_utime_ishare = function(tz = "America/New_York", check_online = TRUE) as.POSIXct("2026-03-29 00:00:00", tz = "UTC"),
    sync_local_ishare_data = function(ticker, ishare_mega_data = NULL, local_path = NULL, cache_dir = NULL, source_utime = NULL) {
      if (ticker == "IAU") stop("download failed")
      list(updated = TRUE, n_rows = 5L, n_new_rows = 1L)
    },
    investdatar::sync_all_ishare_registry_data(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt[ticker == "IVV", status][[1]], "success")
  expect_equal(summary_dt[ticker == "IAU", status][[1]], "error")
  run_log <- investdatar::get_latest_sync_run("ishare", local_path = local_dir)
  expect_equal(run_log$source_id, "ishare")
  expect_equal(nrow(run_log$summary), 2L)
})

test_that("iShare holdings registry sync returns success and failure summary", {
  registry <- data.table::data.table(
    ticker = c("DYNF", "THRO", "IVV"),
    type = c("active", "active", "equity_core")
  )
  local_dir <- withr::local_tempdir()

  summary_dt <- testthat::with_mocked_bindings(
    get_source_config = function(source, config = get_investdatar_config()) {
      if (tolower(source) == "ishare") {
        return(list(holdings_tickers = c("DYNF", "THRO")))
      }
      list()
    },
    get_local_ishare_mega_data = function(local_path = NULL) data.table::data.table(Ticker = c("IVV", "IAU"), etf_href = c("u1", "u2")),
    get_source_utime_ishare = function(tz = "America/New_York", check_online = TRUE) as.POSIXct("2026-03-29 00:00:00", tz = "UTC"),
    sync_local_ishare_holdings = function(ticker, ishare_mega_data = NULL, local_path = NULL, cache_dir = NULL, source_utime = NULL) {
      if (ticker == "THRO") stop("download failed")
      list(updated = TRUE, n_rows = 25L, n_new_rows = 25L)
    },
    investdatar::sync_all_ishare_registry_holdings(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt$ticker, c("DYNF", "THRO"))
  expect_equal(summary_dt[ticker == "DYNF", status][[1]], "success")
  expect_equal(summary_dt[ticker == "THRO", status][[1]], "error")
  run_log <- investdatar::get_latest_sync_run("ishare_holdings", local_path = local_dir)
  expect_equal(run_log$source_id, "ishare_holdings")
  expect_equal(nrow(run_log$summary), 2L)
})

test_that("Yahoo Finance registry sync returns success and failure summary", {
  registry <- data.table::data.table(
    yahoo_finance_ticker = c("^GSPC", "^VIX")
  )
  local_dir <- withr::local_tempdir()
  local_dt <- data.table::data.table(
    source = "quantmod_yahoo",
    symbol = "^GSPC",
    interval = "1d",
    datetime = as.POSIXct(c("2026-03-20", "2026-03-21"), tz = "UTC"),
    date = as.Date(c("2026-03-20", "2026-03-21")),
    open = c(1, 2),
    high = c(1, 2),
    low = c(1, 2),
    close = c(1, 2),
    volume = c(1, 2),
    adj_close = c(1, 2)
  )
  local_file <- getFromNamespace(".quantmod_local_filename", "investdatar")("^GSPC", src = "yahoo", interval = "1d")
  saveRDS(local_dt, file.path(local_dir, local_file))

  summary_dt <- testthat::with_mocked_bindings(
    sync_local_quantmod_OHLC = function(ticker, label = ticker, from, to, src = "yahoo", local_path = NULL) {
      expect_equal(label, ticker)
      if (ticker == "^GSPC") expect_equal(as.Date(from), as.Date("2026-03-11"))
      if (ticker == "^VIX") expect_equal(as.Date(from), as.Date("2025-02-24"))
      if (ticker == "^VIX") stop("download failed")
      list(updated = TRUE, n_rows = 100L, n_new_rows = 3L)
    },
    investdatar::sync_all_yahoofinance_registry_data(
      to = "2026-03-31",
      registry = registry,
      local_path = local_dir
    ),
    .package = "investdatar"
  )

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt[yahoo_finance_ticker == "^GSPC", status][[1]], "success")
  expect_equal(summary_dt[yahoo_finance_ticker == "^VIX", status][[1]], "error")
  expect_equal(summary_dt[yahoo_finance_ticker == "^GSPC", latest_local_date][[1]], as.Date("2026-03-21"))
  run_log <- investdatar::get_latest_sync_run("yahoofinance", local_path = local_dir)
  expect_equal(run_log$source_id, "yahoofinance")
  expect_equal(nrow(run_log$summary), 2L)
})

test_that("RSS registry sync writes a batch run log", {
  registry <- data.table::data.table(
    feed_id = "demo_feed",
    url = "https://example.com/feed.xml",
    parser = "plain"
  )
  local_dir <- withr::local_tempdir()

  summary_dt <- testthat::with_mocked_bindings(
    sync_local_rss_data = function(feed_id, url, parser = c("plain", "gdpnow"), local_path = NULL) {
      list(updated = TRUE, n_rows = 4L, n_new_rows = 1L)
    },
    investdatar::sync_all_rss_registry_data(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(summary_dt$feed_id[[1]], "demo_feed")
  run_log <- investdatar::get_latest_sync_run("rss", local_path = local_dir)
  expect_equal(run_log$source_id, "rss")
  expect_equal(run_log$summary$feed_id[[1]], "demo_feed")
})
