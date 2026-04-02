test_that("sync_local_fred_data uses mocked provider functions", {
  local_dir <- withr::local_tempdir()

  mocked_data <- data.table::data.table(
    date = as.Date(c("2026-02-01", "2026-03-01")),
    value = c(4.2, 4.3)
  )
  mocked_utime <- as.POSIXct("2026-03-20 00:00:00", tz = "UTC")

  res <- testthat::with_mocked_bindings(
    get_source_data_fred = function(series_id, config = NULL) {
      expect_equal(series_id, "FEDFUNDS")
      mocked_data
    },
    get_source_utime_fred = function(series_id, config = NULL, from_server = FALSE, tz = "America/Chicago") {
      expect_equal(series_id, "FEDFUNDS")
      mocked_utime
    },
    investdatar::sync_local_fred_data("FEDFUNDS", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- readRDS(file.path(local_dir, "FEDFUNDS.rds"))
  local_meta <- readRDS(file.path(local_dir, "FEDFUNDS.meta.rds"))

  expect_true(res$updated)
  expect_equal(local_dt$value, mocked_data$value)
  expect_equal(local_meta$source_updated_at, mocked_utime)
})

test_that("sync_local_okx_candle supports mocked latest and history fetches", {
  local_dir <- withr::local_tempdir()

  latest_dt <- data.table::data.table(
    source = "okx",
    symbol = "BTC-USDT-SWAP",
    interval = "4H",
    datetime = as.POSIXct(c("2026-03-26 00:00:00", "2026-03-26 04:00:00"), tz = "UTC"),
    date = as.Date(c("2026-03-26", "2026-03-26")),
    open = c(1, 2),
    high = c(2, 3),
    low = c(0.5, 1.5),
    close = c(1.5, 2.5),
    volume = c(10, 20)
  )
  hist_dt <- data.table::data.table(
    source = "okx",
    symbol = "BTC-USDT-SWAP",
    interval = "4H",
    datetime = as.POSIXct(c("2026-03-25 16:00:00", "2026-03-25 20:00:00"), tz = "UTC"),
    date = as.Date(c("2026-03-25", "2026-03-25")),
    open = c(0.8, 0.9),
    high = c(1.0, 1.1),
    low = c(0.7, 0.8),
    close = c(0.9, 1.0),
    volume = c(8, 9)
  )

  res_latest <- testthat::with_mocked_bindings(
    get_source_data_okx_candle = function(inst_id, bar, limit = 100L, config, tz = "UTC") latest_dt,
    get_source_utime_okx_candle = function(bar, tz = "UTC") as.POSIXct("2026-03-26 04:00:00", tz = "UTC"),
    investdatar::sync_local_okx_candle("BTC-USDT-SWAP", "4H", config = list(), local_path = local_dir, mode = "latest"),
    .package = "investdatar"
  )

  res_hist <- testthat::with_mocked_bindings(
    get_source_hist_data_okx_candle = function(inst_id, bar, before = NULL, limit = 100L, config, tz = "UTC") hist_dt,
    investdatar::sync_local_okx_candle("BTC-USDT-SWAP", "4H", config = list(), local_path = local_dir, mode = "history"),
    .package = "investdatar"
  )

  local_dt <- readRDS(file.path(local_dir, "BTC-USDT-SWAP_4H.rds"))

  expect_true(res_latest$updated)
  expect_true(res_hist$updated)
  expect_equal(nrow(local_dt), 4L)
  expect_equal(local_dt$datetime[[1]], as.POSIXct("2026-03-25 16:00:00", tz = "UTC"))
})

test_that("sync_local_binance_klines writes data under the binance local layout", {
  local_dir <- withr::local_tempdir()

  mocked_dt <- data.table::data.table(
    source = "binance",
    symbol = "ETHUSDT",
    interval = "1m",
    datetime = as.POSIXct(c("2026-03-26 00:00:00", "2026-03-26 00:01:00"), tz = "UTC"),
    date = as.Date(c("2026-03-26", "2026-03-26")),
    open = c(1, 2),
    high = c(2, 3),
    low = c(0.5, 1.5),
    close = c(1.5, 2.5),
    volume = c(10, 20)
  )

  res <- testthat::with_mocked_bindings(
    get_source_data_binance_klines = function(symbol = "ETHUSDT", interval = "1m", start_time = NULL, end_time = NULL, limit = 1500L, tz = "UTC", paginate = TRUE) mocked_dt,
    investdatar::sync_local_binance_klines("ETHUSDT", "1m", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- investdatar::get_local_binance_klines("ETHUSDT", "1m", local_path = local_dir)

  expect_true(res$updated)
  expect_equal(nrow(local_dt), 2L)
  expect_equal(local_dt$close, c(1.5, 2.5))
})

test_that("sync_local_ishare_holdings writes holdings snapshots under the ishare local layout", {
  local_dir <- withr::local_tempdir()

  mocked_dt <- data.table::data.table(
    ticker = c("IVV", "IVV"),
    updated_date = as.Date(c("2026-03-26", "2026-03-26")),
    holding_ticker = c("AAPL", "MSFT"),
    holding_name = c("Apple Inc.", "Microsoft Corp."),
    sector = c("Information Technology", "Information Technology"),
    asset_class = c("Equity", "Equity"),
    weight_pct = c(7.1, 6.2),
    location = c("United States", "United States"),
    exchange = c("NASDAQ", "NASDAQ")
  )

  res <- testthat::with_mocked_bindings(
    get_source_data_ishare_holdings = function(ticker, ishare_mega_data = NULL, cache_dir = NULL, local_path = NULL) mocked_dt,
    get_source_utime_ishare = function(tz = "America/New_York", check_online = TRUE) as.POSIXct("2026-03-26 00:00:00", tz = "UTC"),
    investdatar::sync_local_ishare_holdings("IVV", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- investdatar::get_local_ishare_holdings("IVV", local_path = local_dir)

  expect_true(res$updated)
  expect_equal(nrow(local_dt), 2L)
  expect_equal(local_dt$holding_ticker, c("AAPL", "MSFT"))
  expect_equal(local_dt$ticker, c("IVV", "IVV"))
})

test_that("sync_local_ishare_holdings migrates legacy snapshot lists into long format", {
  local_dir <- withr::local_tempdir()
  legacy_path <- file.path(local_dir, "DYNF_holdings.rds")

  saveRDS(
    list(
      `2026-03-25` = data.table::data.table(
        Ticker = "AAPL",
        Name = "Apple Inc.",
        Sector = "Information Technology",
        `Asset Class` = "Equity",
        `Weight (%)` = "7.10",
        Location = "United States",
        Exchange = "NASDAQ"
      )
    ),
    legacy_path
  )

  mocked_dt <- data.table::data.table(
    ticker = "DYNF",
    updated_date = as.Date("2026-03-26"),
    holding_ticker = "MSFT",
    holding_name = "Microsoft Corp.",
    sector = "Information Technology",
    asset_class = "Equity",
    weight_pct = 6.2,
    location = "United States",
    exchange = "NASDAQ"
  )

  res <- testthat::with_mocked_bindings(
    get_source_data_ishare_holdings = function(ticker, ishare_mega_data = NULL, cache_dir = NULL, local_path = NULL) mocked_dt,
    get_source_utime_ishare = function(tz = "America/New_York", check_online = TRUE) as.POSIXct("2026-03-26 00:00:00", tz = "UTC"),
    investdatar::sync_local_ishare_holdings("DYNF", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- readRDS(legacy_path)

  expect_true(res$updated)
  expect_s3_class(local_dt, "data.table")
  expect_equal(names(local_dt), c("ticker", "updated_date", "holding_ticker", "holding_name", "sector", "asset_class", "weight_pct", "location", "exchange"))
  expect_equal(local_dt$holding_ticker, c("AAPL", "MSFT"))
})

test_that("sync_local_quantmod_OHLC uses the yahoo local layout", {
  local_dir <- withr::local_tempdir()

  mocked_dt <- data.table::data.table(
    source = "quantmod_yahoo",
    symbol = "SPY",
    interval = "1d",
    datetime = as.POSIXct(c("2026-03-25 00:00:00", "2026-03-26 00:00:00"), tz = "UTC"),
    date = as.Date(c("2026-03-25", "2026-03-26")),
    open = c(1, 2),
    high = c(2, 3),
    low = c(0.5, 1.5),
    close = c(1.5, 2.5),
    volume = c(10, 20)
  )

  res <- testthat::with_mocked_bindings(
    fetch_quantmod_OHLC = function(ticker, label = ticker, from, to, src = "yahoo", raw_data = FALSE) mocked_dt,
    investdatar::sync_local_quantmod_OHLC("SPY", from = "2026-03-25", to = "2026-03-26", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- investdatar::get_local_quantmod_OHLC("SPY", local_path = local_dir)

  expect_true(res$updated)
  expect_equal(nrow(local_dt), 2L)
  expect_equal(local_dt$close, c(1.5, 2.5))
})

test_that("sync_local_quantmod_OHLC refreshes existing daily rows with revised values", {
  local_dir <- withr::local_tempdir()

  stale_dt <- data.table::data.table(
    source = "quantmod_yahoo",
    symbol = "HSI",
    interval = "1d",
    datetime = as.POSIXct(c("2026-03-30 00:00:00", "2026-03-31 00:00:00"), tz = "UTC"),
    date = as.Date(c("2026-03-30", "2026-03-31")),
    open = c(1, 2),
    high = c(2, 3),
    low = c(0.5, 1.5),
    close = c(NA_real_, 2.5),
    volume = c(10, 20)
  )
  saveRDS(stale_dt, file.path(local_dir, "HSI__yahoo__1d.rds"))

  refreshed_dt <- data.table::data.table(
    source = "quantmod_yahoo",
    symbol = "HSI",
    interval = "1d",
    datetime = as.POSIXct(c("2026-03-30 00:00:00", "2026-03-31 00:00:00", "2026-04-01 00:00:00"), tz = "UTC"),
    date = as.Date(c("2026-03-30", "2026-03-31", "2026-04-01")),
    open = c(1, 2, 3),
    high = c(2, 3, 4),
    low = c(0.5, 1.5, 2.5),
    close = c(2.1, 2.5, 3.5),
    volume = c(10, 20, 30)
  )

  res <- testthat::with_mocked_bindings(
    fetch_quantmod_OHLC = function(ticker, label = ticker, from, to, src = "yahoo", raw_data = FALSE) refreshed_dt,
    investdatar::sync_local_quantmod_OHLC("HSI", from = "2026-03-30", to = "2026-04-01", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- investdatar::get_local_quantmod_OHLC("HSI", local_path = local_dir)

  expect_true(res$updated)
  expect_equal(res$n_new_rows, 1L)
  expect_equal(local_dt[date == as.Date("2026-03-30"), close][[1]], 2.1)
  expect_equal(max(local_dt$date), as.Date("2026-04-01"))
  expect_equal(nrow(local_dt), 3L)
})

test_that("sync_local_quantmod_OHLC surfaces upstream quantmod errors", {
  local_dir <- withr::local_tempdir()

  testthat::with_mocked_bindings(
    fetch_quantmod_OHLC = function(ticker, label = ticker, from, to, src = "yahoo", raw_data = FALSE) {
      stop("quantmod failed to fetch 'DX-Y.NYB' from source 'yahoo': no data for symbol", call. = FALSE)
    },
    expect_error(
      investdatar::sync_local_quantmod_OHLC("DX-Y.NYB", from = "2026-03-01", to = "2026-03-31", local_path = local_dir),
      "quantmod failed to fetch 'DX-Y.NYB' from source 'yahoo': no data for symbol"
    ),
    .package = "investdatar"
  )
})

test_that("describe_quantmod_data defaults to local date coverage when from and to are omitted", {
  local_dt <- data.table::data.table(
    source = "quantmod_yahoo",
    symbol = "DX-Y.NYB",
    interval = "1d",
    datetime = as.POSIXct(c("2026-03-01 00:00:00", "2026-03-03 00:00:00"), tz = "UTC"),
    date = as.Date(c("2026-03-01", "2026-03-03")),
    open = c(1, 2),
    high = c(2, 3),
    low = c(0.5, 1.5),
    close = c(1.5, 2.5),
    volume = c(10, 20)
  )

  out <- testthat::with_mocked_bindings(
    get_local_quantmod_OHLC = function(label, src = "yahoo", interval = "1d", local_path = NULL) {
      expect_equal(label, "DX-Y.NYB")
      local_dt
    },
    fetch_quantmod_OHLC = function(ticker, label = ticker, from, to, src = "yahoo", raw_data = FALSE) {
      expect_equal(from, as.Date("2026-03-01"))
      expect_equal(to, as.Date("2026-03-03"))
      local_dt
    },
    investdatar::describe_quantmod_data("DX-Y.NYB"),
    .package = "investdatar"
  )

  expect_match(out, "quantmod_yahoo")
})
