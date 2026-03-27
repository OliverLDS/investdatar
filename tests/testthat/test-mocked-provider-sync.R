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
