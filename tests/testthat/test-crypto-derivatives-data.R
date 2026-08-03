test_that("Binance funding and open interest normalize to one schema", {
  funding_raw <- data.table::data.table(
    symbol = "BTCUSDT", fundingTime = c(1735689600000, 1735718400000),
    fundingRate = c("0.0001", "-0.0002"), markPrice = c("95000", "95500")
  )
  funding <- getFromNamespace(".standardize_crypto_derivatives", "investdatar")(
    funding_raw, "binance", "funding_rate", "BTCUSDT", "funding"
  )
  expect_equal(funding$value, c(0.0001, -0.0002))
  expect_equal(funding$mark_price, c(95000, 95500))
  expect_true(all(is.na(funding$open_interest)))

  oi_raw <- data.table::data.table(
    symbol = "BTCUSDT", timestamp = 1735689600000,
    sumOpenInterest = "1000.5", sumOpenInterestValue = "95000000"
  )
  oi <- getFromNamespace(".standardize_crypto_derivatives", "investdatar")(
    oi_raw, "binance", "open_interest", "BTCUSDT", "1h"
  )
  expect_equal(oi$value, 1000.5)
  expect_equal(oi$open_interest_value, 95000000)
  expect_true(is.na(oi$funding_rate))
})

test_that("OKX funding history paginates backward and honors lower bound", {
  calls <- list()
  from <- as.POSIXct("2025-01-01 00:00:00", tz = "UTC")
  from_ms <- as.numeric(from) * 1000
  page_one <- data.frame(
    instId = "BTC-USDT-SWAP", fundingTime = as.character(c(from_ms + 16 * 3600000, from_ms + 8 * 3600000)),
    fundingRate = c("0.0001", "0.0002"), stringsAsFactors = FALSE
  )
  page_two <- data.frame(
    instId = "BTC-USDT-SWAP", fundingTime = as.character(c(from_ms, from_ms - 8 * 3600000)),
    fundingRate = c("0.0003", "0.0004"), stringsAsFactors = FALSE
  )
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query) {
      calls[[length(calls) + 1L]] <<- query
      list(code = "0", msg = "", data = if (length(calls) == 1L) page_one else page_two)
    },
    investdatar::get_source_data_crypto_derivatives(
      "okx", "funding_rate", "BTC-USDT-SWAP", "funding",
      from = from, limit = 2L
    ),
    .package = "investdatar"
  )
  expect_equal(length(calls), 2L)
  expect_equal(nrow(out), 3L)
  expect_true(min(out$datetime) >= from)
  expect_equal(out$value, c(0.0003, 0.0002, 0.0001))
})

test_that("crypto derivatives dispatch validates supported combinations", {
  expect_error(
    investdatar::get_source_data_crypto_derivatives("okx", "open_interest", "BTC-USDT-SWAP"),
    "Unsupported"
  )
  expect_error(
    investdatar::get_source_data_crypto_derivatives("binance", "open_interest", "BTCUSDT"),
    "requires interval"
  )
})

test_that("OKX source update time performs one latest-row request", {
  calls <- 0L
  value <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query) {
      calls <<- calls + 1L
      list(
        code = "0", msg = "",
        data = data.frame(
          instId = "BTC-USDT-SWAP", fundingTime = "1735689600000",
          fundingRate = "0.0001", stringsAsFactors = FALSE
        )
      )
    },
    investdatar::get_source_utime_crypto_derivatives(
      "okx", "funding_rate", "BTC-USDT-SWAP", "funding"
    ),
    .package = "investdatar"
  )
  expect_equal(calls, 1L)
  expect_equal(value, as.POSIXct(1735689600, origin = "1970-01-01", tz = "UTC"))
})

test_that("crypto derivatives sync overlaps and upserts", {
  local_dir <- withr::local_tempdir()
  existing <- getFromNamespace(".standardize_crypto_derivatives", "investdatar")(
    data.table::data.table(
      symbol = "BTCUSDT", fundingTime = 1735689600000,
      fundingRate = "0.0001", markPrice = "95000"
    ),
    "binance", "funding_rate", "BTCUSDT", "funding"
  )
  saveRDS(existing, file.path(local_dir, "binance__funding_rate__BTCUSDT__funding.rds"))
  observed_from <- NULL
  result <- testthat::with_mocked_bindings(
    get_source_data_crypto_derivatives = function(provider, dataset_type, symbol, interval, from, to, limit) {
      observed_from <<- from
      getFromNamespace(".standardize_crypto_derivatives", "investdatar")(
        data.table::data.table(
          symbol = c("BTCUSDT", "BTCUSDT"),
          fundingTime = c(1735689600000, 1735718400000),
          fundingRate = c("0.0002", "0.0003"), markPrice = c("95000", "95500")
        ),
        "binance", "funding_rate", "BTCUSDT", "funding"
      )
    },
    investdatar::sync_local_crypto_derivatives(
      "binance", "funding_rate", "BTCUSDT", "funding",
      from = "2020-01-01", local_path = local_dir, overlap_days = 2L
    ),
    .package = "investdatar"
  )
  expect_equal(observed_from, max(existing$datetime) - as.difftime(2, units = "days"))
  expect_true(result$updated)
  expect_equal(result$n_new_rows, 1L)
  local <- investdatar::get_local_crypto_derivatives("binance", "funding_rate", "BTCUSDT", "funding", local_dir)
  expect_equal(local[datetime == min(datetime), value][[1L]], 0.0002)
})

test_that("crypto derivatives batch writes common summary and run log", {
  local_dir <- withr::local_tempdir()
  registry <- data.table::data.table(
    provider = "binance", dataset_type = "funding_rate", symbol = "BTCUSDT",
    interval = "funding", start = "2025-01-01", active = TRUE
  )
  out <- testthat::with_mocked_bindings(
    sync_local_crypto_derivatives = function(...) list(updated = TRUE, n_rows = 2L, n_new_rows = 2L),
    investdatar::sync_all_crypto_derivatives_registry_data(registry, local_path = local_dir),
    .package = "investdatar"
  )
  expect_equal(out$status, "success")
  expect_equal(out$source_id, "crypto_derivatives")
  expect_equal(investdatar::get_latest_sync_run("crypto_derivatives", local_dir)$source_id, "crypto_derivatives")
})

test_that("shipped derivatives registry covers BTC and ETH across historical datasets", {
  path <- system.file("extdata", "config", "crypto_derivatives_registry.json", package = "investdatar")
  registry <- investdatar::get_crypto_derivatives_registry(path)
  expect_equal(nrow(registry), 6L)
  expect_setequal(registry$provider, c("binance", "okx"))
  expect_setequal(registry$dataset_type, c("funding_rate", "open_interest"))
  expect_true(all(registry$active))
})
