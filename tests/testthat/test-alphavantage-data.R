.alphavantage_fixture <- function() {
  list(`Time Series (Daily)` = list(
    `2026-08-03` = list(`1. open` = "10", `2. high` = "12", `3. low` = "9", `4. close` = "11", `5. volume` = "1000"),
    `2026-08-04` = list(`1. open` = "11", `2. high` = "13", `3. low` = "10", `4. close` = "12", `5. volume` = "1200")
  ))
}

test_that("Alpha Vantage fetch validates and standardizes API data", {
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query = NULL, headers = character(), ...) .alphavantage_fixture(),
    investdatar::get_source_data_alphavantage_ts_daily("AAPL", config = list(api_key = "test", url = "https://example.test")),
    .package = "investdatar"
  )
  expect_equal(out$date, as.Date(c("2026-08-03", "2026-08-04")))
  expect_equal(out$close, c(11, 12))
  expect_equal(unique(out$source), "alphavantage")
})

test_that("Alpha Vantage sync uses full once then compact and writes run logs", {
  local_dir <- withr::local_tempdir()
  modes <- character()
  registry <- data.table::data.table(symbol = c("AAPL", "BAD"), active = TRUE)
  summary <- testthat::with_mocked_bindings(
    get_source_data_alphavantage_ts_daily = function(symbol, mode, config = NULL) {
      modes <<- c(modes, mode)
      if (symbol == "BAD") stop("rate limited")
      .standardize_market_ohlcv(
        data.table::data.table(date = as.Date("2026-08-04"), open = 1, high = 2, low = 1, close = 2, volume = 10),
        "alphavantage", symbol, "1d", "date"
      )
    },
    investdatar::sync_all_alphavantage_registry_data(registry, config = list(), local_path = local_dir),
    .package = "investdatar"
  )
  expect_equal(summary$status, c("success", "error"))
  expect_equal(modes, c("full", "full"))
  expect_equal(investdatar::get_local_alphavantage_data("AAPL", local_dir)$close, 2)
  expect_equal(investdatar::get_latest_sync_run("alphavantage", local_dir)$source_id, "alphavantage")
})

test_that("Alpha Vantage API messages become actionable errors", {
  expect_error(
    testthat::with_mocked_bindings(
      .http_get_json = function(...) list(Note = "rate limit reached"),
      investdatar::get_source_data_alphavantage_ts_daily("AAPL", config = list(api_key = "test", url = "x")),
      .package = "investdatar"
    ),
    "rate limit reached"
  )
})
