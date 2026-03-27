test_that("market schema normalizer standardizes core columns", {
  raw_dt <- data.table::data.table(
    date = as.Date(c("2026-03-25", "2026-03-26")),
    open = c("1", "2"),
    high = c("2", "3"),
    low = c("0.5", "1.5"),
    close = c("1.5", "2.5"),
    volume = c("10", "20"),
    extra_col = c("a", "b")
  )

  out <- getFromNamespace(".standardize_market_ohlcv", "investdatar")(
    raw_dt,
    source = "mock_source",
    symbol = "ABC",
    interval = "1d",
    time_col = "date"
  )

  expect_equal(
    names(out)[1:10],
    c("source", "symbol", "interval", "datetime", "date", "open", "high", "low", "close", "volume")
  )
  expect_equal(out$source[[1]], "mock_source")
  expect_equal(out$symbol[[1]], "ABC")
  expect_s3_class(out$datetime, "POSIXct")
  expect_type(out$open, "double")
  expect_equal(out$extra_col, c("a", "b"))
})
