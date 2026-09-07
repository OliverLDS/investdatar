test_that("CNH diagnostic writes field-level read-only differences", {
  local <- data.table::data.table(
    date = as.Date(c("2026-09-01", "2026-09-02")),
    source = c("quantmod_yahoo", "eastmoney"),
    datetime = as.POSIXct(c("2026-09-01", "2026-09-02"), tz = "UTC"),
    open = c(6.70, 6.71), high = c(6.72, 6.73), low = c(6.69, 6.70),
    close = c(6.71, 6.72), volume = c(1, 1)
  )
  fresh <- data.table::copy(local)
  fresh[, `:=`(source = "eastmoney", open = open + c(0.001, 0), close = close + c(0.001, 0))]
  out <- testthat::with_mocked_bindings(
    get_local_quantmod_OHLC = function(...) local,
    .fetch_eastmoney_ohlc = function(...) fresh,
    diagnose_cnh_cache_integrity(
      local_path = withr::local_tempdir(), output_dir = withr::local_tempdir(),
      from = as.Date("2026-09-01"), to = as.Date("2026-09-02")
    ),
    .package = "investdatar"
  )
  expect_equal(out$summary$symbol, "CNH=X")
  expect_equal(out$summary$difference_rows, 2L)
  expect_true(all(out$differences$classification == "unexplained"))
  expect_true(all(out$differences$fresh_source == "eastmoney"))
  expect_true(all(out$differences$tolerance_absolute == 0.0001))
  expect_true(file.exists(out$artifact_paths$json))
  expect_true(file.exists(out$artifact_paths$csv))
})

test_that("CNH diagnostic records Eastmoney unavailability without cache changes", {
  local <- data.table::data.table(
    date = as.Date("2026-09-01"), source = "eastmoney",
    datetime = as.POSIXct("2026-09-01", tz = "UTC"),
    open = 6.70, high = 6.72, low = 6.69, close = 6.71, volume = 1
  )
  before <- data.table::copy(local)
  out <- testthat::with_mocked_bindings(
    get_local_quantmod_OHLC = function(...) local,
    .fetch_eastmoney_ohlc = function(...) stop("Eastmoney unavailable", call. = FALSE),
    diagnose_cnh_cache_integrity(
      local_path = withr::local_tempdir(), output_dir = withr::local_tempdir(),
      from = as.Date("2026-09-01"), to = as.Date("2026-09-01")
    ),
    .package = "investdatar"
  )
  expect_equal(out$summary$status, "provider_unavailable")
  expect_equal(out$summary$difference_rows, 0L)
  expect_match(out$summary$comparison_error, "Eastmoney unavailable")
  expect_equal(local, before)
})
