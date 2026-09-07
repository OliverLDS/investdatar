test_that("CSI 300 cache audit writes versioned read-only discrepancy artifacts", {
  local_dir <- withr::local_tempdir()
  audit_dir <- file.path(local_dir, "audit")
  cached <- data.table::data.table(
    source = c("quantmod_yahoo", "eastmoney"), symbol = "000300.SS", interval = "1d",
    datetime = as.POSIXct(c("2026-08-10", "2026-08-11"), tz = "UTC"),
    date = as.Date(c("2026-08-10", "2026-08-11")),
    open = c(4700, 4707), high = c(4710, 4715), low = c(4690, 4700),
    close = c(4702.025, 4707.29), volume = c(1, 2), adj_close = c(NA_real_, NA_real_)
  )
  saveRDS(cached, file.path(local_dir, "000300.SS__yahoo__1d.rds"))
  fresh <- data.table::copy(cached)
  fresh[, `:=`(
    source = "eastmoney", close = c(4702.025, 4663.789),
    high = c(4710, 4716), low = c(4690, 4701)
  )]

  out <- testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(ticker, label, from, to) {
      expect_equal(ticker, "1.000300")
      expect_equal(label, "000300.SS")
      expect_equal(as.Date(from), as.Date("2026-08-10"))
      expect_equal(as.Date(to), as.Date("2026-08-11"))
      fresh
    },
    investdatar::audit_csi300_cache_integrity(
      local_path = local_dir, output_dir = audit_dir,
      baostock_corroboration = list(queried = FALSE, note = "external spike only")
    ),
    .package = "investdatar"
  )

  expect_equal(nrow(out$discrepancies), 1L)
  expect_equal(out$discrepancies$date, as.Date("2026-08-11"))
  expect_equal(out$discrepancies$cached_source, "eastmoney")
  expect_equal(out$discrepancies$eastmoney_source, "eastmoney")
  expect_equal(out$discrepancies$close_difference, 43.5)
  expect_true(file.exists(out$artifact_paths$json))
  expect_true(file.exists(out$artifact_paths$csv))
  expect_match(basename(out$artifact_paths$json), "csi300_cache_integrity_v1_0_0_")
  expect_equal(readRDS(file.path(local_dir, "000300.SS__yahoo__1d.rds")), cached)

  report <- jsonlite::fromJSON(out$artifact_paths$json, simplifyVector = FALSE)
  expect_equal(report$schema_version, "1.0.0")
  expect_equal(report$discrepancy_rows, 1L)
  expect_equal(report$baostock_corroboration$note, "external spike only")
  csv <- data.table::fread(out$artifact_paths$csv)
  expect_equal(csv$date, as.Date("2026-08-11"))
  expect_true(all(c("cached_open", "eastmoney_open", "cached_source", "raw_close_difference", "close_abs_difference") %in% names(csv)))
})

test_that("CSI 300 cache audit uses a strict close tolerance and retains empty CSV headers", {
  local_dir <- withr::local_tempdir()
  cached <- data.table::data.table(
    source = "quantmod_yahoo", symbol = "000300.SS", interval = "1d",
    datetime = as.POSIXct("2026-08-10", tz = "UTC"), date = as.Date("2026-08-10"),
    open = 1, high = 2, low = 0.5, close = 1, volume = 1, adj_close = NA_real_
  )
  saveRDS(cached, file.path(local_dir, "000300.SS__yahoo__1d.rds"))
  fresh <- data.table::copy(cached)[, `:=`(source = "eastmoney", close = 1.004)]

  out <- testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(...) fresh,
    investdatar::audit_csi300_cache_integrity(local_path = local_dir, output_dir = file.path(local_dir, "audit")),
    .package = "investdatar"
  )

  expect_equal(nrow(out$discrepancies), 0L)
  expect_true(file.exists(out$artifact_paths$csv))
  expect_equal(out$summary$baostock_corroboration$queried, FALSE)
})

test_that("CSI 300 cache audit rejects invalid tolerance without provider requests", {
  expect_error(
    investdatar::audit_csi300_cache_integrity(close_tolerance = -0.01),
    "non-negative"
  )
})
