test_that("sync_local_data deduplicates and writes sidecar metadata", {
  out_path <- file.path(withr::local_tempdir(), "series.rds")

  first <- data.table::data.table(
    date = as.Date(c("2026-01-01", "2026-01-02")),
    value = c(1, 2)
  )
  second <- data.table::data.table(
    date = as.Date(c("2026-01-02", "2026-01-03")),
    value = c(2, 3)
  )

  res1 <- investdatar::sync_local_data(first, out_path, key_cols = "date", source_utime = as.POSIXct("2026-01-02 00:00:00", tz = "UTC"))
  res2 <- investdatar::sync_local_data(second, out_path, key_cols = "date", source_utime = as.POSIXct("2026-01-03 00:00:00", tz = "UTC"))

  expect_true(file.exists(out_path))
  expect_true(file.exists(sub("\\.rds$", ".meta.rds", out_path)))
  expect_equal(res1$n_rows, 2L)
  expect_equal(res2$n_rows, 3L)
  expect_equal(res2$n_new_rows, 1L)
  expect_equal(investdatar::get_local_data_meta(out_path)$n_rows, 3L)
})

test_that("detect_time_gaps works for fixed and calendar frequencies", {
  candle_dt <- data.table::data.table(
    datetime = as.POSIXct(c("2026-03-26 00:00:00", "2026-03-26 08:00:00"), tz = "UTC")
  )
  fred_dt <- data.table::data.table(
    date = as.Date(c("2026-01-01", "2026-03-01"))
  )

  candle_gaps <- investdatar::detect_time_gaps(candle_dt, time_col = "datetime", frequency = "4H")
  monthly_gaps <- investdatar::detect_time_gaps(fred_dt, time_col = "date", frequency = "Monthly")

  expect_equal(nrow(candle_gaps), 1L)
  expect_equal(candle_gaps$expected_seconds[[1]], 4 * 3600)
  expect_equal(as.Date(monthly_gaps$missing_time[[1]], origin = "1970-01-01"), as.Date("2026-02-01"))
})
