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

test_that("local RDS writes replace existing files without leaving temporary files", {
  out_dir <- withr::local_tempdir()
  out_path <- file.path(out_dir, "atomic.rds")

  getFromNamespace(".safe_save_rds", "investdatar")(list(value = 1L), out_path)
  getFromNamespace(".safe_save_rds", "investdatar")(list(value = 2L), out_path)

  expect_equal(readRDS(out_path)$value, 2L)
  expect_equal(list.files(out_dir, pattern = "^\\.atomic\\.rds\\."), character())
})

test_that("batch sync summaries receive the common contract additively", {
  started_at <- as.POSIXct("2026-08-03 01:00:00", tz = "UTC")
  finished_at <- as.POSIXct("2026-08-03 01:00:02", tz = "UTC")
  input <- data.table::data.table(
    series_id = c("GOOD", "BAD"),
    status = c("success", "error"),
    updated = c(TRUE, FALSE),
    error = c(NA_character_, "failed")
  )

  out <- getFromNamespace(".normalize_sync_summary", "investdatar")(
    input,
    source_id = "fred",
    run_started_at = started_at,
    run_finished_at = finished_at
  )

  expect_true(all(c(
    "source_id", "source_utime", "local_utime", "error_class",
    "error_message", "http_status", "started_at", "finished_at",
    "elapsed_seconds"
  ) %in% names(out)))
  expect_equal(out$series_id, input$series_id)
  expect_equal(out$source_id, c("fred", "fred"))
  expect_equal(out[series_id == "BAD", error_message][[1]], "failed")
  expect_equal(out[series_id == "BAD", error_class][[1]], "sync_error")
  expect_equal(out$elapsed_seconds, c(2, 2))
})

test_that("batch run success reflects row-level errors", {
  successful <- list(summary = data.table::data.table(status = "success", error = NA_character_))
  partial_failure <- list(summary = data.table::data.table(
    status = c("success", "error"), error = c(NA_character_, "upstream failed")
  ))
  message_failure <- list(summary = data.table::data.table(status = "success", error_message = "invalid payload"))
  empty_registry <- list(summary = data.table::data.table())

  expect_true(investdatar::is_sync_run_successful(successful))
  expect_false(investdatar::is_sync_run_successful(partial_failure))
  expect_false(investdatar::is_sync_run_successful(message_failure))
  expect_true(investdatar::is_sync_run_successful(empty_registry))
  expect_false(investdatar::is_sync_run_successful(NULL))
})

test_that("sync_local_data refreshes existing keyed rows when source values change", {
  out_path <- file.path(withr::local_tempdir(), "series.rds")

  first <- data.table::data.table(
    date = as.Date(c("2026-01-01", "2026-01-02")),
    value = c(1, NA_real_)
  )
  second <- data.table::data.table(
    date = as.Date(c("2026-01-02", "2026-01-03")),
    value = c(2, 3)
  )

  investdatar::sync_local_data(first, out_path, key_cols = "date")
  res2 <- investdatar::sync_local_data(second, out_path, key_cols = "date")
  local_dt <- readRDS(out_path)

  expect_true(res2$updated)
  expect_equal(res2$n_new_rows, 1L)
  expect_equal(local_dt[date == as.Date("2026-01-02"), value][[1]], 2)
  expect_equal(nrow(local_dt), 3L)
})

test_that("sync_local_data_batches merges pages before one local sync", {
  out_path <- file.path(withr::local_tempdir(), "series.rds")

  batches <- list(
    data.table::data.table(date = as.Date(c("2026-01-01", "2026-01-02")), value = c(1, 2)),
    data.table::data.table(date = as.Date(c("2026-01-02", "2026-01-03")), value = c(2, 3))
  )

  res <- investdatar::sync_local_data_batches(
    batches = batches,
    local_file_path = out_path,
    key_cols = "date",
    order_cols = "date"
  )
  local_dt <- readRDS(out_path)

  expect_true(res$updated)
  expect_equal(res$n_rows, 3L)
  expect_equal(local_dt$value, c(1, 2, 3))
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
