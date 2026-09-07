.csi300_repair_test_cache <- function() {
  data.table::data.table(
    source = c("quantmod_yahoo", "quantmod_yahoo"), symbol = "000300.SS", interval = "1d",
    datetime = as.POSIXct(c("2026-08-10", "2026-08-11"), tz = "UTC"),
    date = as.Date(c("2026-08-10", "2026-08-11")),
    open = c(4700, 4707), high = c(4710, 4715), low = c(4690, 4700),
    close = c(4680.82, 4707.29), volume = c(1, 2), adj_close = NA_real_
  )
}

.write_csi300_repair_audit <- function(path, cached, candidate, dates = cached$date[[1L]]) {
  old <- cached[date %in% dates]
  fresh <- candidate[date %in% dates]
  discrepancies <- data.table::data.table(
    date = old$date,
    cached_source = old$source,
    cached_open = old$open,
    cached_high = old$high,
    cached_low = old$low,
    cached_close = old$close,
    cached_volume = old$volume,
    cached_adj_close = old$adj_close,
    eastmoney_open = fresh$open,
    eastmoney_high = fresh$high,
    eastmoney_low = fresh$low,
    eastmoney_close = fresh$close,
    eastmoney_volume = fresh$volume,
    eastmoney_adj_close = fresh$adj_close
  )
  jsonlite::write_json(
    list(
      schema_version = "1.0.0", audit_id = "audit-test", symbol = "000300.SS",
      comparison_source = list(provider = "eastmoney", symbol = "1.000300"),
      close_tolerance = 0.01, discrepancies = as.data.frame(discrepancies)
    ),
    path, auto_unbox = TRUE, pretty = TRUE, na = "null"
  )
}

.csi300_repair_test_candidate <- function(cached) {
  candidate <- data.table::copy(cached)
  candidate[, `:=`(
    source = "eastmoney", open = c(4698.82, 4689.45), high = c(4714.46, 4715.88),
    low = c(4659.47, 4660.47), close = c(4702.02, 4663.79), volume = c(10, 20)
  )]
  candidate
}

test_that("CSI 300 repair rejects dates absent from the audit before fetching", {
  local_dir <- withr::local_tempdir()
  cached <- .csi300_repair_test_cache()
  candidate <- .csi300_repair_test_candidate(cached)
  cache_path <- file.path(local_dir, "000300.SS__yahoo__1d.rds")
  audit_path <- file.path(local_dir, "audit.json")
  saveRDS(cached, cache_path)
  .write_csi300_repair_audit(audit_path, cached, candidate)

  testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(...) stop("must not fetch"),
    expect_error(
      investdatar::repair_csi300_cache_integrity(audit_path, as.Date("2026-08-11"), local_path = local_dir),
      "absent from the audit discrepancy set"
    ),
    .package = "investdatar"
  )
  expect_equal(readRDS(cache_path), cached)
})

test_that("CSI 300 repair rejects a fresh candidate that drifted from the audit", {
  local_dir <- withr::local_tempdir()
  cached <- .csi300_repair_test_cache()
  candidate <- .csi300_repair_test_candidate(cached)
  cache_path <- file.path(local_dir, "000300.SS__yahoo__1d.rds")
  audit_path <- file.path(local_dir, "audit.json")
  saveRDS(cached, cache_path)
  .write_csi300_repair_audit(audit_path, cached, candidate)
  drifted <- data.table::copy(candidate)[, close := 4600]

  testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(...) drifted,
    expect_error(
      investdatar::repair_csi300_cache_integrity(audit_path, as.Date("2026-08-10"), local_path = local_dir),
      "no longer matches"
    ),
    .package = "investdatar"
  )
  expect_equal(readRDS(cache_path), cached)
})

test_that("CSI 300 repair dry run writes a proposed log without cache mutation", {
  local_dir <- withr::local_tempdir()
  cached <- .csi300_repair_test_cache()
  candidate <- .csi300_repair_test_candidate(cached)
  cache_path <- file.path(local_dir, "000300.SS__yahoo__1d.rds")
  audit_path <- file.path(local_dir, "audit.json")
  saveRDS(cached, cache_path)
  .write_csi300_repair_audit(audit_path, cached, candidate)

  out <- testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(...) candidate,
    investdatar::repair_csi300_cache_integrity(audit_path, as.Date("2026-08-10"), local_path = local_dir),
    .package = "investdatar"
  )

  expect_true(out$dry_run)
  expect_false(out$updated)
  expect_null(out$backup_path)
  expect_true(file.exists(out$repair_log_path))
  expect_equal(readRDS(cache_path), cached)
  log <- jsonlite::fromJSON(out$repair_log_path, simplifyVector = TRUE)
  expect_equal(log$status, "dry_run")
  expect_equal(log$audit_id, "audit-test")
  expect_equal(log$repairs$old_close, 4680.82)
  expect_equal(log$repairs$new_close, 4702.02)
  expect_equal(log$repairs$new_source, "eastmoney")
})

test_that("CSI 300 repair backs up and changes only approved rows", {
  local_dir <- withr::local_tempdir()
  cached <- .csi300_repair_test_cache()
  candidate <- .csi300_repair_test_candidate(cached)
  cache_path <- file.path(local_dir, "000300.SS__yahoo__1d.rds")
  audit_path <- file.path(local_dir, "audit.json")
  saveRDS(cached, cache_path)
  .write_csi300_repair_audit(audit_path, cached, candidate)

  out <- testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(...) candidate,
    investdatar::repair_csi300_cache_integrity(
      audit_path, as.Date("2026-08-10"), local_path = local_dir, dry_run = FALSE
    ),
    .package = "investdatar"
  )

  repaired <- readRDS(cache_path)
  expect_true(out$updated)
  expect_true(file.exists(out$backup_path))
  expect_equal(readRDS(out$backup_path), cached)
  expect_equal(repaired[date == as.Date("2026-08-10"), close][[1L]], 4702.02)
  expect_equal(repaired[date == as.Date("2026-08-10"), source][[1L]], "eastmoney")
  expect_equal(repaired[date == as.Date("2026-08-11")], cached[date == as.Date("2026-08-11")])
})

test_that("CSI 300 repair restores its backup when the cache write fails", {
  local_dir <- withr::local_tempdir()
  cached <- .csi300_repair_test_cache()
  candidate <- .csi300_repair_test_candidate(cached)
  cache_path <- file.path(local_dir, "000300.SS__yahoo__1d.rds")
  audit_path <- file.path(local_dir, "audit.json")
  saveRDS(cached, cache_path)
  .write_csi300_repair_audit(audit_path, cached, candidate)

  testthat::with_mocked_bindings(
    .fetch_eastmoney_ohlc = function(...) candidate,
    .safe_save_rds = function(...) stop("simulated write failure"),
    expect_error(
      investdatar::repair_csi300_cache_integrity(
        audit_path, as.Date("2026-08-10"), local_path = local_dir, dry_run = FALSE
      ),
      "cache was restored"
    ),
    .package = "investdatar"
  )
  expect_equal(readRDS(cache_path), cached)
})
