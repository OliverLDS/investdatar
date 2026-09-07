test_that("degraded cache policy is opt-in and restricted to Eastmoney fallbacks", {
  helper <- getFromNamespace(".yahoo_degraded_cache_policy", "investdatar")
  expect_false(helper(data.table::data.table(
    fallback_source = "yahoo", degraded_cache_enabled = TRUE,
    degraded_cache_max_staleness_days = 5
  ))$enabled)
  expect_true(helper(data.table::data.table(
    fallback_source = "eastmoney", degraded_cache_enabled = TRUE,
    degraded_cache_max_staleness_days = 5
  ))$enabled)
  expect_false(helper(data.table::data.table(
    fallback_source = "eastmoney", degraded_cache_enabled = FALSE,
    degraded_cache_max_staleness_days = 5
  ))$enabled)
})

test_that("eligible degraded cache requires completed finite rows and age limit", {
  assess <- getFromNamespace(".yahoo_degraded_cache_assessment", "investdatar")
  cache <- data.table::data.table(
    source = "quantmod_yahoo", symbol = "CNH=X", interval = "1d",
    datetime = as.POSIXct(c("2026-09-03", "2026-09-04"), tz = "UTC"),
    date = as.Date(c("2026-09-03", "2026-09-04")),
    open = c(7.1, 7.2), high = c(7.2, 7.3), low = c(7.0, 7.1),
    close = c(7.15, 7.25), volume = c(1, 1), adj_close = c(7.15, 7.25)
  )
  row <- data.table::data.table(
    yahoo_finance_ticker = "CNH=X", fallback_source = "eastmoney",
    degraded_cache_enabled = TRUE, degraded_cache_max_staleness_days = 5
  )

  eligible <- testthat::with_mocked_bindings(
    get_completed_local_quantmod_OHLC = function(...) cache,
    assess("CNH=X", row, withr::local_tempdir(), as_of = as.Date("2026-09-07")),
    .package = "investdatar"
  )
  expect_true(eligible$eligible)
  expect_equal(eligible$last_completed_cached_date, as.Date("2026-09-04"))
  expect_equal(eligible$cache_source, "quantmod_yahoo")
  expect_equal(eligible$staleness_days, 3)

  stale <- testthat::with_mocked_bindings(
    get_completed_local_quantmod_OHLC = function(...) cache,
    assess("CNH=X", row, withr::local_tempdir(), as_of = as.Date("2026-09-20")),
    .package = "investdatar"
  )
  expect_false(stale$eligible)
  expect_equal(stale$reason, "cache_exceeds_max_staleness")

  invalid_cache <- cache
  invalid_cache$close[[2L]] <- NA_real_
  invalid <- testthat::with_mocked_bindings(
    get_completed_local_quantmod_OHLC = function(...) invalid_cache,
    assess("CNH=X", row, withr::local_tempdir(), as_of = as.Date("2026-09-07")),
    .package = "investdatar"
  )
  expect_false(invalid$eligible)
  expect_equal(invalid$reason, "cache_contains_invalid_ohlc")
})

test_that("an unresolved overlap integrity finding blocks degraded success", {
  assess <- getFromNamespace(".yahoo_degraded_cache_assessment", "investdatar")
  local_dir <- withr::local_tempdir()
  audit_dir <- file.path(local_dir, "_audits", "yahoo_recent_overlap")
  dir.create(audit_dir, recursive = TRUE)
  jsonlite::write_json(
    list(instruments = list(list(ticker = "CNH=X", status = "integrity_issue"))),
    file.path(audit_dir, "audit.json"), auto_unbox = TRUE
  )
  row <- data.table::data.table(
    yahoo_finance_ticker = "CNH=X", fallback_source = "eastmoney",
    degraded_cache_enabled = TRUE, degraded_cache_max_staleness_days = 5
  )
  cache <- data.table::data.table(
    source = "quantmod_yahoo", symbol = "CNH=X", interval = "1d",
    datetime = as.POSIXct("2026-09-04", tz = "UTC"), date = as.Date("2026-09-04"),
    open = 7.1, high = 7.2, low = 7, close = 7.15, volume = 1, adj_close = 7.15
  )
  result <- testthat::with_mocked_bindings(
    get_completed_local_quantmod_OHLC = function(...) cache,
    assess("CNH=X", row, local_dir, as_of = as.Date("2026-09-07")),
    .package = "investdatar"
  )
  expect_false(result$eligible)
  expect_equal(result$reason, "cache_has_unresolved_integrity_finding")
})

test_that("Yahoo registry uses degraded_cache only for eligible provider failures", {
  registry <- data.table::data.table(
    yahoo_finance_ticker = c("CNH=X", "SPY"),
    fallback_source = c("eastmoney", NA_character_),
    fallback_ticker = c("133.USDCNH", NA_character_),
    degraded_cache_enabled = c(TRUE, FALSE),
    degraded_cache_max_staleness_days = c(5, NA_real_)
  )
  cache <- data.table::data.table(
    source = "quantmod_yahoo", symbol = "CNH=X", interval = "1d",
    datetime = as.POSIXct("2026-09-04", tz = "UTC"),
    date = as.Date("2026-09-04"), open = 7.1, high = 7.2, low = 7,
    close = 7.15, volume = 1, adj_close = 7.15
  )
  summary <- testthat::with_mocked_bindings(
    sync_local_quantmod_OHLC = function(ticker, ...) {
      if (ticker == "CNH=X") stop("primary and fallback unavailable")
      list(updated = TRUE, n_rows = 1L, n_new_rows = 0L,
           fetch_method = "quantmod", fetch_attempts = 1L,
           primary_error = NA_character_, primary_error_class = NA_character_,
           invalid_ohlc_rows = 0L)
    },
    get_completed_local_quantmod_OHLC = function(...) cache,
    investdatar::sync_all_yahoofinance_registry_data(
      registry = registry, local_path = withr::local_tempdir(),
      to = as.Date("2026-09-07"), retry_delay_seconds = 0
    ),
    .package = "investdatar"
  )
  expect_equal(summary[yahoo_finance_ticker == "CNH=X", status][[1L]], "degraded_cache")
  expect_equal(summary[yahoo_finance_ticker == "CNH=X", health][[1L]], "degraded")
  expect_equal(summary[yahoo_finance_ticker == "SPY", status][[1L]], "success")
  expect_equal(summary[yahoo_finance_ticker == "CNH=X", degraded_symbols][[1L]], "CNH=X")
  expect_true(investdatar::is_sync_run_successful(list(summary = summary)))
  expect_false(investdatar::is_sync_run_successful(list(summary =
    data.table::rbindlist(list(summary, data.table::data.table(status = "error", error = "broken")),
                          fill = TRUE))))
})
