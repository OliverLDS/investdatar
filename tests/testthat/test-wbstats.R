test_that("wbstats normalization standardizes long indicator data", {
  normalize_wb <- getFromNamespace(".normalize_wbstats_data", "investdatar")

  raw_dt <- data.table::data.table(
    indicatorID = "NY.GDP.MKTP.CD",
    country = "World",
    iso2c = "1W",
    iso3c = "WLD",
    date = c("2024", "2025"),
    value = c("100", "110")
  )

  out <- normalize_wb(raw_dt, indicator = "NY.GDP.MKTP.CD", country = "World", freq = "Y")

  expect_equal(
    names(out)[1:8],
    c("source", "indicator_id", "country", "iso2c", "iso3c", "frequency", "date", "value")
  )
  expect_equal(out$source[[1]], "wbstats")
  expect_equal(out$frequency[[1]], "Y")
  expect_s3_class(out$date, "Date")
  expect_type(out$value, "double")
})

test_that("wbstats wrapper omits null and default-only optional arguments", {
  captured_default <- NULL
  captured_explicit <- NULL

  out_default <- testthat::with_mocked_bindings(
    wb_data = function(...) {
      captured_default <<- list(...)
      data.table::data.table(
        indicatorID = "NY.GDP.MKTP.CD",
        country = "US",
        iso2c = "US",
        iso3c = "USA",
        date = "2024",
        value = "1"
      )
    },
    investdatar::get_source_data_wbstats("NY.GDP.MKTP.CD", country = "US"),
    .package = "wbstats"
  )

  out_explicit <- testthat::with_mocked_bindings(
    wb_data = function(...) {
      captured_explicit <<- list(...)
      data.table::data.table(
        indicatorID = "NY.GDP.MKTP.CD",
        country = "US",
        iso2c = "US",
        iso3c = "USA",
        date = "2024",
        value = "1"
      )
    },
    investdatar::get_source_data_wbstats("NY.GDP.MKTP.CD", country = "US", freq = "Y"),
    .package = "wbstats"
  )

  expect_false("cache" %in% names(captured_default))
  expect_false("gapfill" %in% names(captured_default))
  expect_false("date_as_class_date" %in% names(captured_default))
  expect_false("freq" %in% names(captured_default))
  expect_equal(captured_explicit$freq, "Y")
  expect_equal(out_default$indicator_id[[1]], "NY.GDP.MKTP.CD")
  expect_equal(out_explicit$indicator_id[[1]], "NY.GDP.MKTP.CD")
})

test_that("sync_local_wbstats_data works with mocked provider calls", {
  local_dir <- withr::local_tempdir()

  mocked_dt <- data.table::data.table(
    source = "wbstats",
    indicator_id = "NY.GDP.MKTP.CD",
    country = "World",
    iso2c = "1W",
    iso3c = "WLD",
    frequency = "Y",
    date = as.Date(c("2024-01-01", "2025-01-01")),
    value = c(100, 110)
  )
  mocked_utime <- as.POSIXct("2026-03-27 00:00:00", tz = "UTC")

  res <- testthat::with_mocked_bindings(
    get_source_data_wbstats = function(indicator, country = "countries_only", start_date = NULL, end_date = NULL, mrv = NULL, mrnev = NULL, cache = NULL, freq = "Y", gapfill = FALSE, date_as_class_date = FALSE, lang = NULL) {
      mocked_dt
    },
    get_source_utime_wbstats = function(freq = "Y", tz = "UTC") mocked_utime,
    investdatar::sync_local_wbstats_data("NY.GDP.MKTP.CD", "World", freq = "Y", local_path = local_dir),
    .package = "investdatar"
  )

  local_dt <- investdatar::get_local_wbstats_data("NY.GDP.MKTP.CD", "World", freq = "Y", local_path = local_dir)

  expect_true(res$updated)
  expect_equal(nrow(local_dt), 2L)
  expect_equal(local_dt$value, c(100, 110))
  expect_equal(readRDS(sub("\\.rds$", ".meta.rds", res$file_path))$source_updated_at, mocked_utime)
})
