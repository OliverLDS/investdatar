test_that("Treasury Atom parser captures feed-level updated timestamp", {
  feed_text <- paste(
    '<?xml version="1.0" encoding="utf-8" standalone="yes" ?>',
    '<feed xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices" xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" xmlns="http://www.w3.org/2005/Atom">',
    '<updated>2026-04-03T12:31:16Z</updated>',
    '<entry><content type="application/xml"><m:properties><d:NEW_DATE>2026-04-01T00:00:00</d:NEW_DATE><d:BC_10YEAR>4.20</d:BC_10YEAR></m:properties></content></entry>',
    '</feed>'
  )

  parsed <- investdatar:::.parse_treasury_atom_feed(feed_text)

  expect_equal(parsed$source_updated_at, as.POSIXct("2026-04-03 12:31:16", tz = "UTC"))
  expect_equal(parsed$data$BC_10YEAR[[1]], "4.20")
})

test_that("Treasury curve normalizer standardizes long-format series rows", {
  wide_dt <- data.table::data.table(
    NEW_DATE = c("2026-04-01T00:00:00", "2026-04-02T00:00:00"),
    BC_1MONTH = c("4.10", "4.11"),
    BC_2YEAR = c("3.80", "3.81"),
    BC_10YEAR = c("4.20", "4.21"),
    BC_30YEARDISPLAY = c("4.60", "4.61")
  )

  dt <- investdatar:::.normalize_treasury_rates(wide_dt, "par_yield_curve")

  expect_s3_class(dt, "data.table")
  expect_equal(unique(dt$dataset), "par_yield_curve")
  expect_equal(data.table::uniqueN(dt$series_id), 3L)
  expect_true(all(c("dataset", "date", "series_id", "tenor", "measure", "value") %in% names(dt)))
  expect_equal(dt[series_id == "par_yield_curve_10year", value][[1]], 4.2)
})

test_that("Treasury bill normalizer preserves tenor metadata and cusip fields", {
  wide_dt <- data.table::data.table(
    QUOTE_DATE = "2026-04-01T00:00:00",
    ROUND_B1_CLOSE_4WK_2 = "99.50",
    ROUND_B1_YIELD_4WK_2 = "4.15",
    MATURITY_DATE_4WK = "2026-04-30T00:00:00",
    CUSIP_4WK = "912797LC8"
  )

  dt <- investdatar:::.normalize_treasury_rates(wide_dt, "bill_rates")

  expect_equal(nrow(dt), 2L)
  expect_equal(dt[series_id == "bill_close_4wk", maturity_date][[1]], as.Date("2026-04-30"))
  expect_equal(dt[series_id == "bill_yield_4wk", cusip][[1]], "912797LC8")
})

test_that("sync_local_treasury_rates writes data under the treasury local layout", {
  local_dir <- withr::local_tempdir()

  mocked_dt <- data.table::data.table(
    dataset = "par_yield_curve",
    date = as.Date(c("2026-04-01", "2026-04-02")),
    series_id = c("par_yield_curve_2year", "par_yield_curve_2year"),
    tenor = c("2YEAR", "2YEAR"),
    measure = c("yield", "yield"),
    value = c(3.80, 3.81),
    rate_type = c(NA_character_, NA_character_),
    source_field = c("BC_2YEAR", "BC_2YEAR"),
    maturity_date = as.Date(c(NA, NA)),
    cusip = c(NA_character_, NA_character_)
  )

  old_collect <- get(".collect_treasury_dataset", envir = asNamespace("investdatar"))
  assignInNamespace(
    ".collect_treasury_dataset",
    function(dataset, years = NULL) {
      list(
        data = mocked_dt,
        source_updated_at = as.POSIXct("2026-04-03 12:31:16", tz = "UTC")
      )
    },
    ns = "investdatar"
  )
  on.exit(assignInNamespace(".collect_treasury_dataset", old_collect, ns = "investdatar"), add = TRUE)

  res <- investdatar::sync_local_treasury_rates("par_yield_curve", local_path = local_dir)
  local_dt <- investdatar::get_local_treasury_rates("par_yield_curve", local_path = local_dir)
  txt <- investdatar::describe_treasury_rates("par_yield_curve", local_path = local_dir)

  expect_true(res$updated)
  expect_true(file.exists(file.path(local_dir, "par_yield_curve.rds")))
  expect_equal(nrow(local_dt), 2L)
  expect_match(txt, "raw U.S. Treasury rates")
})

test_that("sync_all_treasury_rates returns success and failure summary rows", {
  old_sync_local <- get("sync_local_treasury_rates", envir = asNamespace("investdatar"))
  assignInNamespace(
    "sync_local_treasury_rates",
    function(dataset, years = NULL, local_path = NULL) {
      if (dataset == "real_long_term_rates") stop("download failed")
      list(updated = TRUE, n_rows = 10L, n_new_rows = 2L)
    },
    ns = "investdatar"
  )
  on.exit(assignInNamespace("sync_local_treasury_rates", old_sync_local, ns = "investdatar"), add = TRUE)

  summary_dt <- investdatar::sync_all_treasury_rates(
    datasets = c("bill_rates", "real_long_term_rates"),
    local_path = withr::local_tempdir()
  )

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt[dataset == "bill_rates", status][[1]], "success")
  expect_equal(summary_dt[dataset == "real_long_term_rates", status][[1]], "error")
})
