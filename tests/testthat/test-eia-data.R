.eia_test_payload <- function(data, total = nrow(data), frequency = "weekly") {
  list(response = list(
    total = as.character(total), frequency = frequency,
    dateFormat = "YYYY-MM-DD", data = data
  ))
}

test_that("EIA retrieval paginates and standardizes series observations", {
  calls <- list()
  config <- list(api_key = "test-key", url = "https://api.eia.gov/v2/seriesid")
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query = NULL, headers = character(), ...) {
      calls[[length(calls) + 1L]] <<- query
      if (query$offset == 0L) {
        return(.eia_test_payload(data.frame(
          period = c("2026-07-10", "2026-07-17"), value = c("100", "NA"),
          unit = "Thousand Barrels", seriesDescription = "Crude stocks",
          stringsAsFactors = FALSE
        ), total = 3L))
      }
      .eia_test_payload(data.frame(
        period = "2026-07-24", value = "102", unit = "Thousand Barrels",
        seriesDescription = "Crude stocks", stringsAsFactors = FALSE
      ), total = 3L)
    },
    investdatar::get_source_data_eia(
      "PET.WCESTUS1.W", label = "Commercial crude stocks",
      config = config, from = "2026-07-01", page_size = 2L
    ),
    .package = "investdatar"
  )

  expect_equal(length(calls), 2L)
  expect_equal(calls[[2L]]$offset, 2L)
  expect_equal(calls[[1L]]$start, "2026-07-01")
  expect_equal(nrow(out), 3L)
  expect_s3_class(out$date, "Date")
  expect_true(is.na(out[period == "2026-07-17", value][[1L]]))
  expect_equal(unique(out$label), "Commercial crude stocks")
})

test_that("EIA period parsing supports annual, quarterly, monthly and hourly data", {
  parse_period <- getFromNamespace(".eia_period_date", "investdatar")
  periods <- c("2025", "2026-Q2", "2026-07", "2026-07-24", "2026-07-24T13")
  expect_equal(
    parse_period(periods),
    as.Date(c("2025-01-01", "2026-04-01", "2026-07-01", "2026-07-24", "2026-07-24"))
  )
})

test_that("EIA sync overlaps local observations and upserts revisions", {
  local_dir <- withr::local_tempdir()
  existing <- data.table::data.table(
    source = "eia", series_id = "PET.WCESTUS1.W", label = "Stocks",
    frequency = "weekly", period = "2026-07-24", date = as.Date("2026-07-24"),
    datetime = as.POSIXct(NA, tz = "UTC"), value = 100,
    unit = "Thousand Barrels", description = "Stocks"
  )
  saveRDS(existing, file.path(local_dir, "PET.WCESTUS1.W.rds"))
  result <- testthat::with_mocked_bindings(
    get_source_data_eia = function(series_id, label, config, from, to, page_size) {
      expect_equal(as.Date(from), as.Date("2026-06-23"))
      revised <- data.table::copy(existing)
      revised$value <- 101
      revised
    },
    get_source_utime_eia = function(series_id, config) as.POSIXct("2026-07-24", tz = "UTC"),
    investdatar::sync_local_eia_data(
      "PET.WCESTUS1.W", config = list(api_key = "key", url = "url"),
      from = "1982-01-01", local_path = local_dir
    ),
    .package = "investdatar"
  )

  expect_true(result$updated)
  expect_equal(result$n_new_rows, 0L)
  expect_equal(investdatar::get_local_eia_data("PET.WCESTUS1.W", local_dir)$value, 101)
})

test_that("EIA batch sync writes standard run logs", {
  registry <- data.table::data.table(
    series_id = c("GOOD", "BAD"), label = c("Good", "Bad"),
    main_group = "energy", frequency = "weekly", active = TRUE
  )
  local_dir <- withr::local_tempdir()
  summary_dt <- testthat::with_mocked_bindings(
    sync_local_eia_data = function(series_id, label, config, local_path, ...) {
      if (series_id == "BAD") stop("series unavailable")
      list(updated = TRUE, n_rows = 4L, n_new_rows = 1L)
    },
    investdatar::sync_all_eia_registry_data(
      registry = registry, config = list(api_key = "key", url = "url"), local_path = local_dir
    ),
    .package = "investdatar"
  )

  expect_equal(summary_dt$status, c("success", "error"))
  expect_true(all(summary_dt$source_id == "eia"))
  expect_equal(summary_dt[series_id == "BAD", error_message][[1L]], "series unavailable")
  expect_equal(nrow(investdatar::get_latest_sync_run("eia", local_dir)$summary), 2L)
})

test_that("EIA requires a configured API key", {
  expect_error(
    investdatar::get_source_data_eia("PET.WCESTUS1.W", config = list(api_key = "", url = "url")),
    "EIA API key is missing"
  )
})

test_that("shipped EIA registry contains the six physical-market seeds", {
  path <- system.file("extdata", "config", "eia_series_registry.json", package = "investdatar")
  registry <- investdatar::get_eia_registry(path)
  expect_equal(nrow(registry), 6L)
  expect_true(all(c("PET.WCESTUS1.W", "PET.WCRFPUS2.W", "NG.NW2_EPG0_SWO_R48_BCF.W") %in% registry$series_id))
})
