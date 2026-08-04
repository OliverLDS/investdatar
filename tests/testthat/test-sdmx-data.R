.sdmx_csv_response <- function(text) {
  list(
    status_code = 200L, headers = list(`content-type` = "text/csv"),
    content = charToRaw(text), url = "https://example.test/data"
  )
}

.sdmx_fixture_csv <- function() {
  paste(
    "FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE,OBS_STATUS,TITLE",
    "M,US,2025-01,4.375,A,Policy rate",
    "M,US,2025-02,4.375,A,Policy rate",
    sep = "\n"
  )
}

test_that("SDMX URL construction isolates provider dialects", {
  build <- getFromNamespace(".sdmx_build_url", "investdatar")
  expect_equal(
    build("ecb", "https://data-api.ecb.europa.eu/service/", dataflow = "EXR", key = "D.USD.EUR.SP00.A", flow_ref = "EXR"),
    "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A"
  )
  expect_equal(
    build("eurostat", "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0", agency = "ESTAT", dataflow = "PRC_HICP_MIDX", version = "1.0", key = "M.I15.CP00.EA20"),
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/3.0/data/dataflow/ESTAT/PRC_HICP_MIDX/1.0/M.I15.CP00.EA20"
  )
  expect_equal(
    build("bis", "https://stats.bis.org/api/v2", agency = "BIS", dataflow = "WS_CBPOL", version = "1.0", key = "M.US"),
    "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/M.US"
  )
  expect_error(build("bis", "x", dataflow = "flow"), "require agency")
})

test_that("Eurostat uses SDMX 3 time filters", {
  request <- NULL
  testthat::with_mocked_bindings(
    .http_request = function(method, url, query, headers) {
      request <<- query
      .sdmx_csv_response("freq,unit,coicop,geo,TIME_PERIOD,OBS_VALUE\nM,I15,CP00,EA20,2025-01,100")
    },
    investdatar::get_source_data_sdmx(
      "hicp", "eurostat", "https://example.test/sdmx/3.0", agency = "ESTAT",
      dataflow = "PRC_HICP_MIDX", version = "1.0", key = "M.I15.CP00.EA20",
      dimension_cols = c("freq", "unit", "coicop", "geo"),
      from = "2025-01", to = "2025-12"
    ),
    .package = "investdatar"
  )
  expect_equal(request[["c[TIME_PERIOD]"]], "ge:2025-01+le:2025-12")
  expect_null(request$startPeriod)
})

test_that("IMF DataMapper responses use the common long contract", {
  response <- list(values = list(NGDP_RPCH = list(
    USA = list(`2024` = 2.8, `2025` = 2.1),
    PHL = list(`2024` = 5.7, `2025` = 6.1)
  )))
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query) response,
    investdatar::get_source_data_sdmx(
      "imf_growth", "imf", "https://www.imf.org/external/datamapper/api/v2",
      dataflow = "NGDP_RPCH", key = "USA.PHL", from = "2024", to = "2025"
    ),
    .package = "investdatar"
  )
  expect_equal(nrow(out), 4L)
  expect_setequal(out$REF_AREA, c("USA", "PHL"))
  expect_equal(range(out$date), as.Date(c("2024-01-01", "2025-01-01")))
})

test_that("SDMX CSV is standardized while preserving provider columns", {
  request <- NULL
  out <- testthat::with_mocked_bindings(
    .http_request = function(method, url, query, headers) {
      request <<- list(method = method, url = url, query = query, headers = headers)
      .sdmx_csv_response(.sdmx_fixture_csv())
    },
    investdatar::get_source_data_sdmx(
      series_id = "bis_us_policy", provider = "bis",
      base_url = "https://stats.bis.org/api/v2", agency = "BIS",
      dataflow = "WS_CBPOL", version = "1.0", key = "M.US",
      accept = "application/vnd.sdmx.data+csv;version=2.0.0",
      dimension_cols = c("FREQ", "REF_AREA"), from = "2025-01", to = "2025-02"
    ),
    .package = "investdatar"
  )

  expect_equal(request$query$startPeriod, "2025-01")
  expect_equal(request$query$endPeriod, "2025-02")
  expect_equal(nrow(out), 2L)
  expect_equal(out$date, as.Date(c("2025-01-01", "2025-02-01")))
  expect_equal(out$value, c(4.375, 4.375))
  expect_true(all(out$dimension_key == "FREQ=M|REF_AREA=US"))
  expect_true(all(c("OBS_STATUS", "TITLE") %in% names(out)))
})

test_that("SDMX period parser covers common reporting frequencies", {
  parse_period <- getFromNamespace(".sdmx_period_date", "investdatar")
  actual <- parse_period(c("2025", "2025-S2", "2025-Q3", "2025-07", "2025-07-04"))
  expect_equal(actual, as.Date(c("2025-01-01", "2025-07-01", "2025-07-01", "2025-07-01", "2025-07-04")))
})

test_that("SDMX source update time requests only the latest observation", {
  request <- NULL
  value <- testthat::with_mocked_bindings(
    .http_request = function(method, url, query, headers) {
      request <<- query
      .sdmx_csv_response(.sdmx_fixture_csv())
    },
    investdatar::get_source_utime_sdmx(
      "bis_us_policy", "bis", "https://stats.bis.org/api/v2",
      agency = "BIS", dataflow = "WS_CBPOL", version = "1.0", key = "M.US",
      dimension_cols = c("FREQ", "REF_AREA")
    ),
    .package = "investdatar"
  )
  expect_equal(request$lastNObservations, 1L)
  expect_equal(as.Date(value), as.Date("2025-02-01"))
})

test_that("SDMX sync overlaps local dates and upserts revisions", {
  local_dir <- withr::local_tempdir()
  existing <- getFromNamespace(".standardize_sdmx_data", "investdatar")(
    data.table::data.table(FREQ = "M", REF_AREA = "US", TIME_PERIOD = "2025-02", OBS_VALUE = 4.375),
    series_id = "bis_us_policy", provider = "bis", key = "M.US",
    dimension_cols = c("FREQ", "REF_AREA")
  )
  saveRDS(existing, file.path(local_dir, "bis_us_policy.rds"))
  observed_from <- NULL
  result <- testthat::with_mocked_bindings(
    get_source_data_sdmx = function(..., from = NULL, to = NULL) {
      observed_from <<- from
      getFromNamespace(".standardize_sdmx_data", "investdatar")(
        data.table::data.table(
          FREQ = c("M", "M"), REF_AREA = c("US", "US"),
          TIME_PERIOD = c("2025-02", "2025-03"), OBS_VALUE = c(4.25, 4.25)
        ),
        series_id = "bis_us_policy", provider = "bis", key = "M.US",
        dimension_cols = c("FREQ", "REF_AREA")
      )
    },
    investdatar::sync_local_sdmx_data(
      "bis_us_policy", "bis", "https://example.test", agency = "BIS",
      dataflow = "WS_CBPOL", version = "1.0", key = "M.US",
      dimension_cols = c("FREQ", "REF_AREA"), from = "2000-01-01",
      local_path = local_dir, overlap_days = 31L
    ),
    .package = "investdatar"
  )

  expect_equal(as.Date(observed_from), as.Date("2025-01-01"))
  expect_true(result$updated)
  expect_equal(result$n_new_rows, 1L)
  local <- investdatar::get_local_sdmx_data("bis_us_policy", local_dir)
  expect_equal(local[period == "2025-02", value][[1L]], 4.25)
})

test_that("SDMX batch writes the common summary and run log", {
  local_dir <- withr::local_tempdir()
  registry <- data.table::data.table(
    series_id = "test", provider = "ecb", base_url = "https://example.test",
    dataflow = "EXR", active = TRUE
  )
  registry[, key := "D.USD.EUR.SP00.A"]
  out <- testthat::with_mocked_bindings(
    sync_local_sdmx_data = function(...) list(updated = TRUE, n_rows = 2L, n_new_rows = 1L),
    investdatar::sync_all_sdmx_registry_data(registry, local_path = local_dir),
    .package = "investdatar"
  )
  expect_equal(out$status, "success")
  expect_true(all(c("source_id", "elapsed_seconds", "error_class") %in% names(out)))
  expect_equal(investdatar::get_latest_sync_run("sdmx", local_dir)$source_id, "sdmx")
})

test_that("shipped SDMX registry seeds major public macro providers", {
  path <- system.file("extdata", "config", "sdmx_series_registry.json", package = "investdatar")
  registry <- investdatar::get_sdmx_registry(path)
  expect_setequal(registry$provider, c("ecb", "oecd", "bis", "eurostat", "imf"))
  expect_true(all(registry$active))
})
