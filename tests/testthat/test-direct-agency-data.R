test_that("BLS standardization preserves monthly values and footnotes", {
  series <- list(
    seriesID = "CUSR0000SA0",
    data = data.frame(
      year = "2026", period = "M07", periodName = "July", value = "321.5", latest = "true",
      footnotes = I(list(data.frame(code = "P", text = "Preliminary.")))
    )
  )
  out <- getFromNamespace(".standardize_bls_series", "investdatar")(series, "CPI")
  expect_equal(out$date, as.Date("2026-07-01"))
  expect_equal(out$value, 321.5)
  expect_match(out$footnote, "Preliminary")
})

test_that("BEA Regional standardization preserves geography and units", {
  source <- data.frame(
    GeoFIPS = "01000", GeoName = "Alabama", TimePeriod = "2025", DataValue = "320,500",
    CL_UNIT = "Millions of dollars", UNIT_MULT = "6"
  )
  out <- getFromNamespace(".standardize_bea_regional", "investdatar")(source, "state_gdp", "GDP")
  expect_equal(out$geo_id, "01000")
  expect_equal(out$value, 320500)
  expect_equal(out$date, as.Date("2025-01-01"))
})

test_that("Census response and period helpers normalize matrix JSON", {
  response <- rbind(
    c("cell_value", "data_type_code", "time_slot_id", "category_code", "seasonally_adj", "error_data", "time"),
    c("725,000", "SM", "1", "44X72", "yes", "no", "2026-06")
  )
  dt <- getFromNamespace(".census_response_table", "investdatar")(response)
  expect_equal(dt$cell_value, "725,000")
  expect_equal(getFromNamespace(".census_period_date", "investdatar")(dt$time), as.Date("2026-06-01"))
})

test_that("direct agency registries contain selective official series", {
  bls <- investdatar::get_bls_registry(system.file("extdata", "config", "bls_series_registry.json", package = "investdatar"))
  bea <- investdatar::get_bea_registry(system.file("extdata", "config", "bea_series_registry.json", package = "investdatar"))
  census <- investdatar::get_census_registry(system.file("extdata", "config", "census_series_registry.json", package = "investdatar"))
  expect_equal(nrow(bls), 3L)
  expect_equal(bea$table_name, c("SAGDP2N", "SAINC1N"))
  expect_equal(census$dataset, "marts")
})
