test_that("config loading normalizes paths and resolves source config", {
  cfg_dir <- withr::local_tempdir()
  cfg_path <- file.path(cfg_dir, "investdatar_config.yaml")
  writeLines(
    c(
      "FRED:",
      "  data_path: ./fred_data",
      "  registry_file: fred_macro_series_registry.json",
      "Crypto:",
      "  data_path: ./crypto_data",
      "iShare:",
      "  data_path: ./ishare_data"
    ),
    cfg_path
  )

  cfg <- investdatar::load_investdatar_config(cfg_path)

  expect_true(dir.exists(dirname(investdatar::get_source_data_path("fred", config = cfg, create = TRUE))))
  expect_match(investdatar::get_source_data_path("fred", config = cfg), "fred_data$")
  expect_match(investdatar::get_source_data_path("ishare", config = cfg), "ishare_data$")
  registry_file <- investdatar::get_source_config("fred", config = cfg)$registry_file
  expect_equal(basename(registry_file), "fred_macro_series_registry.json")
  expect_match(registry_file, "fred_macro_series_registry\\.json$")
})

test_that("source specs expose provider capabilities and schema contracts", {
  specs <- investdatar::list_source_specs()

  expect_true(all(c("fred", "wbstats", "ishare", "alphavantage", "quantmod", "okx", "binance") %in% names(specs)))
  expect_s3_class(investdatar::get_source_spec("fred"), "investdatar_source_spec")
  expect_equal(investdatar::get_source_spec("wbstats")$resource_type, "single_series")
  expect_equal(investdatar::get_source_spec("okx")$resource_type, "market_ohlcv")
  expect_true(isTRUE(investdatar::get_source_spec("okx")$capabilities$pagination))
  expect_false(isTRUE(investdatar::get_source_spec("alphavantage")$capabilities$source_utime))
  expect_equal(investdatar::get_source_spec("binance")$schema$key_cols, c("symbol", "interval", "datetime"))
})
