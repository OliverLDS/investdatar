test_that("config loading normalizes paths and resolves source config", {
  cfg_dir <- withr::local_tempdir()
  cfg_path <- file.path(cfg_dir, "investdatar_config.yaml")
  writeLines(
    c(
      "FRED:",
      "  data_path: ./fred_data",
      "  registry_file: fred_macro_series_registry.json",
      "RSS:",
      "  data_path: ./rss_data",
      "  registry_file: rss_feed_registry.json",
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
  expect_match(investdatar::get_source_data_path("rss", config = cfg), "rss_data$")
  expect_match(investdatar::get_source_data_path("ishare", config = cfg), "ishare_data$")
  registry_file <- investdatar::get_source_config("fred", config = cfg)$registry_file
  expect_equal(basename(registry_file), "fred_macro_series_registry.json")
  expect_match(registry_file, "fred_macro_series_registry\\.json$")
  expect_match(investdatar::get_source_config("rss", config = cfg)$registry_file, "rss_feed_registry\\.json$")
})

test_that("shipped example config is available and loads with normalized relative paths", {
  example_path <- system.file("extdata", "investdatar_config_example.yaml", package = "investdatar")
  expect_true(nzchar(example_path))
  expect_true(file.exists(example_path))

  cfg <- investdatar::load_investdatar_config(example_path)

  expect_match(investdatar::get_source_data_path("fred", config = cfg), "extdata/(\\./)?data/fred$")
  expect_match(investdatar::get_source_data_path("rss", config = cfg), "extdata/(\\./)?data/rss$")
  expect_match(investdatar::get_source_data_path("yahoo", config = cfg), "extdata/(\\./)?data/yahoo_finance$")
  expect_match(
    investdatar::get_source_config("fred", config = cfg)$registry_file,
    "extdata/(\\./)?config/fred_macro_series_registry\\.json$"
  )
})

test_that("missing config paths fail with the onboarding hint", {
  expect_error(
    investdatar::load_investdatar_config(""),
    "INVESTDATAR_CONFIG is not set.*investdatar_config_example.yaml"
  )

  expect_error(
    investdatar::load_investdatar_config(file.path(withr::local_tempdir(), "missing.yaml")),
    "Config file does not exist: .*investdatar_config_example.yaml"
  )
})

test_that("missing source data paths fail with a config-focused message", {
  expect_error(
    investdatar::get_source_data_path("fred", config = list(FRED = list())),
    "No data_path configured for source: fred"
  )
})

test_that("source specs expose provider capabilities and schema contracts", {
  specs <- investdatar::list_source_specs()

  expect_true(all(c("fred", "wbstats", "rss", "ishare", "alphavantage", "quantmod", "okx", "binance") %in% names(specs)))
  expect_s3_class(investdatar::get_source_spec("fred"), "investdatar_source_spec")
  expect_equal(investdatar::get_source_spec("wbstats")$resource_type, "single_series")
  expect_equal(investdatar::get_source_spec("rss")$resource_type, "narrative_feed")
  expect_equal(investdatar::get_source_spec("okx")$resource_type, "market_ohlcv")
  expect_true(isTRUE(investdatar::get_source_spec("okx")$capabilities$pagination))
  expect_false(isTRUE(investdatar::get_source_spec("alphavantage")$capabilities$source_utime))
  expect_equal(investdatar::get_source_spec("binance")$schema$key_cols, c("symbol", "interval", "datetime"))
})

test_that("missing FRED registry files return an empty schema-stable table", {
  registry_path <- file.path(withr::local_tempdir(), "missing_fred_registry.json")
  registry <- investdatar::get_fred_registry(registry_path = registry_path)

  expect_s3_class(registry, "data.table")
  expect_equal(names(registry), c("series_id", "main_group", "title", "start", "end", "freq", "units", "season", "update_time"))
  expect_equal(nrow(registry), 0L)
})
