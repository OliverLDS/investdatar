test_that("config loading normalizes paths and resolves source config", {
  cfg_dir <- withr::local_tempdir()
  cfg_path <- file.path(cfg_dir, "investdatar_config.yaml")
  writeLines(
    c(
      "FRED:",
      "  data_path: ./fred_data",
      "  registry_file: fred_macro_series_registry.json",
      "WorldBank:",
      "  data_path: ./world_bank_data",
      "  registry_file: world_bank_series_registry.json",
      "Treasury:",
      "  data_path: ./treasury_data",
      "CFTC:",
      "  data_path: ./cftc_data",
      "  registry_file: cftc_cot_registry.json",
      "FiscalData:",
      "  data_path: ./fiscal_data",
      "  registry_file: fiscaldata_registry.json",
      "EIA:",
      "  data_path: ./eia_data",
      "  registry_file: eia_series_registry.json",
      "SEC:",
      "  data_path: ./sec_data",
      "  registry_file: sec_company_registry.json",
      "SDMX:",
      "  data_path: ./sdmx_data",
      "  registry_file: sdmx_series_registry.json",
      "RSS:",
      "  data_path: ./rss_data",
      "  registry_file: rss_feed_registry.json",
      "Crypto:",
      "  data_path: ./crypto_data",
      "  derivatives_registry_file: crypto_derivatives_registry.json",
      "iShare:",
      "  data_path: ./ishare_data"
    ),
    cfg_path
  )

  cfg <- investdatar::load_investdatar_config(cfg_path)

  expect_true(dir.exists(dirname(investdatar::get_source_data_path("fred", config = cfg, create = TRUE))))
  expect_match(investdatar::get_source_data_path("fred", config = cfg), "fred_data$")
  expect_match(investdatar::get_source_data_path("wbstats", config = cfg), "world_bank_data$")
  expect_match(investdatar::get_source_data_path("treasury", config = cfg), "treasury_data$")
  expect_match(investdatar::get_source_data_path("cftc", config = cfg), "cftc_data$")
  expect_match(investdatar::get_source_data_path("fiscaldata", config = cfg), "fiscal_data$")
  expect_match(investdatar::get_source_data_path("eia", config = cfg), "eia_data$")
  expect_match(investdatar::get_source_data_path("sec", config = cfg), "sec_data$")
  expect_match(investdatar::get_source_data_path("sdmx", config = cfg), "sdmx_data$")
  expect_match(investdatar::get_source_data_path("rss", config = cfg), "rss_data$")
  expect_match(investdatar::get_source_data_path("ishare", config = cfg), "ishare_data$")
  registry_file <- investdatar::get_source_config("fred", config = cfg)$registry_file
  expect_equal(basename(registry_file), "fred_macro_series_registry.json")
  expect_match(registry_file, "fred_macro_series_registry\\.json$")
  expect_match(investdatar::get_source_config("wbstats", config = cfg)$registry_file, "world_bank_series_registry\\.json$")
  expect_match(investdatar::get_source_config("rss", config = cfg)$registry_file, "rss_feed_registry\\.json$")
  expect_match(investdatar::get_source_config("sec", config = cfg)$registry_file, "sec_company_registry\\.json$")
  expect_match(investdatar::get_source_config("sdmx", config = cfg)$registry_file, "sdmx_series_registry\\.json$")
  expect_match(investdatar::get_source_config("crypto", config = cfg)$derivatives_registry_file, "crypto_derivatives_registry\\.json$")
  expect_identical(investdatar::get_source_config("sec_submissions", config = cfg), investdatar::get_source_config("sec", config = cfg))
  expect_identical(investdatar::get_source_config("sec_companyfacts", config = cfg), investdatar::get_source_config("sec", config = cfg))
  expect_identical(investdatar::get_source_config("crypto_derivatives", config = cfg), investdatar::get_source_config("crypto", config = cfg))
})

test_that("shipped example config is available and loads with normalized relative paths", {
  example_path <- system.file("extdata", "investdatar_config_example.yaml", package = "investdatar")
  expect_true(nzchar(example_path))
  expect_true(file.exists(example_path))

  cfg <- investdatar::load_investdatar_config(example_path)

  expect_match(investdatar::get_source_data_path("fred", config = cfg), "extdata/(\\./)?data/fred$")
  expect_match(investdatar::get_source_data_path("wbstats", config = cfg), "extdata/(\\./)?data/world_bank$")
  expect_match(investdatar::get_source_data_path("treasury", config = cfg), "extdata/(\\./)?data/treasury$")
  expect_match(investdatar::get_source_data_path("cftc", config = cfg), "extdata/(\\./)?data/cftc$")
  expect_match(investdatar::get_source_data_path("fiscaldata", config = cfg), "extdata/(\\./)?data/fiscal_data$")
  expect_match(investdatar::get_source_data_path("eia", config = cfg), "extdata/(\\./)?data/eia$")
  expect_match(investdatar::get_source_data_path("sec", config = cfg), "extdata/(\\./)?data/sec$")
  expect_match(investdatar::get_source_data_path("sdmx", config = cfg), "extdata/(\\./)?data/sdmx$")
  expect_match(investdatar::get_source_data_path("rss", config = cfg), "extdata/(\\./)?data/rss$")
  expect_match(investdatar::get_source_data_path("yahoo", config = cfg), "extdata/(\\./)?data/yahoo_finance$")
  expect_match(
    investdatar::get_source_config("wbstats", config = cfg)$registry_file,
    "extdata/(\\./)?config/world_bank_series_registry\\.json$"
  )
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

  expect_true(all(c("fred", "wbstats", "treasury", "cftc", "fiscaldata", "eia", "sec_submissions", "sec_companyfacts", "sdmx", "rss", "ishare", "alphavantage", "quantmod", "okx", "binance", "crypto_derivatives") %in% names(specs)))
  expect_s3_class(investdatar::get_source_spec("fred"), "investdatar_source_spec")
  expect_equal(investdatar::get_source_spec("wbstats")$resource_type, "single_series")
  expect_equal(investdatar::get_source_spec("wbstats")$functions$sync_registry, "sync_all_wbstats_registry_data")
  expect_equal(investdatar::get_source_spec("fred")$functions$sync_registry, "sync_all_fred_registry_data")
  expect_equal(investdatar::get_source_spec("treasury")$resource_type, "rate_panel")
  expect_equal(investdatar::get_source_spec("cftc")$resource_type, "position_panel")
  expect_equal(investdatar::get_source_spec("cftc")$functions$sync_registry, "sync_all_cftc_cot_registry_data")
  expect_equal(investdatar::get_source_spec("fiscaldata")$resource_type, "dated_table")
  expect_equal(investdatar::get_source_spec("fiscaldata")$functions$sync_registry, "sync_all_fiscaldata_registry_data")
  expect_equal(investdatar::get_source_spec("eia")$resource_type, "single_series")
  expect_equal(investdatar::get_source_spec("eia")$functions$sync_registry, "sync_all_eia_registry_data")
  expect_equal(investdatar::get_source_spec("sec_submissions")$resource_type, "filing_event")
  expect_equal(investdatar::get_source_spec("sec_submissions")$functions$sync_registry, "sync_all_sec_submissions_registry_data")
  expect_equal(investdatar::get_source_spec("sec_companyfacts")$resource_type, "fundamental_fact")
  expect_equal(investdatar::get_source_spec("sec_companyfacts")$schema$key_cols, c("cik", "fact_key"))
  expect_equal(investdatar::get_source_spec("sdmx")$resource_type, "multidimensional_series")
  expect_equal(investdatar::get_source_spec("sdmx")$functions$sync_registry, "sync_all_sdmx_registry_data")
  expect_equal(investdatar::get_source_spec("rss")$resource_type, "narrative_feed")
  expect_equal(investdatar::get_source_spec("okx")$resource_type, "market_ohlcv")
  expect_true(isTRUE(investdatar::get_source_spec("okx")$capabilities$pagination))
  expect_false(isTRUE(investdatar::get_source_spec("alphavantage")$capabilities$source_utime))
  expect_equal(investdatar::get_source_spec("treasury")$schema$key_cols, c("dataset", "date", "series_id"))
  expect_equal(investdatar::get_source_spec("binance")$schema$key_cols, c("symbol", "interval", "datetime"))
  expect_equal(investdatar::get_source_spec("crypto_derivatives")$resource_type, "derivatives_series")
  expect_equal(investdatar::get_source_spec("crypto_derivatives")$functions$sync_registry, "sync_all_crypto_derivatives_registry_data")
  expect_true(all(vapply(specs, function(spec) !is.null(spec$functions$describe), logical(1))))
  declared_functions <- unique(unlist(lapply(specs, function(spec) spec$functions), use.names = FALSE))
  expect_true(all(vapply(
    declared_functions,
    exists,
    logical(1),
    envir = asNamespace("investdatar"),
    inherits = FALSE
  )))
  expect_equal(investdatar::get_source_spec("ishare")$functions$sync_registry, "sync_all_ishare_registry_data")
  expect_equal(investdatar::get_source_spec("ishare")$functions$sync_holdings_registry, "sync_all_ishare_registry_holdings")
})

test_that("source spec validation rejects contradictory capabilities", {
  invalid_spec <- structure(
    list(
      source_id = "bad",
      config_key = "Bad",
      local_path_source = "Bad",
      resource_type = "single_series",
      schema = list(time_col = "date", key_cols = "date", value_cols = "value"),
      capabilities = list(sync = FALSE),
      functions = list(fetch = "get_bad_data", describe = "describe_bad_data")
    ),
    class = "investdatar_source_spec"
  )

  expect_error(
    getFromNamespace(".validate_source_spec", "investdatar")(invalid_spec),
    "fetch-only source spec must not declare local_path_source"
  )
})

test_that("source spec validation enforces declared capabilities", {
  base_spec <- structure(
    list(
      source_id = "bad", config_key = "Bad", local_path_source = "Bad",
      resource_type = "single_series",
      schema = list(time_col = "date", key_cols = "date", value_cols = "value"),
      capabilities = list(sync = TRUE, source_utime = TRUE, gap_detection = TRUE),
      functions = list(fetch = "get_bad_data", sync = "sync_bad_data", describe = "describe_bad_data")
    ),
    class = "investdatar_source_spec"
  )
  validate <- getFromNamespace(".validate_source_spec", "investdatar")
  expect_error(validate(base_spec), "fetch_utime")

  base_spec$functions$fetch_utime <- "get_bad_utime"
  expect_error(validate(base_spec), "detect_gaps")
})

test_that("missing FRED registry files return an empty schema-stable table", {
  registry_path <- file.path(withr::local_tempdir(), "missing_fred_registry.json")
  registry <- investdatar::get_fred_registry(registry_path = registry_path)

  expect_s3_class(registry, "data.table")
  expect_equal(names(registry), c("series_id", "main_group", "title", "start", "end", "freq", "units", "season", "update_time"))
  expect_equal(nrow(registry), 0L)
})
