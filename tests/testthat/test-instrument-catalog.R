.instrument_catalog_test_path <- function() {
  path <- system.file("extdata", "instrument_catalog.json", package = "investdatar")
  if (!nzchar(path)) {
    path <- testthat::test_path("..", "..", "inst", "extdata", "instrument_catalog.json")
  }
  path
}

.yahoo_registry_test_path <- function() {
  path <- system.file("extdata", "config", "YahooFinance_ticker_registry.json", package = "investdatar")
  if (!nzchar(path)) {
    path <- testthat::test_path("..", "..", "inst", "extdata", "config", "YahooFinance_ticker_registry.json")
  }
  path
}

test_that("instrument catalog has a valid provider-neutral schema", {
  validation <- investdatar::validate_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )

  expect_true(validation$valid)
  expect_equal(nrow(validation$errors), 0L)
  expect_s3_class(validation$catalog, "data.table")
  expect_true(all(c(
    "schema_version", "instrument_id", "canonical_symbol", "display_name",
    "asset_class", "instrument_type", "quote_currency", "market_calendar",
    "provider_identifiers", "primary_source", "fallback_sources",
    "price_frequency", "supported_intervals", "active"
  ) %in% names(validation$catalog)))
  expect_true(all(validation$catalog$schema_version %in% c("1.0.0", "1.1.0")))
  expect_true(all(validation$catalog$asset_class %in% c(
    "equity", "fixed_income", "commodity", "foreign_exchange", "cryptocurrency"
  )))
  expect_true(all(validation$catalog$market_calendar %in% c(
    "US_EQUITY", "XNYS", "XSHG", "XHKG", "XJPX", "EU_EQUITY",
    "US_TREASURY", "CME_FUTURES", "ICE_FUTURES", "FX_24_5", "CRYPTO_24_7"
  )))
  expect_true(all(validation$catalog$price_frequency %in% c("1d", "4h")))
})

test_that("rates, indices, and continuous futures have explicit 1.1 metadata", {
  catalog <- investdatar::get_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path(), schema_version = "1.1.0"
  )
  expected <- c("^IRX", "ZT=F", "^TNX", "^TYX", "^GSPC", "^IXIC", "^RUT", "^VIX",
                "CL=F", "BZ=F", "GC=F", "SI=F", "HG=F", "^STOXX50E", "^N225", "^HSI")
  rows <- catalog[vapply(provider_identifiers, function(x) isTRUE(x$yahoo %in% expected), logical(1))]
  expect_equal(nrow(rows), length(expected))
  expect_true(all(rows$schema_version == "1.1.0"))
  expect_true(all(rows$quote_unit %in% c("index_points", "yield_percent", "contract_price")))
  expect_true(all(vapply(rows$audit_profile, function(x) isTRUE(is.list(x$volume)) &&
    is.logical(x$volume$meaningful) && length(x$completion_rule) == 1L, logical(1))))
  futures <- rows[instrument_type == "future"]
  expect_setequal(futures$market_calendar, c("CME_FUTURES", "ICE_FUTURES"))
  expect_true(all(vapply(futures$continuous_contract, function(x) {
    identical(x$roll_treatment, "provider_defined") &&
      identical(x$volume_semantics, "provider_defined_continuous_series") &&
      identical(x$deliverable_contract, FALSE)
  }, logical(1))))
  expect_true(all(is.na(rows$primary_listing_mic)))
})

test_that("instrument catalog has unique stable identifiers and symbols", {
  catalog <- investdatar::get_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )

  expect_equal(anyDuplicated(catalog$instrument_id), 0L)
  expect_equal(anyDuplicated(catalog$canonical_symbol), 0L)
  expect_equal(catalog[canonical_symbol == "SPY", instrument_id][[1L]], "etf.us.spy")
  expect_equal(catalog[canonical_symbol == "BTC/USD", instrument_id][[1L]], "crypto.bitcoin-usd")
})

test_that("instrument catalog includes the initial eight instruments", {
  catalog <- investdatar::get_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )
  initial_symbols <- c("SPY", "TLT", "GLD", "EUR/USD", "BTC/USD", "EEM", "HYG", "USO")

  expect_true(all(initial_symbols %in% catalog$canonical_symbol))
  expect_true(all(c("USD/CNH", "CSI300") %in% catalog$canonical_symbol))
  expect_equal(catalog[canonical_symbol == "SPY", provider_identifiers][[1L]]$yahoo, "SPY")
})

test_that("OKX perpetual catalog contains the six maintained 4H instruments", {
  catalog <- investdatar::get_okx_perpetual_catalog(
    catalog_path = .instrument_catalog_test_path()
  )
  expected <- c(
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP",
    "BNB-USDT-SWAP", "XRP-USDT-SWAP", "DOGE-USDT-SWAP"
  )

  expect_equal(nrow(catalog), 6L)
  expect_setequal(vapply(catalog$provider_identifiers, `[[`, character(1), "okx"), expected)
  expect_equal(anyDuplicated(vapply(catalog$provider_identifiers, `[[`, character(1), "okx")), 0L)
  expect_true(all(catalog$asset_class == "cryptocurrency"))
  expect_true(all(catalog$instrument_type == "perpetual_swap"))
  expect_true(all(catalog$quote_currency == "USDT"))
  expect_true(all(catalog$market_calendar == "CRYPTO_24_7"))
  expect_true(all(catalog$price_frequency == "4h"))
  expect_true(all(vapply(catalog$supported_intervals, identical, logical(1), "4h")))
  expect_true(all(catalog$contract_size > 0))
  expect_true(all(catalog$quantity_step > 0))
  expect_true(all(catalog$quantity_unit == "contracts"))
  expect_true(all(catalog$settlement_currency == "USDT"))
  expect_true(all(catalog$contract_structure == "linear_usdt_margined_perpetual"))
})

test_that("OKX perpetual catalog validates active completed-cache mappings", {
  local_dir <- withr::local_tempdir()
  catalog <- investdatar::get_okx_perpetual_catalog(
    catalog_path = .instrument_catalog_test_path()
  )
  for (inst_id in vapply(catalog$provider_identifiers, `[[`, character(1), "okx")) {
    local <- data.table::data.table(
      source = "okx", symbol = inst_id, interval = "4H",
      datetime = as.POSIXct("2026-08-31 20:00:00", tz = "UTC"),
      date = as.Date("2026-08-31"), open = 1, high = 2, low = 0.5,
      close = 1.5, volume = 10
    )
    saveRDS(local, file.path(local_dir, sprintf("%s_4H.rds", inst_id)))
  }

  validation <- investdatar::validate_okx_perpetual_catalog(
    catalog_path = .instrument_catalog_test_path(),
    local_path = local_dir,
    as_of = as.POSIXct("2026-09-01 00:00:00", tz = "UTC")
  )
  expect_true(validation$valid)
  expect_equal(nrow(validation$cache_status), 6L)
  expect_true(all(validation$cache_status$cache_usable))

  unlink(file.path(local_dir, "BTC-USDT-SWAP_4H.rds"))
  missing_cache <- investdatar::validate_okx_perpetual_catalog(
    catalog_path = .instrument_catalog_test_path(),
    local_path = local_dir,
    as_of = as.POSIXct("2026-09-01 00:00:00", tz = "UTC")
  )
  expect_false(missing_cache$valid)
  expect_true(any(missing_cache$errors$check == "okx_cache"))
})

test_that("daily and 4H catalog intervals remain separate", {
  catalog <- investdatar::get_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )
  daily <- catalog[price_frequency == "1d"]
  perpetual <- investdatar::get_okx_perpetual_catalog(.instrument_catalog_test_path())

  expect_true(all(vapply(daily$supported_intervals, identical, logical(1), "1d")))
  expect_true(all(vapply(perpetual$primary_source, function(x) identical(x$provider, "okx"), logical(1))))
  expect_false(any(vapply(perpetual$provider_identifiers, function(x) "yahoo" %in% names(x), logical(1))))
})

test_that("perpetual contract metadata and interval requirements are enforced", {
  records <- jsonlite::read_json(.instrument_catalog_test_path(), simplifyVector = FALSE)
  perpetual <- which(vapply(records, function(x) identical(x$instrument_type, "perpetual_swap"), logical(1)))[[1L]]
  records[[perpetual]]$contract_structure <- NULL
  records[[perpetual]]$price_frequency <- "1d"
  records[[perpetual]]$supported_intervals <- list("1d")
  bad_path <- file.path(withr::local_tempdir(), "bad-perpetual-contract.json")
  jsonlite::write_json(records, bad_path, auto_unbox = TRUE, pretty = TRUE)

  validation <- investdatar::validate_instrument_catalog(bad_path, .yahoo_registry_test_path())
  expect_false(validation$valid)
  expect_true(any(validation$errors$check == "contract_structure"))
  expect_true(any(validation$errors$check == "perpetual_interval"))
})

test_that("instrument catalog Yahoo mappings and fallbacks match the sync registry", {
  catalog_path <- .instrument_catalog_test_path()
  registry_path <- .yahoo_registry_test_path()
  catalog <- investdatar::get_instrument_catalog(catalog_path, registry_path)
  cnh_fallback <- catalog[canonical_symbol == "USD/CNH", fallback_sources][[1L]][[1L]]
  csi_fallback <- catalog[canonical_symbol == "CSI300", fallback_sources][[1L]][[1L]]

  expect_equal(cnh_fallback$provider, "eastmoney")
  expect_equal(cnh_fallback$symbol, "133.USDCNH")
  expect_equal(csi_fallback$provider, "eastmoney")
  expect_equal(csi_fallback$symbol, "1.000300")

  records <- jsonlite::read_json(catalog_path, simplifyVector = FALSE)
  records[[1L]]$provider_identifiers$yahoo <- "MISSING"
  records[[1L]]$primary_source$symbol <- "MISSING"
  bad_mapping_path <- file.path(withr::local_tempdir(), "bad-mapping.json")
  jsonlite::write_json(records, bad_mapping_path, auto_unbox = TRUE, pretty = TRUE)
  bad_mapping <- investdatar::validate_instrument_catalog(bad_mapping_path, registry_path)
  expect_false(bad_mapping$valid)
  expect_true(any(bad_mapping$errors$check == "yahoo_mapping"))

  records <- jsonlite::read_json(catalog_path, simplifyVector = FALSE)
  records[[9L]]$fallback_sources[[1L]]$symbol <- "incorrect"
  bad_fallback_path <- file.path(withr::local_tempdir(), "bad-fallback.json")
  jsonlite::write_json(records, bad_fallback_path, auto_unbox = TRUE, pretty = TRUE)
  bad_fallback <- investdatar::validate_instrument_catalog(bad_fallback_path, registry_path)
  expect_false(bad_fallback$valid)
  expect_true(any(bad_fallback$errors$check == "fallback_consistency"))

  records <- jsonlite::read_json(catalog_path, simplifyVector = FALSE)
  records[[9L]]$fallback_sources[[1L]]$extra <- "not allowed"
  bad_shape_path <- file.path(withr::local_tempdir(), "bad-fallback-shape.json")
  jsonlite::write_json(records, bad_shape_path, auto_unbox = TRUE, pretty = TRUE)
  bad_shape <- investdatar::validate_instrument_catalog(bad_shape_path, registry_path)
  expect_false(bad_shape$valid)
  expect_match(bad_shape$errors[check == "fallback_sources", message][[1L]], "exactly")

  records <- jsonlite::read_json(catalog_path, simplifyVector = FALSE)
  records[[9L]]$fallback_sources <- list(
    list(provider = "z_source", symbol = "Z"),
    list(provider = "a_source", symbol = "A"),
    list(provider = "a_source", symbol = "A")
  )
  unordered_path <- file.path(withr::local_tempdir(), "unordered-fallbacks.json")
  jsonlite::write_json(records, unordered_path, auto_unbox = TRUE, pretty = TRUE)
  unordered <- investdatar::validate_instrument_catalog(unordered_path, registry_path)
  expect_false(unordered$valid)
  fallback_messages <- unordered$errors[check == "fallback_sources", message]
  expect_true(any(grepl("unique", fallback_messages)))
  expect_true(any(grepl("ordered", fallback_messages)))
})

test_that("instrument catalog reports null enum values as validation errors", {
  records <- jsonlite::read_json(.instrument_catalog_test_path(), simplifyVector = FALSE)
  records[[1L]]$asset_class <- NA_character_
  bad_path <- file.path(withr::local_tempdir(), "null-asset-class.json")
  jsonlite::write_json(records, bad_path, auto_unbox = TRUE, pretty = TRUE, na = "null")

  validation <- investdatar::validate_instrument_catalog(bad_path, .yahoo_registry_test_path())
  expect_false(validation$valid)
  expect_true(any(validation$errors$check == "nullability"))
  expect_true(any(validation$errors$check == "asset_class"))
})

test_that("schema 1.1 normalizes legacy calendars and preserves provider routing", {
  legacy <- investdatar::get_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )
  refined <- investdatar::normalize_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )

  expect_equal(nrow(refined), nrow(legacy))
  expect_true(all(refined$schema_version == "1.1.0"))
  expect_equal(refined[canonical_symbol == "SPY", market_calendar][[1L]], "US_EQUITY")
  expect_equal(refined[canonical_symbol == "SPY", provider_identifiers][[1L]]$yahoo, "SPY")
  expect_equal(refined[canonical_symbol == "EUR/USD", quote_unit][[1L]], "currency_price")
  expect_equal(refined[canonical_symbol == "BTC/USD", market_calendar][[1L]], "CRYPTO_24_7")
})

test_that("Batch 1 and Batch 2 catalog coverage has explicit metadata", {
  catalog <- investdatar::normalize_instrument_catalog(
    .instrument_catalog_test_path(), .yahoo_registry_test_path()
  )
  expected <- c(
    "XLK", "XLF", "XLE", "XLI", "XLY", "XLP", "XLV", "XLU", "XLC", "XLB", "XLRE",
    "SOXX", "LQD", "QQQ", "IVV", "IEFA", "IWM", "EFA", "IEF", "SHY", "AGG", "SLV",
    "IAU", "IBIT", "DBC", "UUP", "FXE", "AAPL", "GOOG", "BMNR", "MSTR", "COIN",
    "TSLA", "PLTR", "ORCL", "AVGO", "ASML", "AMZN", "MSFT", "AMD", "IBM", "NVDA",
    "USD/JPY", "GBP/USD", "ETH/USD"
  )
  covered <- catalog[canonical_symbol %in% expected]
  expect_equal(nrow(covered), length(expected))
  expect_true(all(covered$market_calendar %in% c("US_EQUITY", "FX_24_5", "CRYPTO_24_7")))
  expect_true(all(!is.na(covered$quote_unit)))
  expect_true(all(vapply(covered$audit_profile, function(x) {
    is.list(x) && is.list(x$price_tolerance) && !is.null(x$completion_rule)
  }, logical(1))))
  expect_true(all(vapply(covered$provider_identifiers, function(x) !is.null(x$yahoo), logical(1))))
})
