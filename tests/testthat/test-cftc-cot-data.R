.cftc_test_rows <- function(ids, dates, market_codes = rep("020601", length(ids))) {
  data.frame(
    id = ids,
    market_and_exchange_names = "UST BOND - CHICAGO BOARD OF TRADE",
    report_date_as_yyyy_mm_dd = paste0(dates, "T00:00:00.000"),
    cftc_contract_market_code = market_codes,
    cftc_market_code = "CBT ",
    open_interest_all = as.character(seq_along(ids) * 100),
    dealer_positions_long_all = as.character(seq_along(ids) * 10),
    asset_mgr_positions_long = as.character(seq_along(ids) * 20),
    lev_money_positions_long = as.character(seq_along(ids) * 30),
    stringsAsFactors = FALSE
  )
}

test_that("CFTC TFF retrieval paginates and standardizes source rows", {
  calls <- list()
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query = NULL, headers = character(), ...) {
      calls[[length(calls) + 1L]] <<- query
      if (query[["$offset"]] == 0L) {
        return(.cftc_test_rows(c("a", "b"), c("2026-07-14", "2026-07-21")))
      }
      .cftc_test_rows("c", "2026-07-28")
    },
    investdatar::get_source_data_cftc_cot(
      report_variant = "futures_only",
      report_id = "rates_tff",
      market_codes = "020601",
      from = "2026-07-01",
      page_size = 2L
    ),
    .package = "investdatar"
  )

  expect_equal(length(calls), 2L)
  expect_equal(calls[[2L]][["$offset"]], 2L)
  expect_match(calls[[1L]][["$where"]], "cftc_contract_market_code in \\('020601'\\)")
  expect_s3_class(out, "data.table")
  expect_equal(nrow(out), 3L)
  expect_equal(out$report_id, rep("rates_tff", 3L))
  expect_equal(out$report_variant, rep("futures_only", 3L))
  expect_s3_class(out$report_date, "Date")
  expect_type(out$open_interest_all, "double")
  expect_equal(unique(out$cftc_market_code), "CBT")
})

test_that("CFTC source update time uses Socrata metadata", {
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query = NULL, headers = character(), ...) {
      list(rowsUpdatedAt = 1785267053)
    },
    investdatar::get_source_utime_cftc_cot("combined"),
    .package = "investdatar"
  )

  expect_s3_class(out, "POSIXct")
  expect_equal(as.numeric(out), 1785267053)
})

test_that("CFTC local sync overlaps recent reports and upserts revisions", {
  local_dir <- withr::local_tempdir()
  standardize <- getFromNamespace(".standardize_cftc_cot", "investdatar")
  existing <- standardize(
    .cftc_test_rows("a", "2026-07-28"),
    report_id = "tff_futures_only",
    report_variant = "futures_only",
    dataset_id = "gpe5-46if"
  )
  saveRDS(existing, file.path(local_dir, "tff_futures_only.rds"))

  res <- testthat::with_mocked_bindings(
    get_source_data_cftc_cot = function(report_variant, report_type, report_id, dataset_id,
                                        market_codes, from, to, page_size) {
      expect_equal(as.Date(from), as.Date("2026-07-14"))
      revised <- .cftc_test_rows("a", "2026-07-28")
      revised$open_interest_all <- "999"
      standardize(revised, report_id, report_variant, dataset_id)
    },
    get_source_utime_cftc_cot = function(report_variant, report_type, dataset_id) {
      as.POSIXct("2026-07-31 19:30:00", tz = "UTC")
    },
    investdatar::sync_local_cftc_cot(
      report_variant = "futures_only",
      from = "2006-06-13",
      local_path = local_dir
    ),
    .package = "investdatar"
  )

  local_dt <- investdatar::get_local_cftc_cot("tff_futures_only", local_path = local_dir)
  expect_true(res$updated)
  expect_equal(res$n_new_rows, 0L)
  expect_equal(local_dt$open_interest_all, 999)
})

test_that("CFTC registry batch sync returns standard summaries and run logs", {
  registry <- data.table::data.table(
    report_id = c("tff_futures_only", "tff_combined"),
    report_variant = c("futures_only", "combined"),
    dataset_id = c("gpe5-46if", "yw9f-hn96"),
    market_codes = c(NA_character_, NA_character_),
    start = c("2006-06-13", "2006-06-13"),
    active = TRUE
  )
  local_dir <- withr::local_tempdir()

  summary_dt <- testthat::with_mocked_bindings(
    sync_local_cftc_cot = function(report_variant, report_id, dataset_id,
                                   market_codes, from, local_path, ...) {
      if (report_variant == "combined") stop("source unavailable")
      list(updated = TRUE, n_rows = 10L, n_new_rows = 2L)
    },
    investdatar::sync_all_cftc_cot_registry_data(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(summary_dt$status, c("success", "error"))
  expect_true(all(summary_dt$source_id == "cftc"))
  expect_equal(summary_dt[report_variant == "combined", error_message][[1]], "source unavailable")
  expect_equal(summary_dt[report_variant == "combined", error_class][[1]], "simpleError")
  run <- investdatar::get_latest_sync_run("cftc", local_path = local_dir)
  expect_equal(nrow(run$summary), 2L)
})

test_that("shipped CFTC registry pins official TFF, disaggregated, and legacy datasets", {
  registry_path <- system.file("extdata", "config", "cftc_cot_registry.json", package = "investdatar")
  registry <- investdatar::get_cftc_cot_registry(registry_path)

  expect_equal(registry$report_type, rep(c("tff", "disaggregated", "legacy"), each = 2L))
  expect_equal(
    registry$dataset_id,
    c("gpe5-46if", "yw9f-hn96", "72hh-3qpy", "kh3c-gbw2", "6dca-aqww", "jun7-fc8e")
  )
})
