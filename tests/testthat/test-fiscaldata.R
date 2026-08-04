.fiscal_test_response <- function(data, page = 1L, total_pages = 1L) {
  list(
    data = data,
    meta = list(
      count = nrow(data),
      dataTypes = c(
        record_date = "DATE", debt_held_public_amt = "CURRENCY",
        src_line_nbr = "INTEGER", account_type = "STRING",
        close_today_bal = "CURRENCY0", sub_table_name = "STRING"
      ),
      `total-count` = nrow(data) * total_pages,
      `total-pages` = total_pages
    ),
    links = list()
  )
}

test_that("Fiscal Data retrieval paginates and follows declared source types", {
  calls <- list()
  out <- testthat::with_mocked_bindings(
    .http_get_json = function(url, query = NULL, headers = character(), ...) {
      calls[[length(calls) + 1L]] <<- query
      if (query[["page[number]"]] == 1L) {
        return(.fiscal_test_response(data.frame(
          record_date = c("2026-07-29", "2026-07-30"),
          debt_held_public_amt = c("100.25", "null"),
          src_line_nbr = c("1", "1"), stringsAsFactors = FALSE
        ), total_pages = 2L))
      }
      .fiscal_test_response(data.frame(
        record_date = "2026-07-31", debt_held_public_amt = "102.50",
        src_line_nbr = "1", stringsAsFactors = FALSE
      ), page = 2L, total_pages = 2L)
    },
    investdatar::get_source_data_fiscaldata(
      dataset_id = "debt_to_penny",
      endpoint = "v2/accounting/od/debt_to_penny",
      key_cols = "record_date",
      from = "2026-07-01",
      page_size = 2L
    ),
    .package = "investdatar"
  )

  expect_equal(length(calls), 2L)
  expect_equal(calls[[2L]][["page[number]"]], 2L)
  expect_equal(calls[[1L]]$filter, "record_date:gte:2026-07-01")
  expect_s3_class(out$record_date, "Date")
  expect_type(out$debt_held_public_amt, "double")
  expect_true(is.na(out[record_date == as.Date("2026-07-30"), debt_held_public_amt][[1L]]))
  expect_type(out$src_line_nbr, "integer")
})

test_that("Fiscal Data sync uses overlap and registry key columns", {
  local_dir <- withr::local_tempdir()
  existing <- data.table::data.table(
    source = "treasury_fiscaldata", dataset_id = "operating_cash_balance",
    record_date = as.Date("2026-07-30"), account_type = "Total Operating Balance",
    sub_table_name = "Cash Balance Summary", src_line_nbr = 1L,
    close_today_bal = 900
  )
  saveRDS(existing, file.path(local_dir, "operating_cash_balance.rds"))

  result <- testthat::with_mocked_bindings(
    get_source_data_fiscaldata = function(dataset_id, endpoint, date_col, key_cols,
                                          fields, from, to, page_size) {
      expect_equal(as.Date(from), as.Date("2026-07-16"))
      revised <- data.table::copy(existing)
      revised$close_today_bal <- 950
      revised
    },
    get_source_utime_fiscaldata = function(dataset_id, endpoint, date_col) {
      as.POSIXct("2026-07-30", tz = "UTC")
    },
    investdatar::sync_local_fiscaldata(
      dataset_id = "operating_cash_balance",
      endpoint = "v1/accounting/dts/operating_cash_balance",
      key_cols = c("record_date", "account_type", "sub_table_name", "src_line_nbr"),
      from = "2005-10-03",
      local_path = local_dir
    ),
    .package = "investdatar"
  )

  expect_true(result$updated)
  expect_equal(result$n_new_rows, 0L)
  expect_equal(investdatar::get_local_fiscaldata("operating_cash_balance", local_dir)$close_today_bal, 950)
})

test_that("Fiscal Data registry batch sync logs successes and failures", {
  registry <- data.table::data.table(
    dataset_id = c("debt_to_penny", "operating_cash_balance"),
    endpoint = c("debt", "cash"), date_col = "record_date",
    key_cols = list("record_date", c("record_date", "account_type")),
    fields = list(NULL, NULL), frequency = "daily", start = "2000-01-01", active = TRUE
  )
  local_dir <- withr::local_tempdir()
  summary_dt <- testthat::with_mocked_bindings(
    sync_local_fiscaldata = function(dataset_id, endpoint, date_col, key_cols,
                                     fields, from, local_path, ...) {
      if (dataset_id == "operating_cash_balance") stop("bad endpoint")
      list(updated = TRUE, n_rows = 3L, n_new_rows = 1L)
    },
    investdatar::sync_all_fiscaldata_registry_data(registry = registry, local_path = local_dir),
    .package = "investdatar"
  )

  expect_equal(summary_dt$status, c("success", "error"))
  expect_true(all(summary_dt$source_id == "fiscaldata"))
  expect_equal(summary_dt[dataset_id == "operating_cash_balance", error_message][[1L]], "bad endpoint")
  expect_equal(nrow(investdatar::get_latest_sync_run("fiscaldata", local_dir)$summary), 2L)
})

test_that("shipped Fiscal Data registry declares heterogeneous keys", {
  path <- system.file("extdata", "config", "fiscaldata_registry.json", package = "investdatar")
  registry <- investdatar::get_fiscaldata_registry(path)

  expect_equal(
    registry$dataset_id,
    c("debt_to_penny", "operating_cash_balance", "treasury_auctions", "monthly_receipts",
      "monthly_outlays", "interest_expense", "treasury_securities_outstanding")
  )
  expect_equal(unlist(registry$key_cols[[1L]]), "record_date")
  expect_true("account_type" %in% unlist(registry$key_cols[[2L]]))
})
