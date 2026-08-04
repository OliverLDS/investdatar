test_that("fund holdings standardization maps iShares into the neutral contract", {
  source <- data.table::data.table(
    updated_date = as.Date("2026-08-03"), holding_ticker = c("AAPL", "MSFT"),
    holding_name = c("Apple", "Microsoft"), asset_class = "Equity",
    sector = "Technology", weight_pct = c(7.1, 6.4),
    location = "United States", exchange = "NASDAQ"
  )
  out <- investdatar::standardize_fund_holdings(source, "ishare", "IVV")
  expect_named(out, c("provider", "fund_id", "as_of_date", "holding_id", "holding_ticker",
                      "holding_name", "asset_class", "sector", "weight_pct", "shares",
                      "market_value", "currency", "country", "exchange"))
  expect_equal(out$holding_id, c("AAPL", "MSFT"))
  expect_equal(unique(out$country), "United States")
})

test_that("generic fund holdings sync upserts snapshots", {
  local_dir <- withr::local_tempdir()
  first <- data.table::data.table(date = as.Date("2026-08-01"), ticker = "AAA", name = "A", weight = 1)
  second <- data.table::data.table(date = as.Date("2026-08-01"), ticker = "AAA", name = "A", weight = 2)
  investdatar::sync_local_fund_holdings(first, "sample", "FUND", local_path = local_dir)
  investdatar::sync_local_fund_holdings(second, "sample", "FUND", local_path = local_dir)
  out <- investdatar::get_local_fund_holdings("sample", "FUND", local_path = local_dir)
  expect_equal(nrow(out), 1L)
  expect_equal(out$weight_pct, 2)
})
