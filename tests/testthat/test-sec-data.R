.sec_test_config <- function() {
  list(
    user_agent = "Test User test@example.com",
    data_url = "https://data.sec.gov",
    website_url = "https://www.sec.gov",
    request_delay = 0
  )
}

.sec_submission_columns <- function(accessions, dates, forms) {
  n <- length(accessions)
  list(
    accessionNumber = accessions,
    filingDate = dates,
    reportDate = dates,
    acceptanceDateTime = paste0(gsub("-", "", dates), "120000"),
    act = rep("34", n), form = forms, fileNumber = rep("001", n),
    filmNumber = rep("1", n), items = rep("", n), size = rep("100", n),
    isXBRL = rep(1L, n), isInlineXBRL = rep(1L, n),
    primaryDocument = rep("filing.htm", n),
    primaryDocDescription = rep("Filing", n)
  )
}

.sec_companyfacts_fixture <- function() {
  list(
    cik = 320193,
    entityName = "Apple Inc.",
    tickers = list("AAPL"),
    facts = list(
      `us-gaap` = list(
        Assets = list(
          label = "Assets",
          description = "Total assets",
          units = list(
            USD = data.frame(
              end = c("2025-09-27", "2025-09-27"),
              val = c(100, 101),
              accn = c("0001", "0002"),
              fy = c(2025, 2025), fp = c("FY", "FY"),
              form = c("10-K", "10-K/A"), filed = c("2025-10-31", "2025-11-05"),
              frame = c("CY2025Q3I", "CY2025Q3I"), stringsAsFactors = FALSE
            )
          )
        ),
        Revenues = list(
          label = "Revenue", description = "Revenue",
          units = list(USD = data.frame(
            start = "2025-06-29", end = "2025-09-27", val = 50,
            accn = "0001", fy = 2025, fp = "Q4", form = "10-K",
            filed = "2025-10-31", frame = "CY2025Q3",
            stringsAsFactors = FALSE
          ))
        )
      )
    )
  )
}

test_that("SEC ticker mapping resolves CIK and writes registry entries", {
  config <- .sec_test_config()
  mappings <- testthat::with_mocked_bindings(
    .sec_get_json = function(url, config = NULL) list(
      fields = c("cik", "name", "ticker", "exchange"),
      data = list(
        list(320193, "Apple Inc.", "AAPL", "Nasdaq"),
        list(789019, "Microsoft Corp", "MSFT", "Nasdaq")
      )
    ),
    investdatar::get_sec_company_tickers(config = config),
    .package = "investdatar"
  )
  expect_equal(investdatar::resolve_sec_cik("aapl", mappings = mappings), "320193")

  registry_path <- file.path(withr::local_tempdir(), "sec.json")
  row <- testthat::with_mocked_bindings(
    get_sec_company_tickers = function(config = NULL) mappings,
    investdatar::add_sec_registry_company(
      "AAPL", forms = c("10-K", "10-Q"), concepts = "Assets",
      registry_path = registry_path, config = config
    ),
    .package = "investdatar"
  )
  expect_equal(row$cik, "320193")
  expect_equal(unlist(row$forms[[1L]]), c("10-K", "10-Q"))
  expect_equal(nrow(investdatar::get_sec_registry(registry_path)), 1L)
})

test_that("SEC submissions stitch recent and historical files then filter forms", {
  config <- .sec_test_config()
  calls <- character()
  out <- testthat::with_mocked_bindings(
    .sec_get_json = function(url, config = NULL) {
      calls <<- c(calls, url)
      if (grepl("/submissions/CIK[0-9]+\\.json$", url)) {
        return(list(
          name = "Apple Inc.", tickers = list("AAPL"),
          filings = list(
            recent = .sec_submission_columns(c("0002", "0003"), c("2025-11-05", "2026-01-30"), c("10-K/A", "10-Q")),
            files = data.frame(name = "CIK0000320193-submissions-001.json", stringsAsFactors = FALSE)
          )
        ))
      }
      .sec_submission_columns("0001", "2025-10-31", "10-K")
    },
    investdatar::get_source_data_sec_submissions(
      320193, forms = c("10-K", "10-Q"), include_history = TRUE, config = config
    ),
    .package = "investdatar"
  )

  expect_equal(length(calls), 2L)
  expect_equal(out$accession_number, c("0001", "0003"))
  expect_s3_class(out$filing_date, "Date")
  expect_s3_class(out$acceptance_datetime, "POSIXct")
  expect_true(all(out$cik == "320193"))
})

test_that("SEC submission sync uses recent-only overlap after first local sync", {
  local_dir <- withr::local_tempdir()
  existing <- getFromNamespace(".standardize_sec_submissions", "investdatar")(
    .sec_submission_columns("0003", "2026-01-30", "10-Q"),
    cik = 320193, ticker = "AAPL", company_name = "Apple Inc."
  )
  saveRDS(existing, file.path(local_dir, "CIK0000320193.rds"))
  result <- testthat::with_mocked_bindings(
    get_source_data_sec_submissions = function(cik, ticker, company_name, forms,
                                               from, to, include_history, config) {
      expect_equal(as.Date(from), as.Date("2026-01-16"))
      expect_false(include_history)
      revised <- data.table::copy(existing)
      revised$size <- 150
      revised
    },
    investdatar::sync_local_sec_submissions(
      320193, ticker = "AAPL", config = .sec_test_config(),
      from = "2000-01-01", local_path = local_dir
    ),
    .package = "investdatar"
  )
  expect_true(result$updated)
  expect_equal(investdatar::get_local_sec_submissions(320193, local_dir)$size, 150)
})

test_that("SEC Company Facts preserve concepts, units, contexts and amendments", {
  out <- testthat::with_mocked_bindings(
    .sec_get_json = function(url, config = NULL) .sec_companyfacts_fixture(),
    investdatar::get_source_data_sec_companyfacts(
      320193, concepts = c("Assets", "us-gaap:Revenues"),
      config = .sec_test_config()
    ),
    .package = "investdatar"
  )

  expect_equal(nrow(out), 3L)
  expect_true(all(c("Assets", "Revenues") %in% out$concept))
  expect_equal(data.table::uniqueN(out$fact_key), 3L)
  expect_equal(out[accession_number == "0002", value][[1L]], 101)
  expect_s3_class(out$filed, "Date")
  expect_true(is.na(out[concept == "Assets", start][[1L]]))
})

test_that("SEC Company Facts sync overlaps filed dates and upserts facts", {
  local_dir <- withr::local_tempdir()
  existing <- getFromNamespace(".standardize_sec_companyfacts", "investdatar")(
    .sec_companyfacts_fixture(), cik = 320193, concepts = "Assets"
  )
  saveRDS(existing, file.path(local_dir, "CIK0000320193.rds"))
  result <- testthat::with_mocked_bindings(
    get_source_data_sec_companyfacts = function(cik, ticker, company_name, concepts,
                                                forms, from, to, config) {
      expect_equal(as.Date(from), as.Date("2025-10-05"))
      revised <- data.table::copy(existing)
      revised[accession_number == "0002", value := 102]
      revised
    },
    investdatar::sync_local_sec_companyfacts(
      320193, config = .sec_test_config(), from = "2009-01-01", local_path = local_dir
    ),
    .package = "investdatar"
  )
  expect_true(result$updated)
  expect_equal(investdatar::get_local_sec_companyfacts(320193, local_dir)[accession_number == "0002", value][[1L]], 102)
})

test_that("SEC batch workflows keep separate run logs", {
  registry <- data.table::data.table(
    ticker = c("AAPL", "BAD"), cik = c("320193", "1"),
    company_name = c("Apple", "Bad"), forms = list("10-K", "10-K"),
    concepts = list("Assets", "Assets"), active = TRUE
  )
  sec_dir <- withr::local_tempdir()
  submissions_dir <- file.path(sec_dir, "submissions")
  facts_dir <- file.path(sec_dir, "companyfacts")
  dir.create(submissions_dir)
  dir.create(facts_dir)

  submissions <- testthat::with_mocked_bindings(
    sync_local_sec_submissions = function(cik, ticker, company_name, forms, config, local_path, ...) {
      if (ticker == "BAD") stop("bad submissions")
      list(updated = TRUE, n_rows = 1L, n_new_rows = 1L)
    },
    investdatar::sync_all_sec_submissions_registry_data(registry, config = .sec_test_config(), local_path = submissions_dir),
    .package = "investdatar"
  )
  facts <- testthat::with_mocked_bindings(
    sync_local_sec_companyfacts = function(cik, ticker, company_name, concepts, forms, config, local_path, ...) {
      if (ticker == "BAD") stop("bad facts")
      list(updated = TRUE, n_rows = 2L, n_new_rows = 2L)
    },
    investdatar::sync_all_sec_companyfacts_registry_data(registry, config = .sec_test_config(), local_path = facts_dir),
    .package = "investdatar"
  )

  expect_equal(submissions$status, c("success", "error"))
  expect_equal(facts$status, c("success", "error"))
  expect_equal(investdatar::get_latest_sync_run("sec_submissions", submissions_dir)$source_id, "sec_submissions")
  expect_equal(investdatar::get_latest_sync_run("sec_companyfacts", facts_dir)$source_id, "sec_companyfacts")
})

test_that("SEC API requires an identifiable user agent", {
  expect_error(
    investdatar::get_sec_company_tickers(config = list(user_agent = "", data_url = "x", website_url = "x")),
    "SEC user agent is missing"
  )
})

test_that("SEC Frames normalize cross-company observations", {
  fixture <- list(data = data.frame(
    accn = "0000320193-25-000079", cik = 320193, entityName = "Apple Inc.",
    loc = "US-CA", start = "2025-01-01", end = "2025-03-31",
    val = 100, fy = 2025, fp = "Q2", form = "10-Q", filed = "2025-05-02"
  ))
  dt <- testthat::with_mocked_bindings(
    .sec_get_json = function(url, config = NULL) fixture,
    investdatar::get_source_data_sec_frame(
      "us-gaap", "Assets", "USD", "CY2025Q1I", config = .sec_test_config()
    ),
    .package = "investdatar"
  )
  expect_equal(dt$concept, "Assets")
  expect_equal(dt$cik, "320193")
  expect_s3_class(dt$end, "Date")
  expect_equal(dt$value, 100)
})

test_that("selected SEC filing documents are cached without repeated downloads", {
  local_dir <- withr::local_tempdir()
  raw_calls <- 0L
  result <- testthat::with_mocked_bindings(
    .sec_get_raw = function(url, config = NULL, accept = "*/*") {
      raw_calls <<- raw_calls + 1L
      charToRaw("<html>filing</html>")
    },
    {
      first <- investdatar::sync_local_sec_filing_document(
        320193, "0000320193-25-000079", "aapl-20250329.htm",
        local_path = local_dir, config = .sec_test_config()
      )
      second <- investdatar::sync_local_sec_filing_document(
        320193, "0000320193-25-000079", "aapl-20250329.htm",
        local_path = local_dir, config = .sec_test_config()
      )
      list(first = first, second = second)
    },
    .package = "investdatar"
  )
  expect_true(result$first$updated)
  expect_false(result$second$updated)
  expect_equal(raw_calls, 1L)
  expect_true(file.exists(result$first$file_path))
})
