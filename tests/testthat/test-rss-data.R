test_that("GDPNow RSS parser standardizes feed items and parsed fields", {
  feed_text <- paste(
    "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>",
    "<rss version=\"2.0\"><channel>",
    "<title>GDPNow</title>",
    "<item>",
    "<title>Fourth-Quarter GDP Growth Estimate Unchanged</title>",
    "<link>https://www.atlantafed.org/cqer/research/gdpnow?item=abc</link>",
    "<guid>abc</guid>",
    "<pubDate>Monday, 2 Feb 2026 10:44:0 EST</pubDate>",
    "<description>On February 2, the GDPNow model estimate for real GDP growth in the fourth quarter of 2025 is 4.2 percent, unchanged from January 29 after rounding. View GDPNow for more details.</description>",
    "</item>",
    "<item>",
    "<title>Initial Fourth-Quarter GDP Growth Estimate 3.0 Percent</title>",
    "<link>https://www.atlantafed.org/cqer/research/gdpnow?item=def</link>",
    "<guid>def</guid>",
    "<pubDate>Tuesday, 23 Dec 2025 10:28:0 EST</pubDate>",
    "<description>On December 23, the initial GDPNow model estimate for real GDP growth in the fourth quarter of 2025 is 3.0 percent. View GDPNow for more details.</description>",
    "</item>",
    "</channel></rss>"
  )

  dt <- investdatar:::.parse_rss_items(
    feed_text = feed_text,
    feed_id = "atlfed_gdpnow",
    parser = "gdpnow"
  )

  expect_s3_class(dt, "data.table")
  expect_equal(nrow(dt), 2L)
  expect_equal(dt[guid == "abc", feed_id][[1]], "atlfed_gdpnow")
  expect_equal(dt[guid == "abc", period_label][[1]], "2025Q4")
  expect_equal(dt[guid == "abc", estimate_value][[1]], 4.2)
  expect_equal(dt[guid == "def", change_direction][[1]], "initial")
  expect_true(all(c("guid", "published_at", "summary", "narrative_type") %in% names(dt)))
})

test_that("RSS parser preserves numeric RFC-822 timezone offsets", {
  feed_text <- paste(
    "<rss><channel><item>",
    "<title>SEC item</title>",
    "<link>https://example.test/sec-item</link>",
    "<guid>sec-guid</guid>",
    "<pubDate>Thu, 03 Sep 2026 16:30:00 -0400</pubDate>",
    "<description>Release</description>",
    "</item></channel></rss>"
  )
  parsed <- investdatar:::.parse_rss_items(feed_text, "sec_press_releases")
  expect_equal(parsed$published_at, as.POSIXct("2026-09-03 20:30:00", tz = "UTC"))
  expect_equal(parsed$published_date, as.Date("2026-09-03"))
})

test_that("RSS parser removes a UTF-8 BOM before XML parsing", {
  feed_text <- paste(
    "<rss><channel><item><title>Fed item</title>",
    "<link><![CDATA[https://example.test/fed-item]]></link>",
    "<guid><![CDATA[fed-guid]]></guid>",
    "<pubDate><![CDATA[Fri, 4 Sep 2026 15:00:00 GMT]]></pubDate>",
    "<description><![CDATA[Release]]></description></item></channel></rss>"
  )
  feed_text <- rawToChar(c(as.raw(c(0xef, 0xbb, 0xbf)), charToRaw(feed_text)))
  parsed <- investdatar:::.parse_rss_items(feed_text, "fed_press_all")
  expect_equal(parsed$guid, "fed-guid")
  expect_equal(parsed$link, "https://example.test/fed-item")
  expect_equal(parsed$published_at, as.POSIXct("2026-09-04 15:00:00", tz = "UTC"))
})

test_that("RSS HTML fallback preserves CDATA fields", {
  feed_text <- "<rss><channel><item><guid><![CDATA[id]]></guid><pubDate><![CDATA[Fri, 4 Sep 2026 15:00:00 GMT]]></pubDate><title>Title</title></item></channel></rss>"
  parsed <- investdatar:::.parse_rss_items(feed_text, "fed_press_all")
  expect_equal(parsed$guid, "id")
  expect_equal(parsed$published_date, as.Date("2026-09-04"))
})

test_that("sync_local_rss_data writes local feed data and describe_rss_data reads it", {
  local_dir <- withr::local_tempdir()

  mocked_dt <- data.table::data.table(
    feed_id = "atlfed_gdpnow",
    source = "rss",
    guid = c("a", "b"),
    published_at = as.POSIXct(c("2026-02-02 15:44:00", "2026-02-03 15:44:00"), tz = "UTC"),
    published_date = as.Date(c("2026-02-02", "2026-02-03")),
    title = c("Headline A", "Headline B"),
    summary = c("Summary A", "Summary B"),
    link = c("https://example.com/a", "https://example.com/b"),
    author = c(NA_character_, NA_character_),
    category = c(NA_character_, NA_character_),
    narrative_type = c("gdpnow_update", "gdpnow_update"),
    period_text = c("fourth quarter of 2025", "fourth quarter of 2025"),
    period_label = c("2025Q4", "2025Q4"),
    estimate_value = c(4.2, 4.3),
    estimate_unit = c("percent", "percent"),
    change_direction = c("unchanged", "increased")
  )

  old_get_source_data_rss <- get("get_source_data_rss", envir = asNamespace("investdatar"))
  assignInNamespace("get_source_data_rss", function(feed_id, url, parser = c("plain", "gdpnow")) mocked_dt, ns = "investdatar")
  on.exit(assignInNamespace("get_source_data_rss", old_get_source_data_rss, ns = "investdatar"), add = TRUE)

  res <- investdatar::sync_local_rss_data(
    feed_id = "atlfed_gdpnow",
    url = "https://www.atlantafed.org/rss/GDPNow",
    parser = "gdpnow",
    local_path = local_dir
  )

  local_dt <- investdatar::get_local_rss_data("atlfed_gdpnow", local_path = local_dir)
  txt <- investdatar::describe_rss_data("atlfed_gdpnow", local_path = local_dir)

  expect_true(res$updated)
  expect_equal(nrow(local_dt), 2L)
  expect_match(txt, "RSS narrative items")
  expect_match(txt, "estimate_value")
})

test_that("RSS incremental results identify inserted items, not changed or fetched rows", {
  local_dir <- withr::local_tempdir()
  calls <- 0L
  feed_rows <- function() {
    calls <<- calls + 1L
    dt <- data.table::data.table(
      feed_id = "sec_press_releases", source = "rss",
      guid = c("a", "b"),
      published_at = as.POSIXct(c("2026-09-01 10:00:00", "2026-09-02 10:00:00"), tz = "UTC"),
      published_date = as.Date(c("2026-09-01", "2026-09-02")),
      title = c("A", "B"), summary = c("old", "old"),
      link = c("https://example.test/a", "https://example.test/b"),
      author = NA_character_, category = NA_character_
    )
    if (calls >= 3L) {
      dt <- rbind(dt, data.table::data.table(
        feed_id = "sec_press_releases", source = "rss", guid = "c",
        published_at = as.POSIXct("2026-09-03 10:00:00", tz = "UTC"),
        published_date = as.Date("2026-09-03"), title = "C", summary = "new",
        link = "https://example.test/c", author = NA_character_, category = NA_character_
      ))
      dt[guid == "b", summary := "revised"]
    }
    dt
  }
  old_fetch <- get("get_source_data_rss", envir = asNamespace("investdatar"))
  assignInNamespace("get_source_data_rss", function(...) feed_rows(), ns = "investdatar")
  on.exit(assignInNamespace("get_source_data_rss", old_fetch, ns = "investdatar"), add = TRUE)

  first <- investdatar::sync_local_rss_data("sec_press_releases", "https://example.test/feed", local_path = local_dir)
  second <- investdatar::sync_local_rss_data("sec_press_releases", "https://example.test/feed", local_path = local_dir)
  third <- investdatar::sync_local_rss_data("sec_press_releases", "https://example.test/feed", local_path = local_dir)

  expect_equal(first$n_new_rows, 2L)
  expect_equal(second$n_new_rows, 0L)
  expect_equal(third$n_new_rows, 1L)
  expect_equal(third$inserted_ids, "c")
  expect_equal(third$inserted_published_at_min, as.POSIXct("2026-09-03 10:00:00", tz = "UTC"))
  expect_equal(third$latest_fetched_published_at, as.POSIXct("2026-09-03 10:00:00", tz = "UTC"))
  expect_equal(third$latest_local_published_at, as.POSIXct("2026-09-03 10:00:00", tz = "UTC"))
  local <- investdatar::get_local_rss_data("sec_press_releases", local_path = local_dir)
  expect_equal(local[guid %in% third$inserted_ids, guid], "c")
  expect_equal(local[guid == "b", summary], "revised")
})

test_that("RSS sync reconciles legacy synthetic GUIDs by stable links", {
  local_dir <- withr::local_tempdir()
  old <- data.table::data.table(
    feed_id = "fed_press_all", source = "rss",
    guid = "fed_press_all::NA::A", published_at = as.POSIXct(NA),
    published_date = as.Date(NA), title = "A", summary = "old",
    link = "https://example.test/a", author = NA_character_, category = NA_character_
  )
  saveRDS(old, file.path(local_dir, "fed_press_all.rds"))
  new <- data.table::copy(old)
  new[, `:=`(
    guid = "https://example.test/a",
    published_at = as.POSIXct("2026-09-04 15:00:00", tz = "UTC"),
    published_date = as.Date("2026-09-04"), summary = "current"
  )]
  old_fetch <- get("get_source_data_rss", envir = asNamespace("investdatar"))
  assignInNamespace("get_source_data_rss", function(...) new, ns = "investdatar")
  on.exit(assignInNamespace("get_source_data_rss", old_fetch, ns = "investdatar"), add = TRUE)

  result <- investdatar::sync_local_rss_data("fed_press_all", "https://example.test/feed", local_path = local_dir)
  expect_equal(result$n_new_rows, 0L)
  expect_equal(result$inserted_ids, character())
  expect_equal(as.numeric(result$latest_local_published_at), as.numeric(as.POSIXct("2026-09-04 15:00:00", tz = "UTC")))
  cached <- investdatar::get_local_rss_data("fed_press_all", local_path = local_dir)
  expect_equal(cached$guid, "https://example.test/a")
  expect_equal(cached$summary, "current")
})

test_that("RSS registry helpers return schema-stable tables and batch sync summaries", {
  registry_path <- file.path(withr::local_tempdir(), "rss_registry.json")
  registry <- investdatar::get_rss_registry(registry_path = registry_path)

  expect_s3_class(registry, "data.table")
  expect_equal(names(registry), c("feed_id", "provider", "url", "type", "parser", "main_group", "ca_bundle", "active"))

  registry <- data.table::data.table(
    feed_id = c("atlfed_gdpnow", "bad_feed"),
    url = c("https://www.atlantafed.org/rss/GDPNow", "https://example.com/bad"),
    parser = c("gdpnow", "plain"),
    active = c(TRUE, TRUE)
  )

  old_sync_local_rss_data <- get("sync_local_rss_data", envir = asNamespace("investdatar"))
  assignInNamespace(
    "sync_local_rss_data",
    function(feed_id, url, parser = c("plain", "gdpnow"), local_path = NULL) {
      if (feed_id == "bad_feed") {
        stop(structure(
          list(message = "SSL certificate problem: unable to get local issuer certificate", call = NULL),
          class = c("curl_error_ssl_cacert", "error", "condition")
        ))
      }
      list(updated = TRUE, n_rows = 4L, n_new_rows = 1L)
    },
    ns = "investdatar"
  )
  on.exit(assignInNamespace("sync_local_rss_data", old_sync_local_rss_data, ns = "investdatar"), add = TRUE)

  summary_dt <- investdatar::sync_all_rss_registry_data(registry = registry, local_path = withr::local_tempdir())

  expect_equal(nrow(summary_dt), 2L)
  expect_equal(summary_dt[feed_id == "atlfed_gdpnow", status][[1]], "success")
  expect_equal(summary_dt[feed_id == "bad_feed", status][[1]], "error")
  expect_equal(summary_dt[feed_id == "bad_feed", error_class][[1]], "curl_error_ssl_cacert")
  expect_match(summary_dt[feed_id == "bad_feed", error_message][[1]], "unable to get local issuer")
  expect_false(investdatar::is_sync_run_successful(list(summary = summary_dt)))
})

test_that("RSS CA bundles are request scoped and other feeds do not inherit them", {
  local_dir <- withr::local_tempdir()
  bundle <- file.path(local_dir, "current-ca.pem")
  writeLines("test bundle", bundle)
  withr::local_options(list(
    investdatar.config = list(RSS = list(feed_ca_bundles = list(cftc_press_releases = bundle))),
    investdatar.config_dir = local_dir
  ))
  registry <- data.table::data.table(
    feed_id = c("cftc_press_releases", "fed_press_all"),
    url = c("https://www.cftc.gov/RSS/RSSGP/rssgp.xml", "https://www.federalreserve.gov/feeds/press_all.xml"),
    parser = "plain", active = TRUE
  )
  observed <- list()
  old_sync <- get("sync_local_rss_data", envir = asNamespace("investdatar"))
  assignInNamespace(
    "sync_local_rss_data",
    function(feed_id, url, parser = "plain", local_path = NULL, ca_bundle = NULL) {
      observed[[feed_id]] <<- ca_bundle
      list(updated = TRUE, n_rows = 1L, n_new_rows = 1L)
    },
    ns = "investdatar"
  )
  on.exit(assignInNamespace("sync_local_rss_data", old_sync, ns = "investdatar"), add = TRUE)

  summary_dt <- investdatar::sync_all_rss_registry_data(registry, local_path = local_dir)

  expect_true(all(summary_dt$status == "success"))
  expect_equal(
    observed$cftc_press_releases,
    normalizePath(bundle, winslash = "/", mustWork = TRUE)
  )
  expect_null(observed$fed_press_all)
  expect_true(investdatar::is_sync_run_successful(list(summary = summary_dt)))
  expect_equal(
    investdatar::get_latest_sync_run("rss", local_dir)$params$ca_bundle_feed_ids,
    "cftc_press_releases"
  )
})

test_that("an invalid feed CA bundle fails only that RSS feed", {
  local_dir <- withr::local_tempdir()
  registry <- data.table::data.table(
    feed_id = c("cftc_press_releases", "fed_press_all"),
    url = c("https://www.cftc.gov/RSS/RSSGP/rssgp.xml", "https://www.federalreserve.gov/feeds/press_all.xml"),
    parser = "plain", active = TRUE
  )
  old_sync <- get("sync_local_rss_data", envir = asNamespace("investdatar"))
  assignInNamespace(
    "sync_local_rss_data",
    function(feed_id, url, parser = "plain", local_path = NULL, ca_bundle = NULL) {
      list(updated = TRUE, n_rows = 1L, n_new_rows = 1L)
    },
    ns = "investdatar"
  )
  on.exit(assignInNamespace("sync_local_rss_data", old_sync, ns = "investdatar"), add = TRUE)

  summary_dt <- investdatar::sync_all_rss_registry_data(
    registry, local_path = local_dir,
    ca_bundles = c(cftc_press_releases = file.path(local_dir, "missing.pem"))
  )

  expect_equal(summary_dt$status, c("error", "success"))
  expect_match(summary_dt$error_message[[1L]], "does not exist")
  expect_false(investdatar::is_sync_run_successful(list(summary = summary_dt)))
  expect_false(investdatar::is_sync_run_successful(investdatar::get_latest_sync_run("rss", local_dir)))
})
