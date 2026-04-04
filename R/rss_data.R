#' Get RSS Registry File Path
#'
#' Resolve the JSON registry path for RSS feed metadata. If no explicit
#' `registry_file` is configured, the function falls back to a default filename
#' in the package config directory.
#'
#' @param config_dir Optional configuration directory used for the fallback
#'   registry path.
#'
#' @return Character scalar path.
#' @export
get_rss_registry_file_path <- function(config_dir = NULL) {
  cfg <- tryCatch(get_source_config("rss"), error = function(e) list())
  registry_file <- cfg$registry_file

  if (is.null(registry_file) || !nzchar(registry_file)) {
    if (is.null(config_dir)) {
      config_dir <- getOption("investdatar.config_dir")
    }
    if (is.null(config_dir) || !nzchar(config_dir)) {
      stop(
        "No RSS registry path is configured. Set RSS.registry_file in your ",
        "config or load a config file rooted at the desired directory."
      )
    }
    return(file.path(config_dir, "rss_feed_registry.json"))
  }

  .normalize_scalar_path(registry_file, config_dir = getOption("investdatar.config_dir"))
}

#' Get RSS Registry
#'
#' @param registry_path Optional JSON registry path.
#'
#' @return `data.table`.
#' @export
get_rss_registry <- function(registry_path = get_rss_registry_file_path()) {
  .read_json_registry(
    registry_path,
    empty_cols = c("feed_id", "provider", "url", "type", "parser", "main_group", "active")
  )
}

.rss_local_filename <- function(feed_id) {
  feed_id <- gsub("[^A-Za-z0-9._-]+", "_", feed_id)
  sprintf("%s.rds", feed_id)
}

.fetch_rss_feed_text <- function(url) {
  res <- curl::curl_fetch_memory(url)
  rawToChar(res$content)
}

.extract_xml_text <- function(node, xpath, ns = xml2::xml_ns(node)) {
  out <- xml2::xml_text(xml2::xml_find_first(node, xpath, ns = ns), trim = TRUE)
  if (!length(out) || is.na(out)) {
    return(NA_character_)
  }
  out
}

.parse_rss_pubdate <- function(x, tz = "UTC") {
  x <- trimws(x)
  x[!nzchar(x)] <- NA_character_
  if (length(x) == 0L) {
    return(as.POSIXct(character(), tz = tz))
  }

  normalized <- gsub("(\\s)(\\d):(\\d\\d)(:\\d\\d)?(\\s[A-Z]{2,4})$", "\\g{1}0\\2:\\3\\4\\5", x, perl = TRUE)
  normalized <- gsub(":(\\d)(\\s[A-Z]{2,4})$", ":0\\1\\2", normalized, perl = TRUE)
  zone <- sub("^.*\\s([A-Z]{2,4})$", "\\1", normalized, perl = TRUE)
  zone[zone == normalized] <- NA_character_
  ts_text <- trimws(sub("\\s[A-Z]{2,4}$", "", normalized, perl = TRUE))

  parsed <- vapply(seq_along(ts_text), function(i) {
    if (is.na(ts_text[[i]])) {
      return(as.numeric(NA))
    }

    zone_i <- zone[[i]]
    parse_tz <- if (!is.na(zone_i) && zone_i %in% c("EST", "EDT")) "America/New_York" else tz
    ts_i <- as.POSIXct(
      ts_text[[i]],
      tz = parse_tz,
      tryFormats = c(
        "%a, %d %b %Y %H:%M:%S",
        "%A, %d %b %Y %H:%M:%S",
        "%a, %e %b %Y %H:%M:%S",
        "%A, %e %b %Y %H:%M:%S"
      )
    )
    as.numeric(ts_i)
  }, numeric(1))

  as.POSIXct(parsed, origin = "1970-01-01", tz = tz)
}

.quarter_label_to_code <- function(x) {
  q_map <- c(first = "Q1", second = "Q2", third = "Q3", fourth = "Q4")
  m <- regexec("(first|second|third|fourth) quarter of ([0-9]{4})", tolower(x))
  parts <- regmatches(tolower(x), m)[[1]]
  if (length(parts) != 3L) {
    return(NA_character_)
  }
  paste0(parts[3], q_map[[parts[2]]])
}

.parse_gdpnow_direction <- function(title, summary) {
  txt <- tolower(paste(title, summary))
  if (grepl("\\binitial\\b", txt)) return("initial")
  if (grepl("\\bunchanged\\b", txt)) return("unchanged")
  if (grepl("\\bincreased\\b", txt)) return("increased")
  if (grepl("\\bdecreased\\b", txt)) return("decreased")
  NA_character_
}

.parse_rss_gdpnow <- function(dt) {
  dt <- data.table::copy(data.table::as.data.table(dt))
  dt[, narrative_type := "gdpnow_update"]

  period_text <- sub(".* in the ([^.]+?) is [+-]?[0-9.]+ percent.*", "\\1", dt$summary, perl = TRUE)
  period_text[period_text == dt$summary] <- NA_character_

  estimate_value <- sub(".* is ([+-]?[0-9.]+) percent.*", "\\1", dt$summary, perl = TRUE)
  estimate_value[estimate_value == dt$summary] <- NA_character_

  dt[, period_text := period_text]
  dt[, period_label := vapply(period_text, .quarter_label_to_code, character(1))]
  dt[, estimate_value := suppressWarnings(as.numeric(estimate_value))]
  dt[, estimate_unit := ifelse(is.na(estimate_value), NA_character_, "percent")]
  dt[, change_direction := vapply(seq_len(.N), function(i) .parse_gdpnow_direction(title[[i]], summary[[i]]), character(1))]
  dt[]
}

.parse_rss_items <- function(feed_text, feed_id, source = "rss", parser = c("plain", "gdpnow")) {
  parser <- match.arg(parser)
  doc <- xml2::read_xml(feed_text)
  items <- xml2::xml_find_all(doc, ".//item")

  out <- data.table::rbindlist(
    lapply(items, function(item) {
      categories <- xml2::xml_find_all(item, "./category")
      data.table::data.table(
        feed_id = feed_id,
        source = source,
        guid = .extract_xml_text(item, "./guid"),
        published_at = .parse_rss_pubdate(.extract_xml_text(item, "./pubDate"), tz = "UTC"),
        title = .extract_xml_text(item, "./title"),
        summary = .extract_xml_text(item, "./description"),
        link = .extract_xml_text(item, "./link"),
        author = .extract_xml_text(item, "./author"),
        category = if (length(categories) == 0L) NA_character_ else paste(xml2::xml_text(categories, trim = TRUE), collapse = " | ")
      )
    }),
    use.names = TRUE,
    fill = TRUE
  )

  if (nrow(out) == 0L) {
    out <- data.table::data.table(
      feed_id = character(),
      source = character(),
      guid = character(),
      published_at = as.POSIXct(character(), tz = "UTC"),
      title = character(),
      summary = character(),
      link = character(),
      author = character(),
      category = character()
    )
  }

  if (all(is.na(out$guid) | !nzchar(out$guid))) {
    out[, guid := link]
  }
  out[, published_date := as.Date(published_at, tz = "UTC")]

  if (identical(parser, "gdpnow")) {
    out <- .parse_rss_gdpnow(out)
  }

  data.table::setorderv(out, "published_at")
  out[]
}

#' Get RSS Feed Data
#'
#' Download an RSS feed and return a normalized item-level `data.table`.
#'
#' @param feed_id Local feed identifier.
#' @param url RSS feed URL.
#' @param parser Parsing strategy. Currently supports `"plain"` and `"gdpnow"`.
#'
#' @return `data.table`.
#' @export
get_source_data_rss <- function(feed_id, url, parser = c("plain", "gdpnow")) {
  parser <- match.arg(parser)
  feed_text <- .fetch_rss_feed_text(url)
  .parse_rss_items(feed_text = feed_text, feed_id = feed_id, parser = parser)
}

#' Get RSS Feed Last Update Time
#'
#' @param feed_id Local feed identifier.
#' @param url RSS feed URL.
#' @param parser Parsing strategy. Currently supports `"plain"` and `"gdpnow"`.
#'
#' @return POSIXct or `NULL`.
#' @export
get_source_utime_rss <- function(feed_id, url, parser = c("plain", "gdpnow")) {
  dt <- get_source_data_rss(feed_id = feed_id, url = url, parser = parser)
  if (nrow(dt) == 0L || all(is.na(dt$published_at))) {
    return(NULL)
  }
  max(dt$published_at, na.rm = TRUE)
}

#' Get Local RSS Data
#'
#' @param feed_id Local feed identifier.
#' @param local_path Optional local storage path.
#'
#' @return `data.table` or `NULL`.
#' @export
get_local_rss_data <- function(feed_id, local_path = NULL) {
  if (is.null(local_path)) {
    local_path <- get_source_data_path("rss")
  }

  .read_local_data_table(file.path(local_path, .rss_local_filename(feed_id)), sort_cols = "published_at")
}

#' Synchronize Local RSS Data
#'
#' @param feed_id Local feed identifier.
#' @param url RSS feed URL.
#' @param parser Parsing strategy. Currently supports `"plain"` and `"gdpnow"`.
#' @param local_path Optional local storage path.
#'
#' @return A sync result list.
#' @export
sync_local_rss_data <- function(feed_id, url, parser = c("plain", "gdpnow"), local_path = NULL) {
  parser <- match.arg(parser)
  if (is.null(local_path)) {
    local_path <- get_source_data_path("rss", create = TRUE)
  }

  new_dt <- get_source_data_rss(feed_id = feed_id, url = url, parser = parser)
  source_utime <- if (nrow(new_dt) == 0L || all(is.na(new_dt$published_at))) NULL else max(new_dt$published_at, na.rm = TRUE)

  sync_local_data(
    new_data = new_dt,
    local_file_path = file.path(local_path, .rss_local_filename(feed_id)),
    key_cols = c("feed_id", "guid"),
    order_cols = "published_at",
    source_utime = source_utime
  )
}

#' Synchronize All RSS Registry Feeds
#'
#' @param registry Optional RSS registry table.
#' @param local_path Optional local storage path.
#'
#' @return Summary `data.table`.
#' @export
sync_all_rss_registry_data <- function(registry = get_rss_registry(), local_path = NULL) {
  stopifnot(all(c("feed_id", "url") %in% names(registry)))

  if (is.null(local_path)) {
    local_path <- get_source_data_path("rss", create = TRUE)
  }

  if ("active" %in% names(registry)) {
    active <- tolower(as.character(registry$active))
    registry <- registry[is.na(active) | active %in% c("true", "1", "yes", "y")]
  }

  summary_list <- lapply(seq_len(nrow(registry)), function(i) {
    feed_id <- registry$feed_id[[i]]
    url <- registry$url[[i]]
    parser <- "plain"
    if ("parser" %in% names(registry)) {
      parser_value <- registry$parser[[i]]
      if (!is.null(parser_value) && length(parser_value) == 1L && !is.na(parser_value) && nzchar(parser_value)) {
        parser <- parser_value
      }
    }

    tryCatch(
      {
        res <- sync_local_rss_data(feed_id = feed_id, url = url, parser = parser, local_path = local_path)
        data.table::data.table(
          feed_id = feed_id,
          status = "success",
          updated = isTRUE(res$updated),
          n_rows = if (!is.null(res$n_rows)) res$n_rows else NA_integer_,
          n_new_rows = if (!is.null(res$n_new_rows)) res$n_new_rows else NA_integer_,
          error = NA_character_
        )
      },
      error = function(e) {
        data.table::data.table(
          feed_id = feed_id,
          status = "error",
          updated = FALSE,
          n_rows = NA_integer_,
          n_new_rows = NA_integer_,
          error = conditionMessage(e)
        )
      }
    )
  })

  data.table::rbindlist(summary_list, use.names = TRUE, fill = TRUE)
}

#' Describe RSS Data
#'
#' Creates a compact narrative for locally stored RSS narrative data.
#'
#' @param feed_id Local feed identifier.
#' @param local_path Optional local storage path.
#'
#' @return Character scalar narrative.
#' @export
describe_rss_data <- function(feed_id, local_path = NULL) {
  dt <- get_local_rss_data(feed_id = feed_id, local_path = local_path)
  if (is.null(dt) || nrow(dt) == 0L) {
    stop("Local RSS data not found for feed_id: ", feed_id)
  }

  parsed_cols <- intersect(c("narrative_type", "period_label", "estimate_value", "change_direction"), names(dt))
  paste(
    sprintf("This object is a data.table of RSS narrative items for feed %s.", feed_id),
    "Core columns are feed_id, source, guid, published_at, published_date, title, summary, link, author, and category.",
    sprintf("The table currently contains %s rows.", nrow(dt)),
    .describe_time_coverage(dt$published_at),
    sprintf("The summary column has %s missing values.", sum(is.na(dt$summary))),
    if (length(parsed_cols) > 0L) {
      sprintf("Parsed feed-specific columns currently available are %s.", paste(parsed_cols, collapse = ", "))
    } else {
      "No feed-specific parsed columns are currently available."
    },
    "This summary is intended to help another analyst agent understand the dataset before writing R analysis or visualization code."
  )
}
