#' Master List of RSS Feed Sources for News & Research
#'
#' A structured data frame of curated RSS feed URLs for technology, economics, opinion, and history content.
#' Each feed is categorized by topic, region, type, and poster identity (e.g., media, academia, government).
#'
#' @format A data frame with 20 rows and 5 columns:
#' \describe{
#'   \item{\code{url}}{RSS feed URL (character).}
#'   \item{\code{source}}{Name of the source (e.g., "arXiv AI", "BBC Technology").}
#'   \item{\code{topic}}{Thematic category: \code{"technology"}, \code{"economy"}, \code{"opinion"}, or \code{"history"}.}
#'   \item{\code{region}}{Geographic focus (e.g., "US", "Europe", "China", "global").}
#'   \item{\code{type}}{Content format: \code{"news"}, \code{"opinion"}, \code{"historical review"}.}
#'   \item{\code{poster}}{Publisher type: \code{"media"}, \code{"government"}, or \code{"academia"}.}
#' }
#' @source Mixed RSS feeds from arXiv, NYT, BBC, Bloomberg, IMF, ECB, Fed, SCMP, TOI, and others.
#' @export
RSSlist <- data.frame(
  url = c(
    # TECH & AI
    "https://arxiv.org/rss/cs.AI",
    "https://feeds.bbci.co.uk/news/technology/rss.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    
    # ECONOMY & POLICY
    "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
    "https://www.bloomberg.com/feed/podcast/etf-report.xml",
    "https://www.federalreserve.gov/feeds/press_all.xml",
    "https://www.ecb.europa.eu/rss/press.html",
    "https://www.imf.org/en/News/rss",

    # REGIONAL: ASIA
    "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms",
    "https://www.scmp.com/rss/91/feed",  # SCMP Business
    "https://vietnamnews.vn/rss/economy.rss",  # Vietnam News
    "https://www.straitstimes.com/news/singapore/rss.xml",  # Singapore

    # OPINION & LONGFORM
    "https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml",
    "https://www.project-syndicate.org/feed",  # global op-eds
    "https://feeds.feedburner.com/LongReads",  # longform

    # HISTORY & ACADEMIA
    "https://historynewsnetwork.org/rss.xml",
    "https://aeon.co/feed.rss",  # philosophy/history longform

    # BUSINESS & MARKETS
    "https://www.marketwatch.com/rss/topstories",
    "https://www.reutersagency.com/feed/?best-sectors=business-finance&post_type=best",
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml"  # WSJ Markets

  ),
  source = c(
    "arXiv AI",
    "BBC Technology",
    "NYT Technology",
    
    "NYT Economy",
    "Bloomberg ETF Podcast",
    "US Federal Reserve",
    "European Central Bank",
    "IMF News",

    "Times of India - Business",
    "SCMP Business",
    "Vietnam News - Economy",
    "Straits Times Singapore",

    "NYT Opinion",
    "Project Syndicate",
    "LongReads",

    "History News Network",
    "Aeon Philosophy",

    "MarketWatch",
    "Reuters Business",
    "WSJ Markets"
  ),
  topic = c(
    "technology", "technology", "technology",
    "economy", "economy", "economy", "economy", "economy",
    "economy", "economy", "economy", "economy",
    "opinion", "opinion", "opinion",
    "history", "history",
    "economy", "economy", "economy"
  ),
  region = c(
    "global", "Europe", "US",
    "US", "US", "US", "Europe", "global",
    "India", "China", "Vietnam", "Southeast Asia",
    "US", "global", "global",
    "US", "global",
    "US", "global", "US"
  ),
  type = c(
    "historical review", "news", "news",
    "news", "opinion", "news", "news", "news",
    "news", "news", "news", "news",
    "opinion", "opinion", "opinion",
    "historical review", "historical review",
    "news", "news", "news"
  ),
  poster = c(
    "academia", "media", "media",
    "media", "media", "government", "government", "government",
    "media", "media", "media", "media",
    "media", "media", "media",
    "academia", "media",
    "media", "media", "media"
  ),
  stringsAsFactors = FALSE
)

#' Fetch and Parse RSS Feed by Source Name
#'
#' Downloads and parses articles from a source listed in \code{RSSlist}.
#' Attempts to extract clean text from either the `<content:encoded>` or `<description>` fields.
#' Uses \pkg{xml2} and \pkg{rvest} for HTML parsing and content extraction.
#'
#' @param rss_name A character string matching the \code{source} field in \code{RSSlist}.
#'
#' @return A data frame with columns:
#' \describe{
#'   \item{\code{title}}{Title of the article.}
#'   \item{\code{link}}{URL to the full article.}
#'   \item{\code{content}}{Textual content extracted from the feed (sanitized HTML).}
#'   \item{\code{pubDate}}{Publication date, as a character string.}
#' }
#'
#' @examples
#' \dontrun{
#' df <- fetch_rss("arXiv AI")
#' head(df)
#' }
#'
#' @seealso \code{\link{RSSlist}} for the source catalog.
#' @export
fetch_rss <- function(rss_name) {
  url <- RSSlist[RSSlist$source==rss_name,]$url
  res <- httr::GET(url)
  if (httr::http_error(res)) {
    warning("Failed to fetch: ", url)
    return(NULL)
  }

  doc <- xml2::read_xml(httr::content(res, as = "text", encoding = "UTF-8"))
  items <- xml2::xml_find_all(doc, "//item")
  ns <- xml2::xml_ns(doc)

  # Try to find which prefix maps to content namespace
  content_prefix <- names(ns)[ns == "http://purl.org/rss/1.0/modules/content/"]
  if (length(content_prefix) > 0) {
    prefix <- gsub("^d1:", "", content_prefix[1])  # remove default xml2 prefix like "d1:"
    xpath_encoded <- paste0(".//", prefix, ":encoded")
  } else {
    xpath_encoded <- NA  # No valid content namespace found
  }

  extract_clean_content <- function(item) {
    content_node <- NULL
    if (!is.na(xpath_encoded)) {
      content_node <- tryCatch(
        xml2::xml_find_first(item, xpath_encoded, ns),
        error = function(e) NA
      )
    }
    if (is.na(content_node) || length(content_node) == 0) {
      content_node <- xml2::xml_find_first(item, "description")
    }

    raw_html <- xml2::xml_text(content_node)
    if (!nzchar(raw_html)) return("")
    
    tryCatch(
      rvest::html_text(xml2::read_html(paste0("<div>", raw_html, "</div>"))),
      error = function(e) ""
    )
  }

  data.frame(
    title = xml2::xml_text(xml2::xml_find_all(items, "title")),
    link = xml2::xml_text(xml2::xml_find_all(items, "link")),
    content = vapply(items, extract_clean_content, character(1)),
    pubDate = xml2::xml_text(xml2::xml_find_all(items, "pubDate")),
    stringsAsFactors = FALSE
  )
}

# agent <- NewsReaderAgent$new()
# agent$fetch_rss("History News Network")
# agent$fetch_rss("US Federal Reserve")
# agent$fetch_rss("arXiv AI")


