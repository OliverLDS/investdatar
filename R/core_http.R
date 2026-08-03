.investdatar_user_agent <- function() {
  version <- tryCatch(
    as.character(utils::packageVersion("investdatar")),
    error = function(e) "development"
  )
  sprintf("investdatar/%s", version)
}

.http_perform_once <- function(method, url, query, headers, timeout_seconds) {
  response <- httr::VERB(
    verb = method,
    url = url,
    query = query,
    httr::add_headers(.headers = headers),
    httr::timeout(timeout_seconds)
  )

  list(
    status_code = httr::status_code(response),
    headers = httr::headers(response),
    content = httr::content(response, as = "raw"),
    url = response$url
  )
}

.http_retry_delay <- function(headers, attempt, max_delay = 30) {
  retry_after <- headers[["retry-after"]]
  if (!is.null(retry_after)) {
    retry_seconds <- suppressWarnings(as.numeric(retry_after))
    if (!is.na(retry_seconds)) {
      return(min(max(retry_seconds, 0), max_delay))
    }
  }
  min(2^(attempt - 1L), max_delay)
}

.new_http_error <- function(response) {
  body <- rawToChar(response$content)
  if (nchar(body) > 500L) {
    body <- paste0(substr(body, 1L, 500L), "...")
  }
  structure(
    list(
      message = sprintf("HTTP %s returned for %s%s", response$status_code, response$url,
                        if (nzchar(body)) paste0(": ", body) else ""),
      call = NULL,
      status_code = response$status_code,
      response_headers = response$headers,
      response_body = body,
      url = response$url
    ),
    class = c("investdatar_http_error", "error", "condition")
  )
}

.new_http_transport_error <- function(error, method, url, attempts) {
  structure(
    list(
      message = sprintf(
        "%s %s failed after %s attempt(s): %s",
        method, url, attempts, conditionMessage(error)
      ),
      call = NULL,
      url = url,
      attempts = attempts,
      parent = error
    ),
    class = c("investdatar_http_transport_error", "error", "condition")
  )
}

.http_request <- function(method = "GET", url, query = NULL, headers = character(),
                          timeout_seconds = 30, max_attempts = 3L,
                          retry_status = c(429L, 500L, 502L, 503L, 504L)) {
  method <- toupper(method)
  max_attempts <- max(1L, as.integer(max_attempts))
  if (!any(tolower(names(headers)) == "user-agent")) {
    headers[["User-Agent"]] <- .investdatar_user_agent()
  }

  for (attempt in seq_len(max_attempts)) {
    transport_error <- NULL
    response <- tryCatch(
      .http_perform_once(
        method = method,
        url = url,
        query = query,
        headers = headers,
        timeout_seconds = timeout_seconds
      ),
      error = function(e) {
        transport_error <<- e
        NULL
      }
    )
    if (is.null(response)) {
      if (attempt == max_attempts) {
        stop(.new_http_transport_error(transport_error, method, url, attempts = attempt))
      }
      Sys.sleep(.http_retry_delay(list(), attempt = attempt))
      next
    }
    status <- as.integer(response$status_code)

    if (status >= 200L && status < 300L) {
      return(response)
    }
    if (!status %in% retry_status || attempt == max_attempts) {
      stop(.new_http_error(response))
    }
    Sys.sleep(.http_retry_delay(response$headers, attempt = attempt))
  }

  stop("HTTP request failed without a response.", call. = FALSE)
}

.http_response_text <- function(response, encoding = "UTF-8") {
  iconv(rawToChar(response$content), from = encoding, to = "UTF-8")
}

.http_get_json <- function(url, query = NULL, headers = character(), ...) {
  response <- .http_request("GET", url, query = query, headers = headers, ...)
  jsonlite::fromJSON(.http_response_text(response), simplifyVector = TRUE)
}
