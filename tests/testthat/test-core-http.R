test_that("HTTP requests retry retryable responses", {
  attempts <- 0L
  request <- getFromNamespace(".http_request", "investdatar")

  response <- testthat::with_mocked_bindings(
    .http_perform_once = function(method, url, query, headers, timeout_seconds) {
      attempts <<- attempts + 1L
      if (attempts == 1L) {
        return(list(
          status_code = 429L,
          headers = list("retry-after" = "0"),
          content = charToRaw("rate limited"),
          url = url
        ))
      }
      list(
        status_code = 200L,
        headers = list(),
        content = charToRaw("{}"),
        url = url
      )
    },
    request("GET", "https://example.com", max_attempts = 2L),
    .package = "investdatar"
  )

  expect_equal(attempts, 2L)
  expect_equal(response$status_code, 200L)
})

test_that("HTTP errors carry structured status metadata", {
  request <- getFromNamespace(".http_request", "investdatar")

  error <- testthat::with_mocked_bindings(
    .http_perform_once = function(method, url, query, headers, timeout_seconds) {
      list(
        status_code = 404L,
        headers = list(),
        content = charToRaw("not found"),
        url = url
      )
    },
    tryCatch(
      request("GET", "https://example.com/missing", max_attempts = 1L),
      error = identity
    ),
    .package = "investdatar"
  )

  expect_s3_class(error, "investdatar_http_error")
  expect_equal(error$status_code, 404L)
  expect_match(conditionMessage(error), "HTTP 404")
})

test_that("HTTP requests retry transport failures and retain the final cause", {
  attempts <- 0L
  request <- getFromNamespace(".http_request", "investdatar")

  error <- testthat::with_mocked_bindings(
    .http_perform_once = function(method, url, query, headers, timeout_seconds) {
      attempts <<- attempts + 1L
      stop("TLS handshake failed")
    },
    .http_retry_delay = function(headers, attempt, max_delay = 30) 0,
    tryCatch(
      request("GET", "https://example.com", max_attempts = 2L),
      error = identity
    ),
    .package = "investdatar"
  )

  expect_equal(attempts, 2L)
  expect_s3_class(error, "investdatar_http_transport_error")
  expect_match(conditionMessage(error), "TLS handshake failed")
  expect_equal(error$attempts, 2L)
})
