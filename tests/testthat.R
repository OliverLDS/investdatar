library(testthat)
library(investdatar)

if (requireNamespace("waldo", quietly = TRUE)) {
  test_check("investdatar")
} else {
  message("Skipping testthat suite because 'waldo' is unavailable in this environment.")
}
