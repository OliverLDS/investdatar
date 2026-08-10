## R CMD check results

* Local macOS R 4.2.3: `R CMD check --as-cran` is run before submission.
* GitHub Actions: R release on Ubuntu, macOS, and Windows; R-devel on Ubuntu.
* Win-Builder: R-devel and R-release checks are submitted before submission.
* R-hub: Windows and Linux R-devel checks are submitted before submission.

## Test isolation

All provider HTTP requests in the test suite are replaced with mocked bindings
or local fixtures. Examples that require external APIs are wrapped in
`\\dontrun{}`.

## Downstream dependencies

There are no known downstream dependencies.
