## R CMD check results

* GitHub Actions (2026-08-11): R CMD check --as-cran passed on R release
  for Ubuntu, macOS, and Windows, and on R-devel for Ubuntu.
  https://github.com/OliverLDS/investdatar/actions/runs/31455611157
* R-hub (2026-08-10): passed on Windows R-devel and Linux R-devel.
  https://github.com/OliverLDS/investdatar/actions/runs/31407360816
* Win-Builder (2026-08-11): R-release and R-devel both completed with no
  errors or warnings. Each has the standard CRAN incoming NOTE for a new
  submission.
  https://win-builder.r-project.org/3aM0jhgh8XG6
  https://win-builder.r-project.org/9JTB3qV6oZg1

## Test isolation

All provider HTTP requests in the test suite are replaced with mocked bindings
or local fixtures. Examples that require external APIs are wrapped in
`\\dontrun{}`.

## Downstream dependencies

There are no known downstream dependencies.
