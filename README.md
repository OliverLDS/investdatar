# investdatar

`investdatar` is an R package for retrieving, standardizing, and locally
synchronizing investment-related datasets in R. It acts as a data access and
local storage layer for financial datasets, with a consistent provider-facing
workflow for:

- comparing local and source update times
- synchronizing locally stored data with upstream sources
- detecting gaps in historical records
- reading locally stored data efficiently
- generating structured dataset descriptions for LLM-based analyst agents that
  produce R analysis and visualization code

Supported providers currently include:

- FRED
- World Bank via `wbstats`
- U.S. Treasury raw daily rates
- RSS narrative feeds
- iShares
- OKX
- Binance
- AlphaVantage
- Yahoo Finance via `quantmod`

## Installation

```r
# install.packages("remotes")
remotes::install_github("OliverLDS/investdatar")
```

`investdatar` is currently released through GitHub, not CRAN.

## Configuration

Users should define `INVESTDATAR_CONFIG` in their `.Renviron` file. It must
point to a YAML file. A minimal example is shipped with the package at
`inst/extdata/investdatar_config_example.yaml`. Credentials such as
`FRED_API_KEY` and `ALPHAVANTAGE_API_KEY` should also be stored in `.Renviron`
when needed.

```sh
INVESTDATAR_CONFIG=/absolute/path/to/investdatar_config.yaml
FRED_API_KEY=your_fred_key
ALPHAVANTAGE_API_KEY=your_alphavantage_key
```

The YAML file is intended for local storage paths and source-specific metadata.
Some providers require API keys, while others such as Yahoo Finance do not.

Minimal example:

```yaml
FRED:
  data_path: /absolute/path/to/fred_data
  registry_file: /absolute/path/to/fred_macro_series_registry.json

WorldBank:
  data_path: /absolute/path/to/world_bank_data
  registry_file: /absolute/path/to/world_bank_series_registry.json

Treasury:
  data_path: /absolute/path/to/treasury_data

RSS:
  data_path: /absolute/path/to/rss_data
  registry_file: /absolute/path/to/rss_feed_registry.json

Crypto:
  data_path: /absolute/path/to/crypto_data
  # OKX local files are stored under /absolute/path/to/crypto_data/okx
  # Binance local files should be stored under /absolute/path/to/crypto_data/binance

iShare:
  data_path: /absolute/path/to/ishare_data
  registry_file: /absolute/path/to/ishare_ticker_registry.json
  holdings_tickers: [DYNF, THRO, BAI, BDYN, BDVL]

YahooFinance:
  data_path: /absolute/path/to/yahoo_finance_data
  registry_file: /absolute/path/to/YahooFinance_ticker_registry.json
```

Relative paths are also supported and are resolved relative to the config file
location.

## First Run

Start from the shipped example, adjust the local paths, then point
`INVESTDATAR_CONFIG` at your copy.

```r
example_cfg <- system.file("extdata", "investdatar_config_example.yaml", package = "investdatar")
example_cfg
```

## Basic Usage

```r
library(investdatar)

cfg <- get_investdatar_config()

fred_dt <- get_source_data_fred("DGS10")
fred_sync <- sync_local_fred_data("DGS10")
fred_local <- get_local_FRED_data("DGS10")

wb_dt <- get_source_data_wbstats("NY.GDP.MKTP.CD", country = "US")
wb_sync <- sync_local_wbstats_data("NY.GDP.MKTP.CD", "US")
wb_local <- get_local_wbstats_data("NY.GDP.MKTP.CD", "US")

treasury_dt <- get_source_data_treasury_rates("par_yield_curve", years = 2026)
treasury_sync <- sync_local_treasury_rates("par_yield_curve")
treasury_local <- get_local_treasury_rates("par_yield_curve")

rss_dt <- get_source_data_rss("atlfed_gdpnow", "https://www.atlantafed.org/rss/GDPNow", parser = "gdpnow")
rss_sync <- sync_local_rss_data("atlfed_gdpnow", "https://www.atlantafed.org/rss/GDPNow", parser = "gdpnow")
rss_local <- get_local_rss_data("atlfed_gdpnow")

ishare_local <- get_local_ishare_data("IVV")
ishare_holdings_sync <- sync_all_ishare_registry_holdings()
ishare_holdings_local <- get_local_ishare_holdings("DYNF")

okx_local <- get_local_okx_candle("BTC-USDT-SWAP", "4H")

yahoo_dt <- fetch_quantmod_OHLC("SPY", from = "2024-01-01", to = "2024-12-31")
yahoo_sync <- sync_all_yahoofinance_registry_data(from = "2024-01-01", to = "2024-12-31")

specs <- list_source_specs()

prompt_txt <- describe_fred_data("DGS10")
```

Minimal local-sync workflow:

```r
library(investdatar)

cfg <- load_investdatar_config(Sys.getenv("INVESTDATAR_CONFIG"))

fred_sync <- sync_local_fred_data("DGS10")
fred_local <- get_local_FRED_data("DGS10")
fred_meta <- get_local_data_meta(fred_sync$file_path)
```

For spec-driven local access, the current local-reader functions map to source
specs as follows:

- `fred` -> `get_local_FRED_data()`
- `wbstats` -> `get_local_wbstats_data()`
- `rss` -> `get_local_rss_data()`
- `treasury` -> `get_local_treasury_rates()`
- `ishare` -> `get_local_ishare_data()`
- `okx` -> `get_local_okx_candle()`
- `binance` -> `get_local_binance_klines()`
- `quantmod` with `src = "yahoo"` -> `get_local_quantmod_OHLC()`

Local path conventions for other market-data specs:

- `binance` should use a `binance/` subdirectory under the configured
  `Crypto.data_path`, mirroring the `okx/` layout
- `quantmod` with `src = "yahoo"` should use the configured
  `YahooFinance.data_path`

Current local sync helpers include:

- `sync_local_fred_data()`
- `sync_local_wbstats_data()`
- `sync_local_rss_data()`
- `sync_local_treasury_rates()`
- `sync_local_ishare_data()`
- `sync_local_ishare_holdings()`
- `sync_local_okx_candle()`
- `sync_local_binance_klines()`
- `sync_local_quantmod_OHLC()`

Yahoo Finance registry batch sync is also available through
`sync_all_yahoofinance_registry_data()`. It reads tickers from the configured
`YahooFinance.registry_file` and synchronizes each one via `quantmod`.

World Bank registry batch sync is available through
`sync_all_wbstats_registry_data()`. It reads indicator definitions from the
configured `WorldBank.registry_file` and synchronizes each registered
`indicator + country + freq` series. If `country` is blank in the registry,
the sync falls back to the package default World Bank scope, which is
`countries_only`.

Treasury raw-rate batch sync is available through `sync_all_treasury_rates()`.
It synchronizes the five built-in Treasury datasets into the configured
`Treasury.data_path`:

- `bill_rates`
- `par_yield_curve`
- `long_term_rates`
- `real_yield_curve`
- `real_long_term_rates`

RSS feed registry batch sync is available through
`sync_all_rss_registry_data()`. It reads feed metadata from the configured
`RSS.registry_file` and synchronizes each configured feed into a local `.rds`
table.

For scheduled local maintenance, a runnable example is shipped at
`inst/scripts/daily_sync.R`. It uses a daily batch plus slower weekly and
monthly syncs:

```r
sync_safe <- function(expr) {
  try(expr, silent = TRUE)
}

sync_safe(investdatar::sync_all_ishare_registry_holdings())
sync_safe(investdatar::sync_all_rss_registry_data())
sync_safe(investdatar::sync_all_treasury_rates())
sync_safe(investdatar::sync_all_yahoofinance_registry_data())

if (format(Sys.Date(), "%u") == "1") {
  sync_safe(investdatar::sync_all_fred_registry_data())
  sync_safe(investdatar::sync_all_ishare_registry_data())
}

if (format(Sys.Date(), "%d") == "01") {
  sync_safe(investdatar::sync_all_wbstats_registry_data())
}
```

You can run the shipped example with:

```sh
Rscript inst/scripts/daily_sync.R
```

The shipped daily sync script checks the latest batch run logs and skips
sources that already ran on the same calendar day, so repeated invocations do
not re-fetch the same batch unnecessarily.

A second shipped script can be run after the sync to print recent RSS headlines
in the terminal and request short AI summaries for newly updated FRED and Yahoo
data:

```sh
Rscript inst/scripts/report_recent_sync.R
```

The reporting script reads the latest batch sync logs to detect which datasets
were actually refreshed and reports newly inserted RSS items from that run. The
AI summary step is optional and requires the `inferencer` package plus your
OpenRouter credentials in the local environment.

The shipped example registry includes Atlanta Fed, SEC, Federal Reserve, and
CFTC seeds:

```json
[
  {
    "feed_id": "atlfed_gdpnow",
    "provider": "atlanta_fed",
    "url": "https://www.atlantafed.org/rss/GDPNow",
    "type": "macro_narrative",
    "parser": "gdpnow",
    "main_group": "us_growth_nowcast",
    "active": true
  },
  {
    "feed_id": "sec_press_releases",
    "provider": "sec",
    "url": "https://www.sec.gov/news/pressreleases.rss",
    "type": "regulatory_press_release",
    "parser": "plain",
    "main_group": "us_regulation",
    "active": true
  },
  {
    "feed_id": "fed_press_all",
    "provider": "federal_reserve",
    "url": "https://www.federalreserve.gov/feeds/press_all.xml",
    "type": "central_bank_press_release",
    "parser": "plain",
    "main_group": "us_monetary_policy",
    "active": true
  },
  {
    "feed_id": "cftc_press_releases",
    "provider": "cftc",
    "url": "https://www.cftc.gov/RSS/RSSGP/rssgp.xml",
    "type": "regulatory_press_release",
    "parser": "plain",
    "main_group": "us_derivatives_regulation",
    "active": true
  }
]
```

For iShares holdings, `sync_all_ishare_registry_holdings()` no longer syncs the
entire iShares registry by default. It reads `iShare.holdings_tickers` from the
package config and, unless you override it, tracks only:

- `DYNF`
- `THRO`
- `BAI`
- `BDYN`
- `BDVL`

`alphavantage` currently provides fetch/standardization helpers but does not yet
expose a package-level local reader/sync helper in the same pattern.

## Notes

- Some provider functions require suggested packages such as `okxr`,
  `quantmod`, `wbstats`, or `zoo`.
- `alphavantage` is currently fetch-only at the package level.
- Local sync helpers write `.rds` data files and `.meta.rds` sidecar metadata.
- Configuration is read from the YAML file referenced by
  `INVESTDATAR_CONFIG`.
- Project URL: <https://github.com/OliverLDS/investdatar>
- Issue tracker: <https://github.com/OliverLDS/investdatar/issues>

## Local Verification

This repository includes a local-library verification workflow so package tests
do not depend on whatever happens to be installed in the global R library.

```sh
scripts/install-local-lib.sh
scripts/verify-local.sh
```

By default, both scripts use `INVESTDATAR_LOCAL_LIB=/tmp/investdatar-r-lib`.
