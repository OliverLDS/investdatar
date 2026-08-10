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
- CFTC TFF, Disaggregated, and Legacy positioning
- U.S. Treasury Fiscal Data
- U.S. Energy Information Administration fundamentals
- SEC EDGAR submissions, XBRL Company Facts and Frames, selected filing documents, and optional bulk archives
- registry-driven SDMX data from ECB, OECD, BIS, Eurostat, and IMF endpoints
- direct BLS labor, BEA regional, and Census Economic Indicators series
- RSS narrative feeds
- iShares
- OKX
- Binance
- Binance and OKX historical crypto derivatives metrics
- AlphaVantage
- Yahoo Finance via `quantmod`

## Installation

```r
# CRAN release, when available:
# install.packages("investdatar")

# Development version:
# install.packages("remotes")
remotes::install_github("OliverLDS/investdatar")
```

Development versions are available through GitHub. Use CRAN releases when
available.

## Configuration

Users should define `INVESTDATAR_CONFIG` in their `.Renviron` file. It must
point to a YAML file. A minimal example is shipped with the package at
`inst/extdata/investdatar_config_example.yaml`. Credentials such as
`FRED_API_KEY`, `ALPHAVANTAGE_API_KEY`, `EIA_API_KEY`, `BLS_API_KEY`,
`BEA_API_KEY`, `CENSUS_API_KEY`, and `SEC_USER_AGENT`
should also be stored in `.Renviron` when needed.

```sh
INVESTDATAR_CONFIG=/absolute/path/to/investdatar_config.yaml
FRED_API_KEY=your_fred_key
ALPHAVANTAGE_API_KEY=your_alphavantage_key
EIA_API_KEY=your_eia_key
BLS_API_KEY=your_optional_bls_key
BEA_API_KEY=your_bea_key
CENSUS_API_KEY=your_census_key
SEC_USER_AGENT=Your Name your_email@example.com
```

The YAML file is intended for local storage paths and source-specific metadata.
Some providers require API keys, while others such as Yahoo Finance do not.
OKX candle sync also falls back to package defaults when no config is passed;
set `OKX_API_KEY`, `OKX_SECRET_KEY`, and `OKX_PASSPHRASE` in `.Renviron` if
you want authenticated OKX access.

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

CFTC:
  data_path: /absolute/path/to/cftc_data
  registry_file: /absolute/path/to/cftc_cot_registry.json

FiscalData:
  data_path: /absolute/path/to/fiscal_data
  registry_file: /absolute/path/to/fiscaldata_registry.json

EIA:
  data_path: /absolute/path/to/eia_data
  registry_file: /absolute/path/to/eia_series_registry.json

BLS:
  data_path: /absolute/path/to/bls_data
  registry_file: /absolute/path/to/bls_series_registry.json

BEA:
  data_path: /absolute/path/to/bea_data
  registry_file: /absolute/path/to/bea_series_registry.json

Census:
  data_path: /absolute/path/to/census_data
  registry_file: /absolute/path/to/census_series_registry.json

SEC:
  data_path: /absolute/path/to/sec_data
  registry_file: /absolute/path/to/sec_company_registry.json
  frames_registry_file: /absolute/path/to/sec_frames_registry.json

SDMX:
  data_path: /absolute/path/to/sdmx_data
  registry_file: /absolute/path/to/sdmx_series_registry.json

RSS:
  data_path: /absolute/path/to/rss_data
  registry_file: /absolute/path/to/rss_feed_registry.json
  # Optional request-scoped overrides for runtimes with an incomplete CA bundle.
  # feed_ca_bundles:
  #   cftc_press_releases: /absolute/path/to/current-ca-bundle.pem

Crypto:
  data_path: /absolute/path/to/crypto_data
  derivatives_registry_file: /absolute/path/to/crypto_derivatives_registry.json
  # OKX local files are stored under /absolute/path/to/crypto_data/okx
  # Binance local files should be stored under /absolute/path/to/crypto_data/binance

iShare:
  data_path: /absolute/path/to/ishare_data
  registry_file: /absolute/path/to/ishare_ticker_registry.json
  holdings_tickers: [DYNF, THRO, BAI, BDYN, BDVL]

YahooFinance:
  data_path: /absolute/path/to/yahoo_finance_data
  registry_file: /absolute/path/to/YahooFinance_ticker_registry.json

AlphaVantage:
  data_path: /absolute/path/to/alphavantage_data
  registry_file: /absolute/path/to/alphavantage_series_registry.json
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

cftc_dt <- get_source_data_cftc_cot("futures_only", market_codes = "020601")
cftc_sync <- sync_all_cftc_cot_registry_data()
cftc_local <- get_local_cftc_cot("tff_futures_only")

fiscal_sync <- sync_all_fiscaldata_registry_data()
debt_local <- get_local_fiscaldata("debt_to_penny")

eia_sync <- sync_all_eia_registry_data()
crude_stocks <- get_local_eia_data("PET.WCESTUS1.W")

sec_submissions_sync <- sync_all_sec_submissions_registry_data()
sec_facts_sync <- sync_all_sec_companyfacts_registry_data()

sdmx_sync <- sync_all_sdmx_registry_data()
ecb_fx <- get_local_sdmx_data("ecb_usd_eur_daily")

derivatives_sync <- sync_all_crypto_derivatives_registry_data()
btc_funding <- get_local_crypto_derivatives("binance", "funding_rate", "BTCUSDT", "funding")

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
- `cftc` -> `get_local_cftc_cot()`
- `fiscaldata` -> `get_local_fiscaldata()`
- `eia` -> `get_local_eia_data()`
- `sec_submissions` -> `get_local_sec_submissions()`
- `sec_companyfacts` -> `get_local_sec_companyfacts()`
- `sdmx` -> `get_local_sdmx_data()`
- `ishare` -> `get_local_ishare_data()`
- `okx` -> `get_local_okx_candle()`
- `binance` -> `get_local_binance_klines()`
- `quantmod` with `src = "yahoo"` -> `get_local_quantmod_OHLC()`
- completed Yahoo daily bars -> `get_completed_local_quantmod_OHLC()`
- `crypto_derivatives` -> `get_local_crypto_derivatives()`

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
- `sync_local_cftc_cot()`
- `sync_local_fiscaldata()`
- `sync_local_eia_data()`
- `sync_local_sec_submissions()`
- `sync_local_sec_companyfacts()`
- `sync_local_sdmx_data()`
- `sync_local_ishare_data()`
- `sync_local_ishare_holdings()`
- `sync_local_okx_candle()`
- `sync_local_binance_klines()`
- `sync_local_quantmod_OHLC()`
- `sync_local_crypto_derivatives()`

For large candle repair workflows, prefer batch repair helpers that fetch all
missing pages or windows in memory and write the local `.rds` file once:

- `repair_local_okx_candle_gaps()`
- `repair_local_binance_klines_gaps()`

OKX and Binance candle readers, sync functions, and repair helpers also accept
`storage = "monthly"`. This migrates a monolithic cache into `YYYY-MM.rds`
partitions and rewrites only months touched by an upsert; bounded reads load
only relevant partitions.

Yahoo Finance registry batch sync is also available through
`sync_all_yahoofinance_registry_data()`. It reads tickers from the configured
`YahooFinance.registry_file` and synchronizes each one via `quantmod`. Each
symbol receives bounded retries with exponential backoff; incomplete OHLC
windows are reported as errors and are not upserted into the local cache.
Completeness requires finite open, high, low, and close values. End-of-window
coverage allows a seven-calendar-day grace period for weekends and market
holidays. Start-of-window coverage and the minimum weekday-row check apply
only when valid local bars already establish the instrument's history, so a
newly listed instrument is not rejected merely for lacking earlier data.
Isolated non-finite bars are dropped rather than being allowed to overwrite a
valid local bar; materially short windows are still rejected.
The shipped 58-symbol seed registry is
`inst/extdata/config/YahooFinance_ticker_registry.json`; copy it into the
configured runtime path when initializing a local registry. Prefer the
deterministic bootstrap below: it creates an absent runtime registry from the
tracked seed and refuses to overwrite an existing one. Validate an existing
registry before scheduled syncs so required fallback declarations cannot drift
silently across machines.

```r
bootstrap_yahoofinance_registry()
validate_yahoofinance_registry()
```

When validation fails, restore the required fallback entries from the tracked
seed. Alternatively, first back up the existing runtime JSON file, remove it,
and run `bootstrap_yahoofinance_registry()` to recreate it. The default Yahoo
registry batch sync performs this validation before making provider requests.

For a known Yahoo-only failure, a registry row can opt into an explicit,
provenance-preserving fallback rather than silently substituting data:

```json
{
  "yahoo_finance_ticker": "000300.SS",
  "fallback_source": "eastmoney",
  "fallback_ticker": "1.000300"
}
```

Fallback bars retain `source = "eastmoney"`, and batch summaries identify
`fetch_method = "eastmoney_fallback"`, `fetch_attempts`, and the failed
primary request in `primary_error`. Before an external fallback, Yahoo failures
also try Yahoo's chart-range endpoint and report
`fetch_method = "yahoo_chart_range_fallback"` when that same-source recovery
succeeds. The external fallback is limited to declared rows; it never replaces
a finite local Yahoo bar and all other symbols continue to use Yahoo through
`quantmod`.

Daily Yahoo OHLC cache rows dated on the current UTC date are provisional even
when open, high, low, close, and volume are finite. Source timestamps and cache
freshness indicate retrieval timing, not that a daily bar is final. Raw reads
retain those rows; use `get_completed_local_quantmod_OHLC()` with an explicit
UTC `as_of` timestamp for analysis requiring completed daily bars. The next
overlap sync upserts the finalized same-date row.

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

CFTC Commitments of Traders batch sync is available through
`sync_all_cftc_cot_registry_data()`. The registry pins the official
TFF, Disaggregated, and Legacy futures-only and combined datasets and can optionally
restrict downloads to selected CFTC contract-market codes. Local synchronization
uses a two-week overlap and keyed upserts so routine runs retrieve only recent
report weeks while retaining corrected values.

Treasury Fiscal Data batch sync is available through
`sync_all_fiscaldata_registry_data()`. The shipped registry covers Debt to the
Penny, the Daily Treasury Statement Operating Cash Balance, auctions, monthly
receipts and outlays, interest expense, and Treasury securities outstanding. Each entry
declares its endpoint and key columns, allowing heterogeneous Treasury tables to
retain their source fields while sharing pagination, incremental synchronization,
metadata, and run-log behavior.

EIA registry batch sync is available through `sync_all_eia_registry_data()`.
The initial registry tracks six weekly physical-market fundamentals covering
petroleum inventories, crude production and refinery inputs, and Lower-48
natural-gas storage. Set `EIA_API_KEY` in `.Renviron`; routine syncs overlap the
latest local month and upsert revised observations.

SEC EDGAR uses one company registry for two independent local datasets.
`sync_all_sec_submissions_registry_data()` stores filing events keyed by CIK and
accession number, including historical submission files on first sync.
`sync_all_sec_companyfacts_registry_data()` stores XBRL facts in long form while
retaining taxonomy, unit, reporting context, accession, and amendment details.
Set `SEC_USER_AGENT` to an identifiable contact before making SEC requests.
Cross-company XBRL Frames can be cached with `sync_local_sec_frame()` or an
explicit Frames registry. `sync_sec_filing_documents()` downloads only selected
primary documents from cached submissions, while `sync_local_sec_bulk_archive()`
keeps the SEC nightly bulk ZIPs opt-in.

SDMX batch sync is available through `sync_all_sdmx_registry_data()`. Registry
entries declare the provider, dataflow, key, CSV format, observation columns,
and dimensions; the local canonical fields are stored alongside the original
provider columns. The shipped registry includes ECB exchange and policy rates,
BIS policy rates, Eurostat HICP, IMF DataMapper macro indicators, and an OECD
composite-leading-indicator seed using the official SDMX REST v1 endpoint.

Crypto derivatives batch sync is available through
`sync_all_crypto_derivatives_registry_data()`. The shipped registry tracks BTC
and ETH funding, open interest, mark/index prices, basis, and Binance long-short
ratios. Public websocket liquidation events can be upserted with
`sync_local_crypto_liquidations()`; private account force-order history is not
mislabeled as market-wide liquidation data.

BLS, BEA, and Census provide selective direct-agency registry workflows through
`sync_all_bls_registry_data()`, `sync_all_bea_registry_data()`, and
`sync_all_census_registry_data()`. The seeds focus on labor-market series,
state GDP/income panels, and advance retail sales where the direct APIs expose
useful source dimensions.

RSS feed registry batch sync is available through
`sync_all_rss_registry_data()`. It reads feed metadata from the configured
`RSS.registry_file` and synchronizes each configured feed into a local `.rds`
table. If the runtime's default certificate store cannot verify one feed, add
that feed under `RSS.feed_ca_bundles` or set its registry `ca_bundle` field.
The override applies only to that feed request and does not disable TLS
verification. Registry-level `ca_bundle` values take precedence over config.

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

`standardize_fund_holdings()` defines a provider-neutral long holdings contract;
`get_local_ishare_holdings_standardized()` converts existing iShares caches
without changing their backward-compatible file layout.

Alpha Vantage supports local readers, incremental full/compact sync, a registry
batch workflow, sidecar metadata, and run logs in the same pattern as other
market providers.

## Notes

- Some provider functions require suggested packages such as `okxr`,
  `quantmod`, `wbstats`, or `zoo`.
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
