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

This package is currently released through GitHub, not CRAN.

## Configuration

Users should define `INVESTDATAR_CONFIG` in their `.Renviron` file. It must
point to a YAML file that they create and maintain themselves. Credentials such
as `FRED_API_KEY` and `ALPHAVANTAGE_API_KEY` should also be stored in
`.Renviron`.

```sh
INVESTDATAR_CONFIG=/absolute/path/to/investdatar_config.yaml
FRED_API_KEY=your_fred_key
ALPHAVANTAGE_API_KEY=your_alphavantage_key
```

The YAML file is intended for local storage paths and source-specific metadata
such as registry file names, cache paths, and identifier lists. Some providers
require API keys, while others such as Yahoo Finance do not.

Example:

```yaml
FRED:
  data_path: /absolute/path/to/fred_data
  registry_file: /absolute/path/to/fred_macro_series_registry.json

WorldBank:
  data_path: /absolute/path/to/world_bank_data

Crypto:
  data_path: /absolute/path/to/crypto_data

iShare:
  data_path: /absolute/path/to/ishare_data
```

## Basic usage

```r
library(investdatar)

cfg <- get_investdatar_config()

fred_dt <- get_source_data_fred("DGS10")
fred_sync <- sync_local_fred_data("DGS10")

wb_dt <- get_source_data_wbstats("NY.GDP.MKTP.CD", country = "US")
wb_sync <- sync_local_wbstats_data("NY.GDP.MKTP.CD", "US")

specs <- list_source_specs()

prompt_txt <- describe_fred_data("DGS10")
```

## Notes

- Some provider functions require suggested packages such as `okxr`,
  `quantmod`, `wbstats`, or `zoo`.
- Local sync helpers write `.rds` data files and `.meta.rds` sidecar metadata.
- Configuration is read from the YAML file referenced by
  `INVESTDATAR_CONFIG`.
- Project URL: <https://github.com/OliverLDS/investdatar>
- Issue tracker: <https://github.com/OliverLDS/investdatar/issues>
