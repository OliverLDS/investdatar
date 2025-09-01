# investdatar <img src="https://img.shields.io/badge/status-development-blue" align="right"/>

**Investment Data Access and Preparation Toolkit**

---

## Overview

`investdatar` provides a lightweight toolkit for retrieving, preparing, and managing investment-related datasets. It includes helpers for:

- 📊 **Macroeconomic data** via the FRED API
- 🔥 **Crypto market candles** (OKX) with gap detection
- ☁️ **Data sync** utilities for moving candle files between local machines and Google Cloud VMs
- ⏱ **Timeframe utilities** for converting exchange-specific tags into durations

The package is designed to integrate smoothly with R workflows for trading, analysis, and backtesting.

---

## Installation

```r
# install.packages("devtools")
devtools::install_github("OliverLDS/investdatar")
```

---

## Example Usage

### Retrieve FRED Series

```r
config <- list(
  api_key = "your_api_key",
  url     = "https://api.stlouisfed.org/fred/series",
  mode    = "json"
)

dt <- get_source_data_fred("DGS10", config)
head(dt)
```

### Detect Gaps in OKX Candle Data

```r
library(data.table)

# Example data with a missing gap
ts <- as.POSIXct(c(
  "2025-09-01 00:00:00",
  "2025-09-01 04:00:00",
  "2025-09-01 08:00:00"
), tz = "UTC")

dt <- data.table(timestamp = ts)

# Detect gaps
 detect_time_gaps_okx_candle(dt, bar = "4H")
```

### Sync Candle Files with VM

```r
# Download a candle file from VM
download_candle_from_vm(
  inst_id = "ETH-USDT-SWAP",
  bar     = "15m",
  vm_name = "my-vm",
  vm_zone = "asia-east2"
)

# Upload a candle file to VM
upload_candle_from_vm(
  inst_id = "ETH-USDT-SWAP",
  bar     = "15m",
  vm_name = "my-vm",
  vm_zone = "asia-east2"
)
```

---

## Requirements

- R >= 3.5.0
- Packages: `data.table`, `jsonlite`, `curl`
- For VM sync helpers: Google Cloud SDK (`gcloud`) must be installed and authenticated

---

## License

MIT © 2025 Oliver Lee
