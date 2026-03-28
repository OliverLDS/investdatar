# investdatar 0.1.1

- Fixed `get_source_data_wbstats()` so default calls no longer fail from
  forwarding problematic `NULL` and default-only arguments into `wbstats`.
- Added local read/sync helpers for Binance and quantmod/Yahoo:
  `get_local_binance_klines()`, `sync_local_binance_klines()`,
  `get_local_quantmod_OHLC()`, and `sync_local_quantmod_OHLC()`.
- Made source-spec local path metadata consistent with the actual storage
  layout, including `Crypto/okx`, `Crypto/binance`, and `YahooFinance`.
- Expanded README configuration and usage guidance for Yahoo Finance and local
  data access across specs.
