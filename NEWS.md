# investdatar 0.1.3.9

- Added `sync_local_data_batches()` so repair workflows can combine multiple
  fetched pages or windows in memory and write local `.rds` storage once.
- Added OKX and Binance candle repair helpers that fetch multiple missing
  pages or windows before performing a single local upsert.
- Updated source specs and README documentation for candle repair helpers.
- Refined shipped scheduled-sync examples with a cadence-aware `daily_sync.R`
  and a standalone `check_sync_freshness.R` entrypoint for shell workflows.

# investdatar 0.1.3.8

- Added batch run-log writing to the remaining registry and batch sync helpers
  so scheduled sync scripts can consistently inspect the latest run state.
- Updated the shipped `daily_sync.R` example to skip sources that already ran
  on the same day, based on the latest batch sync log.
- Updated Treasury default sync behavior so the first run backfills full
  history, while later runs fetch only from the latest local year through the
  current year.

# investdatar 0.1.3.7

- Added batch sync run logs plus `get_latest_sync_run()` so downstream scripts
  can inspect the most recent registry or batch sync result directly.
- Updated Yahoo Finance batch sync so omitted `from` now uses per-ticker local
  coverage minus a 10-day overlap, reducing download size while preserving
  safety for revised latest rows.
- Added shipped `daily_sync.R` and `report_recent_sync.R` examples for local
  scheduled sync plus terminal reporting of newly inserted RSS items and
  optional AI summaries of recent FRED and Yahoo updates.

# investdatar 0.1.3.6

- Added a World Bank registry workflow with helpers to resolve, read, extend,
  and batch-sync registered `wbstats` indicator definitions.
- Added a shipped yearly World Bank indicator seed registry derived from the
  package's existing analysis workflows.
- Updated config handling and package docs so `WorldBank.registry_file` can
  drive registry-based batch sync, with blank registry `country` values
  defaulting to the standard `countries_only` scope.

# investdatar 0.1.3.5

- Added a raw U.S. Treasury rates provider covering daily bill rates, par
  yield curve rates, long-term rates, real yield curve rates, and real
  long-term rates from the Treasury XML feeds.
- Added standardized fetch, source-update-time, local read, local sync, batch
  sync, and dataset-description helpers for Treasury rate panels.
- Added Treasury source-spec integration plus shipped example config and README
  guidance for the new provider.

# investdatar 0.1.3.4

- Fixed RSS registry batch sync so active feeds are filtered correctly instead
  of collapsing to an empty batch.
- Hardened RSS feed parsing for live malformed feeds by cleaning leading BOM or
  whitespace and falling back to tolerant HTML parsing when strict XML parsing
  fails.
- Added local RSS cleanup and stronger fallback keys so malformed legacy rows,
  including previously broken Federal Reserve press-release rows, are repaired
  on read and resync.

# investdatar 0.1.3.3

- Expanded the shipped RSS registry seeds to include SEC press releases and
  Federal Reserve press releases as registry-driven plain RSS feeds.
- Updated the package README to document the broader shipped RSS seed set.

# investdatar 0.1.3.2

- Added a narrow RSS narrative-feed module with standardized fetch, local read,
  local sync, registry batch sync, and dataset-description helpers.
- Added a feed-specific parser for Atlanta Fed GDPNow RSS items, including
  parsed fields such as period label, estimate value, and change direction.
- Added RSS source-spec integration plus shipped example config and registry
  seed entries for RSS feeds.

# investdatar 0.1.3.1

- Fixed local sync behavior so source rows with existing keys are refreshed
  when upstream providers revise already-known observations instead of only
  appending unseen keys.
- This resolves stale latest-row issues in Yahoo Finance via `quantmod` and
  the same sync behavior for other providers that write through the shared
  local sync helper.

# investdatar 0.1.3

- Added iShares holdings fetch, local read, single-ticker sync, and registry
  batch sync helpers.
- Added automatic migration of legacy iShares holdings snapshot-list files into
  the new fixed-column long-table format.
- Added Yahoo Finance registry helpers and batch sync through `quantmod`,
  driven by `YahooFinance.registry_file`.
- Improved `quantmod` sync error reporting so upstream Yahoo failures surface as
  explicit errors instead of collapsing to `new_data_is_null`.
- Updated `describe_quantmod_data()` so omitted `from` and `to` default to the
  oldest and newest dates in local quantmod data.
- Added `iShare.holdings_tickers` config support and restricted default
  holdings batch sync to the configured subset.

# investdatar 0.1.2

- Public GitHub release metadata cleanup, including author contact and package
  description updates.
- Added a shipped example configuration file and expanded README onboarding for
  first-time users.
- Hardened fresh-install configuration and FRED registry handling so missing
  files fail more predictably.
- Added tests covering example configuration loading and missing-config or
  missing-registry behavior.

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
