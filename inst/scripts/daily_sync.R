sync_safe <- function(expr) {
  try(expr, silent = TRUE)
}

sync_safe(investdatar::sync_all_ishare_registry_holdings())
sync_safe(investdatar::sync_all_rss_registry_data())
sync_safe(investdatar::sync_all_treasury_rates())
sync_safe(investdatar::sync_all_yahoofinance_registry_data())

# Use numeric weekday to avoid locale-dependent weekday names.
if (format(Sys.Date(), "%u") == "1") {
  sync_safe(investdatar::sync_all_fred_registry_data())
  sync_safe(investdatar::sync_all_ishare_registry_data())
}

if (format(Sys.Date(), "%d") == "01") {
  sync_safe(investdatar::sync_all_wbstats_registry_data())
}
