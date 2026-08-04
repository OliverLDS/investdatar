.fund_holdings_aliases <- list(
  as_of_date = c("as_of_date", "updated_date", "date"),
  holding_id = c("holding_id", "cusip", "isin", "sedol"),
  holding_ticker = c("holding_ticker", "ticker", "Ticker"),
  holding_name = c("holding_name", "name", "Name"),
  asset_class = c("asset_class", "Asset Class"), sector = c("sector", "Sector"),
  weight_pct = c("weight_pct", "weight", "Weight (%)"), shares = c("shares", "quantity"),
  market_value = c("market_value", "market_value_usd"), currency = c("currency"),
  country = c("country", "location", "Location"), exchange = c("exchange", "Exchange")
)

.fund_holdings_column <- function(dt, aliases, type = "character") {
  found <- aliases[aliases %in% names(dt)]
  if (length(found)) return(dt[[found[[1L]]]])
  switch(type, numeric = rep(NA_real_, nrow(dt)), date = as.Date(rep(NA_character_, nrow(dt))), rep(NA_character_, nrow(dt)))
}

#' Standardize Fund Holdings Across Issuers
#'
#' Converts issuer-specific snapshots to the provider-neutral holdings contract.
#'
#' @param data Holdings table.
#' @param provider Stable provider identifier, such as `ishare`.
#' @param fund_id Stable fund ticker or identifier.
#' @param as_of_date Optional snapshot date overriding the source column.
#' @param column_map Optional named list mapping canonical names to source columns.
#' @return A provider-neutral long `data.table`.
#' @export
standardize_fund_holdings <- function(data, provider, fund_id, as_of_date = NULL, column_map = list()) {
  dt <- data.table::as.data.table(data)
  aliases <- .fund_holdings_aliases
  for (nm in names(column_map)) aliases[[nm]] <- as.character(column_map[[nm]])
  get <- function(nm, type = "character") .fund_holdings_column(dt, aliases[[nm]], type)
  dates <- if (is.null(as_of_date)) as.Date(get("as_of_date", "date")) else rep(as.Date(as_of_date), nrow(dt))
  ticker <- trimws(as.character(get("holding_ticker")))
  name <- trimws(as.character(get("holding_name")))
  holding_id <- trimws(as.character(get("holding_id")))
  missing_id <- is.na(holding_id) | !nzchar(holding_id)
  holding_id[missing_id] <- ticker[missing_id]
  missing_id <- is.na(holding_id) | !nzchar(holding_id)
  holding_id[missing_id] <- name[missing_id]
  out <- data.table::data.table(
    provider = tolower(as.character(provider)), fund_id = toupper(as.character(fund_id)),
    as_of_date = dates, holding_id = holding_id, holding_ticker = ticker,
    holding_name = name, asset_class = as.character(get("asset_class")), sector = as.character(get("sector")),
    weight_pct = .to_num(get("weight_pct")), shares = .to_num(get("shares")),
    market_value = .to_num(get("market_value")), currency = as.character(get("currency")),
    country = as.character(get("country")), exchange = as.character(get("exchange"))
  )
  out <- out[!is.na(as_of_date) & !is.na(holding_id) & nzchar(holding_id)]
  data.table::setorderv(out, c("as_of_date", "holding_id"))
  out[]
}

.fund_holdings_local_file <- function(provider, fund_id, local_path) {
  file.path(local_path, tolower(provider), paste0(gsub("[^A-Za-z0-9._-]", "_", toupper(fund_id)), "_holdings.rds"))
}

#' Read Provider-Neutral Local Fund Holdings
#'
#' @param provider Fund issuer/provider identifier.
#' @param fund_id Fund ticker or stable identifier.
#' @param local_path Optional generic holdings storage root.
#' @return A provider-neutral `data.table`, or `NULL`.
#' @export
get_local_fund_holdings <- function(provider, fund_id, local_path = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("fund_holdings")
  .read_local_data_table(.fund_holdings_local_file(provider, fund_id, local_path), sort_cols = c("as_of_date", "holding_id"))
}

#' Synchronize Provider-Neutral Fund Holdings
#'
#' @param data Holdings table, standardized internally.
#' @inheritParams get_local_fund_holdings
#' @param as_of_date Optional snapshot-date override.
#' @param column_map Optional canonical-to-source column mapping.
#' @param source_utime Optional upstream update time.
#' @return A standard synchronization result.
#' @export
sync_local_fund_holdings <- function(data, provider, fund_id, local_path = NULL,
                                     as_of_date = NULL, column_map = list(), source_utime = NULL) {
  if (is.null(local_path)) local_path <- get_source_data_path("fund_holdings", create = TRUE)
  new_data <- standardize_fund_holdings(data, provider, fund_id, as_of_date, column_map)
  sync_local_data(
    new_data, .fund_holdings_local_file(provider, fund_id, local_path),
    key_cols = c("provider", "fund_id", "as_of_date", "holding_id"),
    order_cols = c("as_of_date", "holding_id"), source_utime = source_utime
  )
}

#' Convert Cached iShares Holdings To The Neutral Contract
#'
#' @param ticker iShares fund ticker.
#' @param local_path Optional iShares storage directory.
#' @return A provider-neutral holdings `data.table`, or `NULL`.
#' @export
get_local_ishare_holdings_standardized <- function(ticker, local_path = NULL) {
  dt <- get_local_ishare_holdings(ticker, local_path = local_path)
  if (is.null(dt)) return(NULL)
  standardize_fund_holdings(dt, provider = "ishare", fund_id = ticker)
}
