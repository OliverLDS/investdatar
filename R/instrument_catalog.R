.instrument_catalog_required_fields <- c(
  "schema_version", "instrument_id", "canonical_symbol", "display_name",
  "asset_class", "instrument_type", "quote_currency", "market_calendar",
  "provider_identifiers", "primary_source", "fallback_sources",
  "price_frequency", "supported_intervals", "active"
)

.instrument_catalog_asset_classes <- c(
  "equity", "fixed_income", "commodity", "foreign_exchange", "cryptocurrency"
)

.instrument_catalog_market_calendars <- c("XNYS", "XSHG", "FX_24_5", "CRYPTO_24_7")

.instrument_catalog_instrument_types <- c("etf", "equity_index", "spot_fx", "spot_crypto")

.instrument_catalog_seed_path <- function() {
  path <- system.file("extdata", "instrument_catalog.json", package = "investdatar")
  if (!nzchar(path) || !file.exists(path)) {
    stop("The packaged instrument catalog seed is unavailable.", call. = FALSE)
  }
  path
}

.instrument_catalog_records <- function(catalog_path) {
  if (!file.exists(catalog_path)) {
    stop("Instrument catalog file does not exist: ", catalog_path, call. = FALSE)
  }
  records <- jsonlite::read_json(catalog_path, simplifyVector = FALSE)
  if (!is.list(records) || is.null(records) || length(records) == 0L) {
    stop("Instrument catalog must be a non-empty JSON array.", call. = FALSE)
  }
  records
}

.instrument_catalog_records_to_table <- function(records) {
  rows <- lapply(records, function(record) {
    scalar_names <- c(
      "schema_version", "instrument_id", "canonical_symbol", "display_name",
      "asset_class", "instrument_type", "quote_currency", "market_calendar",
      "price_frequency", "active"
    )
    scalar <- lapply(scalar_names, function(name) record[[name]] %||% NA)
    names(scalar) <- scalar_names
    row <- data.table::as.data.table(scalar)
    row[["provider_identifiers"]] <- list(record$provider_identifiers %||% list())
    row[["primary_source"]] <- list(record$primary_source %||% list())
    row[["fallback_sources"]] <- list(record$fallback_sources %||% list())
    row[["supported_intervals"]] <- list(unlist(record$supported_intervals %||% character(), use.names = FALSE))
    row
  })
  data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
}

.instrument_catalog_is_scalar_string <- function(value) {
  is.character(value) && length(value) == 1L && !is.na(value) && nzchar(value)
}

.instrument_catalog_add_error <- function(errors, check, instrument_id, message) {
  data.table::rbindlist(list(
    errors,
    data.table::data.table(check = check, instrument_id = instrument_id, message = message)
  ))
}

.instrument_catalog_fallbacks_match <- function(catalog_fallbacks, registry_fallbacks) {
  if (length(catalog_fallbacks) != length(registry_fallbacks)) return(FALSE)
  if (length(catalog_fallbacks) == 0L) return(TRUE)

  catalog_keys <- vapply(catalog_fallbacks, function(source) {
    paste(source$provider %||% "", source$symbol %||% "", sep = "\r")
  }, character(1))
  registry_keys <- vapply(registry_fallbacks, function(source) {
    paste(source$provider %||% "", source$symbol %||% "", sep = "\r")
  }, character(1))
  identical(sort(catalog_keys), sort(registry_keys))
}

.instrument_catalog_fallback_key <- function(source) {
  paste(source$provider, source$symbol, sep = "\r")
}

#' Validate the Instrument Catalog
#'
#' Validates the package-owned, provider-neutral instrument catalog and its
#' Yahoo identifier and fallback projection against a Yahoo Finance registry.
#' The Yahoo registry remains the sync authority: catalog fallback entries are
#' validated projections, not instructions for synchronization.
#'
#' Every public field is required and non-null except that `fallback_sources`
#' may be an empty array. `asset_class` must be one of `equity`,
#' `fixed_income`, `commodity`, `foreign_exchange`, or `cryptocurrency`.
#' `market_calendar` must be one of `XNYS`, `XSHG`, `FX_24_5`, or
#' `CRYPTO_24_7`. `provider_identifiers` is a non-empty named list whose
#' values are non-empty provider symbols. `quote_currency` is a three-letter
#' uppercase currency code; `CNH` is permitted as the conventional offshore
#' Chinese-yuan quote code. `price_frequency` and every
#' `supported_intervals` entry are currently limited to `1d`.
#' Each `fallback_sources` object has exactly `provider` and `symbol` fields;
#' pairs must be unique and ordered lexicographically by provider then symbol.
#'
#' @param catalog_path Path to a catalog JSON file. Defaults to the packaged
#'   seed.
#' @param yahoo_registry_path Path to the Yahoo Finance sync registry. Defaults
#'   to the packaged seed registry so the catalog is usable without a runtime
#'   configuration file. Pass a runtime registry path to validate that runtime
#'   registry explicitly.
#'
#' @return A list with `valid`, an `errors` data table, and a `catalog` data
#'   table. The catalog retains nested provider mappings as list columns.
#' @export
validate_instrument_catalog <- function(catalog_path = .instrument_catalog_seed_path(),
                                        yahoo_registry_path = get_yahoofinance_seed_registry_path()) {
  records <- .instrument_catalog_records(catalog_path)
  errors <- data.table::data.table(
    check = character(), instrument_id = character(), message = character()
  )
  ids <- character()
  symbols <- character()

  yahoo_registry <- .read_json_registry(
    yahoo_registry_path,
    empty_cols = c("yahoo_finance_ticker", "fallback_source", "fallback_ticker")
  )
  yahoo_registry[, `:=`(
    yahoo_finance_ticker = as.character(yahoo_finance_ticker),
    fallback_source = as.character(fallback_source),
    fallback_ticker = as.character(fallback_ticker)
  )]

  for (record in records) {
    instrument_id <- as.character(record$instrument_id %||% NA_character_)
    missing_fields <- setdiff(.instrument_catalog_required_fields, names(record))
    if (length(missing_fields) > 0L) {
      errors <- .instrument_catalog_add_error(
        errors, "required_fields", instrument_id,
        paste("Missing required field(s):", paste(missing_fields, collapse = ", "))
      )
      next
    }

    for (field in c("schema_version", "instrument_id", "canonical_symbol", "display_name", "asset_class", "instrument_type", "quote_currency", "market_calendar", "price_frequency")) {
      if (!.instrument_catalog_is_scalar_string(record[[field]])) {
        errors <- .instrument_catalog_add_error(errors, "nullability", instrument_id, paste0(field, " must be a non-empty string."))
      }
    }
    if (!identical(record$schema_version, "1.0.0")) {
      errors <- .instrument_catalog_add_error(errors, "schema_version", instrument_id, "schema_version must be '1.0.0'.")
    }
    if (!.instrument_catalog_is_scalar_string(record$asset_class) || !record$asset_class %in% .instrument_catalog_asset_classes) {
      errors <- .instrument_catalog_add_error(errors, "asset_class", instrument_id, "asset_class is not an allowed enum value.")
    }
    if (!.instrument_catalog_is_scalar_string(record$instrument_type) || !record$instrument_type %in% .instrument_catalog_instrument_types) {
      errors <- .instrument_catalog_add_error(errors, "instrument_type", instrument_id, "instrument_type is not an allowed enum value.")
    }
    if (!.instrument_catalog_is_scalar_string(record$market_calendar) || !record$market_calendar %in% .instrument_catalog_market_calendars) {
      errors <- .instrument_catalog_add_error(errors, "market_calendar", instrument_id, "market_calendar is not an allowed enum value.")
    }
    if (!.instrument_catalog_is_scalar_string(record$quote_currency) || !grepl("^[A-Z]{3}$", record$quote_currency)) {
      errors <- .instrument_catalog_add_error(errors, "quote_currency", instrument_id, "quote_currency must be a three-letter uppercase code.")
    }
    if (!identical(record$price_frequency, "1d")) {
      errors <- .instrument_catalog_add_error(errors, "price_frequency", instrument_id, "price_frequency must be '1d'.")
    }
    if (!is.logical(record$active) || length(record$active) != 1L || is.na(record$active)) {
      errors <- .instrument_catalog_add_error(errors, "active", instrument_id, "active must be a non-null logical scalar.")
    }

    intervals <- unlist(record$supported_intervals, use.names = FALSE)
    if (!is.character(intervals) || length(intervals) == 0L || any(!intervals %in% "1d")) {
      errors <- .instrument_catalog_add_error(errors, "supported_intervals", instrument_id, "supported_intervals must be a non-empty array containing only '1d'.")
    }

    identifiers <- record$provider_identifiers
    if (!is.list(identifiers) || length(identifiers) == 0L || is.null(names(identifiers)) || any(!nzchar(names(identifiers))) || any(!vapply(identifiers, .instrument_catalog_is_scalar_string, logical(1)))) {
      errors <- .instrument_catalog_add_error(errors, "provider_identifiers", instrument_id, "provider_identifiers must be a non-empty named mapping of provider symbols.")
    }

    primary <- record$primary_source
    if (!is.list(primary) || !.instrument_catalog_is_scalar_string(primary$provider) || !.instrument_catalog_is_scalar_string(primary$symbol) || is.null(identifiers[[primary$provider]]) || !identical(identifiers[[primary$provider]], primary$symbol)) {
      errors <- .instrument_catalog_add_error(errors, "primary_source", instrument_id, "primary_source must identify a matching provider_identifiers entry.")
    }

    catalog_fallbacks <- record$fallback_sources
    if (!is.list(catalog_fallbacks)) {
      errors <- .instrument_catalog_add_error(errors, "fallback_sources", instrument_id, "fallback_sources must be an array, not null.")
      catalog_fallbacks <- list()
    }
    invalid_fallbacks <- vapply(catalog_fallbacks, function(source) {
      !is.list(source) || !identical(sort(names(source)), c("provider", "symbol")) ||
        !.instrument_catalog_is_scalar_string(source$provider) ||
        !.instrument_catalog_is_scalar_string(source$symbol)
    }, logical(1))
    if (any(invalid_fallbacks)) {
      errors <- .instrument_catalog_add_error(errors, "fallback_sources", instrument_id, "Each fallback source must contain exactly non-empty provider and symbol strings.")
    } else if (length(catalog_fallbacks) > 0L) {
      fallback_keys <- vapply(catalog_fallbacks, .instrument_catalog_fallback_key, character(1))
      if (anyDuplicated(fallback_keys)) {
        errors <- .instrument_catalog_add_error(errors, "fallback_sources", instrument_id, "fallback_sources provider/symbol pairs must be unique.")
      }
      if (!identical(fallback_keys, sort(fallback_keys))) {
        errors <- .instrument_catalog_add_error(errors, "fallback_sources", instrument_id, "fallback_sources must be ordered by provider then symbol.")
      }
    }

    yahoo_symbol <- identifiers[["yahoo"]]
    if (!is.null(yahoo_symbol) && .instrument_catalog_is_scalar_string(yahoo_symbol)) {
      registry_row <- yahoo_registry[yahoo_finance_ticker == yahoo_symbol]
      if (nrow(registry_row) != 1L) {
        errors <- .instrument_catalog_add_error(errors, "yahoo_mapping", instrument_id, "Yahoo-backed instrument must have exactly one Yahoo registry entry.")
      } else {
        registry_fallbacks <- if (!is.na(registry_row$fallback_source[[1L]]) && nzchar(registry_row$fallback_source[[1L]])) {
          list(list(provider = registry_row$fallback_source[[1L]], symbol = registry_row$fallback_ticker[[1L]]))
        } else {
          list()
        }
        if (!.instrument_catalog_fallbacks_match(catalog_fallbacks, registry_fallbacks)) {
          errors <- .instrument_catalog_add_error(errors, "fallback_consistency", instrument_id, "Catalog fallback_sources do not match the Yahoo registry fallback declaration.")
        }
      }
    }

    ids <- c(ids, instrument_id)
    symbols <- c(symbols, as.character(record$canonical_symbol %||% NA_character_))
  }

  for (id in unique(ids[duplicated(ids) & !is.na(ids)])) {
    errors <- .instrument_catalog_add_error(errors, "unique_instrument_id", id, "instrument_id must be unique.")
  }
  for (symbol in unique(symbols[duplicated(symbols) & !is.na(symbols)])) {
    errors <- .instrument_catalog_add_error(errors, "unique_canonical_symbol", NA_character_, paste0("canonical_symbol must be unique: ", symbol))
  }

  list(
    valid = nrow(errors) == 0L,
    errors = errors[],
    catalog = .instrument_catalog_records_to_table(records)
  )
}

#' Get the Instrument Catalog
#'
#' Returns factual, provider-neutral market-instrument metadata for downstream
#' consumers. It deliberately excludes editorial, ranking, simulation, and
#' execution fields. `canonical_symbol` is a consumer identity, not a provider
#' symbol. Current Yahoo local-cache access and synchronization resolve through
#' `provider_identifiers$yahoo`; do not pass `canonical_symbol` directly to
#' `get_completed_local_quantmod_OHLC()`. `primary_source` describes intended
#' routing; actual row-level provenance remains the `source` recorded by
#' synchronization.
#'
#' @inheritParams validate_instrument_catalog
#' @param validate Logical. When `TRUE` (the default), validate the catalog and
#'   Yahoo registry before returning it.
#'
#' @return A `data.table` with one row per instrument. `provider_identifiers`,
#'   `primary_source`, `fallback_sources`, and `supported_intervals` are list
#'   columns that preserve their JSON mapping or array structure.
#' @export
get_instrument_catalog <- function(catalog_path = .instrument_catalog_seed_path(),
                                   yahoo_registry_path = get_yahoofinance_seed_registry_path(),
                                   validate = TRUE) {
  validation <- validate_instrument_catalog(catalog_path, yahoo_registry_path)
  if (isTRUE(validate) && !validation$valid) {
    messages <- paste(validation$errors$instrument_id, validation$errors$message, sep = ": ")
    stop("Instrument catalog validation failed: ", paste(messages, collapse = "; "), call. = FALSE)
  }
  validation$catalog[]
}
