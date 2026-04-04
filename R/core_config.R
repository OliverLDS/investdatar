.normalize_scalar_path <- function(path, config_dir = NULL) {
  if (is.null(path) || !is.character(path) || length(path) != 1L || !nzchar(path)) {
    return(path)
  }

  path <- path.expand(path)
  if (!is.null(config_dir) && !grepl("^(/|[A-Za-z]:[/\\\\])", path)) {
    path <- file.path(config_dir, path)
  }

  normalizePath(path, winslash = "/", mustWork = FALSE)
}

.normalize_config_paths <- function(x, config_dir = NULL) {
  if (is.list(x)) {
    out <- x
    for (nm in names(out)) {
      out[[nm]] <- .normalize_config_paths(out[[nm]], config_dir = config_dir)
    }
    return(out)
  }

  if (is.character(x) && length(x) == 1L && grepl("(path|file)$", deparse(substitute(x)), ignore.case = TRUE)) {
    return(.normalize_scalar_path(x, config_dir = config_dir))
  }

  x
}

.normalize_config_tree <- function(cfg, config_dir = NULL) {
  if (!is.list(cfg)) {
    return(cfg)
  }

  out <- cfg
  for (nm in names(out)) {
    value <- out[[nm]]
    if (is.list(value)) {
      out[[nm]] <- .normalize_config_tree(value, config_dir = config_dir)
      next
    }

    if (is.character(value) && length(value) == 1L && grepl("(path|file)$", nm, ignore.case = TRUE)) {
      out[[nm]] <- .normalize_scalar_path(value, config_dir = config_dir)
    }
  }

  out
}

.resolve_source_key <- function(source, config = NULL) {
  stopifnot(is.character(source), length(source) == 1L)
  source_lc <- tolower(source)

  alias_map <- list(
    fred = c("fred"),
    wbstats = c("wbstats", "worldbank", "world_bank", "wb"),
    ishare = c("ishare", "ishares", "ishare_etf", "ishare-etf"),
    rss = c("rss"),
    crypto = c("crypto"),
    okx = c("okx"),
    binance = c("binance"),
    quantmod = c("quantmod"),
    yahoofinance = c("yahoofinance", "yahoo", "yahoofinance"),
    alphavantage = c("alphavantage", "alpha_vantage", "alpha-vantage")
  )

  canonical <- names(alias_map)[vapply(alias_map, function(x) source_lc %in% x, logical(1))]
  if (length(canonical) == 0L) {
    canonical <- source_lc
  }

  if (is.null(config)) {
    return(canonical[[1]])
  }

  existing <- names(config)
  existing_lc <- tolower(existing)

  idx <- match(canonical[[1]], existing_lc)
  if (!is.na(idx)) {
    return(existing[[idx]])
  }

  if (canonical[[1]] == "ishare") {
    idx <- match("ishare", gsub("[^a-z]", "", existing_lc))
    if (!is.na(idx)) {
      return(existing[[idx]])
    }
  }

  if (canonical[[1]] == "yahoofinance") {
    idx <- match("yahoofinance", gsub("[^a-z]", "", existing_lc))
    if (!is.na(idx)) {
      return(existing[[idx]])
    }
  }

  NULL
}

.default_api_configs <- function() {
  list(
    fred = list(
      api_key = Sys.getenv("FRED_API_KEY", unset = ""),
      url = "https://api.stlouisfed.org/fred/series",
      mode = "json"
    ),
    alphavantage = list(
      api_key = Sys.getenv("ALPHAVANTAGE_API_KEY", unset = ""),
      url = "https://www.alphavantage.co/query"
    )
  )
}

.config_example_hint <- function() {
  "See inst/extdata/investdatar_config_example.yaml for a minimal example."
}

#' Load Package Configuration
#'
#' Loads the package YAML configuration and caches it in package options.
#'
#' @param config_path Optional path to the YAML config file. If omitted, the
#'   function uses the `INVESTDATAR_CONFIG` environment variable.
#'
#' @return A normalized configuration list.
#' @export
load_investdatar_config <- function(config_path = Sys.getenv("INVESTDATAR_CONFIG", unset = "")) {
  if (!nzchar(config_path)) {
    stop(
      "No config path supplied and INVESTDATAR_CONFIG is not set. ",
      .config_example_hint()
    )
  }

  config_path <- .normalize_scalar_path(config_path)
  if (!file.exists(config_path)) {
    stop(
      "Config file does not exist: ", config_path, ". ",
      .config_example_hint()
    )
  }

  yaml_lines <- readLines(config_path, warn = FALSE)
  yaml_lines <- gsub("\t", "  ", yaml_lines, fixed = TRUE)
  cfg <- yaml::yaml.load(paste(yaml_lines, collapse = "\n"))
  cfg <- .normalize_config_tree(cfg, config_dir = dirname(config_path))

  options(investdatar.config = cfg)
  options(investdatar.config_path = config_path)
  options(investdatar.config_dir = dirname(config_path))

  cfg
}

#' Get Package Configuration
#'
#' Returns the cached package configuration, loading it from YAML if needed.
#'
#' @param reload Logical. Reload from disk even if configuration is already
#'   cached.
#' @param config_path Optional config path used when `reload = TRUE` or the
#'   cache is empty.
#'
#' @return A configuration list.
#' @export
get_investdatar_config <- function(reload = FALSE, config_path = Sys.getenv("INVESTDATAR_CONFIG", unset = "")) {
  cfg <- getOption("investdatar.config")

  if (reload || is.null(cfg)) {
    if (!nzchar(config_path)) {
      stop(
        "Package configuration is not loaded. Set INVESTDATAR_CONFIG or call ",
        "load_investdatar_config(). ", .config_example_hint()
      )
    }
    return(load_investdatar_config(config_path = config_path))
  }

  cfg
}

#' Get Source Configuration
#'
#' Returns the configuration block for a named source.
#'
#' @param source Character source name.
#' @param config Optional package config list.
#'
#' @return A list, or an empty list if the source is not configured.
#' @export
get_source_config <- function(source, config = get_investdatar_config()) {
  key <- .resolve_source_key(source, config = config)
  if (is.null(key) || is.null(config[[key]])) {
    return(list())
  }
  config[[key]]
}

#' Get Source Data Path
#'
#' Resolves the local data directory for a source using the package config.
#'
#' @param source Character source name.
#' @param config Optional package config list.
#' @param subdir Optional subdirectory appended to the source data path.
#' @param create Logical. Create the directory if it does not exist.
#'
#' @return Character scalar path.
#' @export
get_source_data_path <- function(source, config = get_investdatar_config(), subdir = NULL, create = FALSE) {
  source_cfg <- get_source_config(source, config = config)
  data_path <- source_cfg$data_path

  if (is.null(data_path) || !nzchar(data_path)) {
    stop(
      "No data_path configured for source: ", source, ". ",
      "Add a 'data_path' entry for this source in your config file. ",
      .config_example_hint()
    )
  }

  path <- .normalize_scalar_path(data_path)
  if (!is.null(subdir)) {
    path <- file.path(path, subdir)
  }

  if (isTRUE(create)) {
    dir.create(path, recursive = TRUE, showWarnings = FALSE)
  }

  path
}

.get_api_config <- function(source, config = NULL) {
  defaults <- .default_api_configs()[[tolower(source)]]
  if (is.null(config)) {
    cfg <- tryCatch(get_source_config(source), error = function(e) list())
    config <- utils::modifyList(defaults, cfg)
  } else {
    config <- utils::modifyList(defaults, config)
  }
  config
}
