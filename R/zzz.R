.onLoad <- function(libname, pkgname) {
  cfg_path <- Sys.getenv("INVESTDATAR_CONFIG", unset = "")

  if (!nzchar(cfg_path)) {
    return()
  }

  cfg_path <- path.expand(cfg_path)

  if (!file.exists(cfg_path)) {
    warning("INVESTDATAR_CONFIG does not point to an existing file: ", cfg_path)
    return()
  }

  load_investdatar_config(cfg_path)
}
