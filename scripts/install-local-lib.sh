#!/usr/bin/env sh
set -eu

LOCAL_LIB="${INVESTDATAR_LOCAL_LIB:-/tmp/investdatar-r-lib}"

mkdir -p "$LOCAL_LIB"

export LC_ALL=C
export R_LIBS="$LOCAL_LIB${R_LIBS+:$R_LIBS}"

Rscript -e 'install.packages(c("roxygen2", "testthat", "waldo", "withr", "brio", "callr", "ps", "processx"), repos = "https://cloud.r-project.org")'
