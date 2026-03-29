#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
LOCAL_LIB="${INVESTDATAR_LOCAL_LIB:-/tmp/investdatar-r-lib}"

if [ ! -d "$LOCAL_LIB" ]; then
  echo "Missing local verification library: $LOCAL_LIB" >&2
  echo "Initialize it first with scripts/install-local-lib.sh." >&2
  exit 1
fi

export LC_ALL=C
export R_LIBS="$LOCAL_LIB${R_LIBS+:$R_LIBS}"

cd "$ROOT_DIR"

Rscript -e 'roxygen2::roxygenise()'
Rscript -e 'testthat::test_local(".", reporter = "summary")'
rm -rf investdatar.Rcheck
R CMD build .
R CMD check investdatar_0.1.2.tar.gz --no-manual
