#!/bin/zsh
set -euo pipefail

script_dir="${0:A:h}"
Rscript "$script_dir/check_sync_freshness.R"
