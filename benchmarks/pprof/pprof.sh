#!/bin/bash
set -e

PPROF_FILE="${1:-cpu.pprof}"

# Define the list of paths where pprof can find the source code.
# Initially, this list contains paths to the current directory, the Go standard library, and project dependencies.
SOURCE_PATH="$(pwd):$(go env GOROOT)/src:$(go env GOMODCACHE)"
TRIM_PATH=""

MODULES=(
    "github.com/VictoriaMetrics/VictoriaMetrics"
    "github.com/VictoriaMetrics/easyproto"
    "github.com/VictoriaMetrics/metrics"
)
for mod in "${MODULES[@]}"; do
    # Add the local vendor path to source paths.
    SOURCE_PATH="$SOURCE_PATH:$(pwd)/vendor/$mod"
    # Retrieve the module version to populate trim_path.
    MOD_VERSION=$(go list -m -f "$mod@{{.Version}}" "$mod")

    if [ -z "$TRIM_PATH" ]; then
        TRIM_PATH="$MOD_VERSION"
    else
        TRIM_PATH="$TRIM_PATH:$MOD_VERSION"
    fi
done

go tool pprof -trim_path "$TRIM_PATH" -source_path "$SOURCE_PATH" "$PPROF_FILE"
