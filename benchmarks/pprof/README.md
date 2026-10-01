# pprof Source Path Helper

This script is a wrapper around `go tool pprof` designed to
correctly display source code when profiling VictoriaLogs components.

It automatically configures the `-source_path` and `-trim_path` flags to
resolve issues with mismatched paths for modules containing uppercase letters.

## The Problem

By default, pprof looks for source code in the current directory, which often excludes external dependencies.
Even if you manually point the `-source_path` flag to the `GOMODCACHE` directory,
you will find that this approach does not work for modules with uppercase letters in their names:

1. Go escapes uppercase letters in file paths on disk.
   For example, the module `github.com/VictoriaMetrics/VictoriaMetrics` is stored as
   `github.com/!victoria!metrics/!victoria!metrics`.
2. `pprof` searches for source files using the original import path (`VictoriaMetrics`),
   but the directory on disk has a different name (`!victoria!metrics`).
3. As a result, `pprof` fails to locate the file.

As a result, manually configuring `-source_path` and `-trim_path` becomes challenging.

## How It Works

The script resolves this issue by:

1. Removing the module path prefix for problematic modules using the `-trim_path` flag.
2. Constructing the relative path to the source file for problematic modules using the `-source_path` flag.
3. Calling `pprof` with properly generated `-source_path` and `-trim_path` flags.

**Note:** In case of identical relative source paths, the script uses the first match found.
The current working directory has higher priority than external dependencies.
For conflicts between external modules, the script uses the first occurrence in the `MODULES` list.

For example, if the module `github.com/VictoriaMetrics/VictoriaMetrics` contains `pkg/storage/indexdb/indexdb.go`
and the module `github.com/VictoriaMetrics/VictoriaLogs` contains `pkg/storage/indexdb/indexdb.go`,
the script will use the version from the first module in the list.

## Usage

Run the script with the path to the pprof profile as an argument:

```sh
./pprof.sh mem.pprof
```

Or use the HTTP protocol to fetch the profile from a running VictoriaLogs instance:

```sh
./pprof.sh http://localhost:9428/debug/pprof/heap
```

## Configuration

Currently, the script is configured for specific modules with uppercase letters in their names.
If you need to add other libraries that contain uppercase letters in their names,
edit the `MODULES` array inside the script:

```sh
MODULES=(
    "github.com/VictoriaMetrics/VictoriaMetrics"
    # ...
    # Add your module here:
    "github.com/VictoriaMetrics/MyLib"
)
```
