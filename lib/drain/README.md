Forked from https://github.com/faceair/drain.

The following changes were made to adapt the library for use in VictoriaLogs:
- **Performance**
  - Refactored the code to use v2 of the simplelru package which includes performance improvements and leverages Go generics.
- **Parallel Processing Support**:
  - Added `TrainWithHits(content string, hits uint64)` to support training with pre-aggregated counts.
  - Added `Merge(other *Drain)` to allow merging multiple Drain trees, enabling parallel processing across multiple pipeline shards.
- **Memory Efficiency & Safety**:
  - Integrated with VictoriaLogs' `stateSize` budgeting system. The `Drain` struct now accepts a `*int` budget and subtracts estimated memory usage for nodes, clusters, and cloned strings.
  - Implemented string cloning (`strings.Clone`) for tokens stored in the prefix tree and log clusters. This prevents memory corruption when training the model using strings from temporary buffers or arenas.
  - Optimized the internal cache to "touch" clusters during updates, ensuring they are correctly maintained in the LRU cache.
  - Set the default value for MaxClusters to 1000.
- **Data Integrity**:
  - Changed `LogCluster.size` from `int` to `uint64` to handle massive hit counts without overflow.
