# Experimental sealed-block Stream compression

> Historical shared-dictionary experiment. The current plain-only persistence
> policy and sustained CPU/latency report are in [STREAM-PLAIN-ZSTD.md](STREAM-PLAIN-ZSTD.md).
> Plain-policy persistence no longer has the limitation described below.

This server-side prototype is based on Redis upstream commit `68d9a2df0`.
Ordinary XADD/XRANGE/XREAD and group commands retain their logical bytes and IDs.
The current revision compares **shared, versioned dictionaries** with ordinary
Zstd at the same level, using Korvet-shaped events as well as the initial six
workloads. It supersedes the embedded-dictionary experiment; its historical
numbers remain in `stream-compression-results.json` and are not directly
comparable because dictionary size, training, codec level and payloads changed.

## Build and configure

```sh
make -C src -j4 BUILD_COMPRESSION=yes MALLOC=libc OPT=-O2 REDIS_CFLAGS='-Werror -DREDIS_TEST' all
src/redis-server test stream-compression
./runtest --compression --single unit/type/stream-compression --single unit/type/stream --single unit/type/stream-cgroups --single unit/moduleapi/stream --clients 4 --timeout 180
```

This reuses upstream's opt-in libzstd build/linking. Normal builds do not need
Zstd and reject enabling compression. The local library is Zstd 1.5.7. The
custom-allocation APIs used here are exposed through ZSTD_STATIC_LINKING_ONLY;
this prototype has not been tested against other libzstd versions/builds.

Create a stream first, then use binary-safe RESP:

```text
XCFGSET events COMPRESSION ZSTD
XCFGSET events COMPRESSION ZSTD DICTIONARY <trained-dictionary-bytes>
XCFGSET events COMPRESSION OFF
```

Both Zstd modes use **level 3** with checksums. Only trained dictionaries with a
nonzero native Zstd dictionary ID, at most 65536 bytes, are accepted. All command
arguments are validated before applying configuration. DICTIONARY requires
COMPRESSION ZSTD in the same command. Identical dictionary bytes/IDs reuse an
existing generation; the same ID with different retained bytes is rejected.
ZSTD without DICTIONARY switches to ordinary Zstd. OFF stops future compression.
No training occurs in Redis or on XADD: training is offline in the harness.

## Storage layout and ownership

Streams retain the radix tree mapping master entry IDs to listpacks. Existing
master field schemas, delta IDs, ordering, duplicate fields and binary values
are preserved by compressing the entire listpack. A compressed node is a valid
two-element physical listpack:

```text
[-1, "SZ02" | original_length:u32be | encoding:u32be(1=Zstd) |
     dictionary_id:u32be(0=none) | checksummed_Zstd_frame]
```

Normal stream listpacks never have negative live-entry counts. Dictionary
bytes are stored **once per stream generation**, outside the blocks. Each
registry node owns immutable bytes, the Zstd ID, and a stored-block reference
count. The current generation is also retained when its block count is zero.
There are at most 16 retained generations. Lookup/collection is therefore
bounded. Registry allocations and dictionary allocator sizes are charged to the
stream's alloc_size and to Redis used_memory. COPY duplicates the blocks and
registry, preserving reference counts and independent ownership across keys.

Rotation releases only an unused previous current generation. Historical
versions stay available until their last block is freed or materialized. DEL,
UNLINK, expiration and flushing use the stream destructor, which releases every
reference and then the registry. No dictionary can be deleted while a stored
block still references it. An external client rotation which would exceed the
16-generation limit is rejected atomically. A slot becomes available when a
retired generation loses its final reference. There is no global registry or
cross-stream dependency.

Compression happens once when XADD seals a previous append listpack. The append
block stays raw, and enabling compression does not backfill old blocks. Codec
input and decoded allocations are capped at **64 KiB per block**; larger blocks
remain raw. Each append attempts compression on at most one sealed block. A
candidate must reduce both encoded and allocator usable bytes, including the
physical envelope. Shared dictionary cost is charged once at configuration,
not repeatedly to candidates: it remains in every memory measurement, including
before the stream breaks even or when incompressible data stays raw.

Reads allocate iterator-owned decoded scratch and free it at each node
transition/stop. Mutations materialize the affected stored block and release its
dictionary reference; they leave it raw. Trimming also materializes visited
boundary nodes even if it deletes nothing. An older compressed node that becomes
the append tail is materialized before append. Large scans/trims retain their
existing O(N) command cost; a bounded codec does not make those commands constant
work. Small pages can repeatedly decode the same block.

Both modes create/free a codec context per block. Context reuse and prepared
CDict/DDict caching are deliberately absent, so dictionary digestion remains a
CPU cost and timings are not directly comparable to Korvet's current cached
contexts. All Redis-side Zstd context/dictionary validation allocations use
custom zmalloc callbacks. XINFO STREAM and FULL expose an optional `compression`
map: enabled, level, dictionary-generations, dictionary-memory (including
registry metadata/slack), dictionary-block-references, compression-scratch-peak,
and decompression-scratch-peak. The two peaks count codec-native heap allocations
plus concurrently allocated frame/listpack scratch, excluding stored inputs,
dictionary registry and command reply buffers, stack usage and allocator-internal realloc transients. They are per-operation allocation
high-water marks, not OS RSS or whole-command memory. Decode scratch may coexist
with another iterator's scratch; that concurrency is outside these counters.

## Validation and corruption

Decode validates envelope version, encoding, length bounds, registry dictionary
identity against the frame ID, a single complete frame without trailing bytes,
frame content size, checksum, exact decoded length and deep stream-listpack
integrity. The decoder's maximum window is 64 KiB. It never substitutes empty
records or a default dictionary. The fallible decoder returns an explicit error;
command iterators fail-stop through serverPanic on internal corruption, matching
the existing internal listpack-corruption model. Recoverable RESP corruption
errors remain out of scope. Memory envelopes are never loaded from persistence.

## Persistence, replication and physical-state divergence

RDB saving/DUMP/full replication sync decode envelopes and emit the original
ordinary listpacks. There is no new RDB type or version. RDB and RDB-preamble
AOF load raw data with compression policy OFF; historical dictionaries need not
survive because no persisted block references them. Reapply XCFGSET for future
appends. Plain AOF rewrite emits original entries and restores current policy
for future appends, but historical rewritten entries are initially raw.

Incremental replication/AOF propagate original command bytes, including XCFGSET.
A replica/replay may have different physical dictionary references: a no-op trim
can materialize a block only on the primary, and RDB full sync starts raw.
Therefore a registry cap or dictionary-ID collision accepted on the primary
must not abort logical replay on a replica. For CLIENT_MASTER/server.loading
only, a local registry conflict logs a warning, clears the current alias,
disables future compression, and retains every referenced old generation.
Subsequent appends are raw. This is **encoding-policy fallback**, never a decode
fallback. It is visible as enabled=0 in XINFO. Direct-client conflicts still
return errors. A later configuration can re-enable compression once references
permit it. This bounded fallback avoids unbounded materialization or registry
growth and preserves the data through live replication and incremental AOF.
The cap/no-op-trim case is a focused regression for both paths.

Mixed-version replication and incremental/plain AOF containing experimental
XCFGSET options are unsupported: old servers/builds without Zstd reject them.
RDB compatibility is upstream's format at this commit, not a promise that old
Redis versions can load it. Disable compression before producing a plain AOF
for builds that do not recognize these options. Partial resync/failover are not
separately exercised. RDB, DUMP/RESTORE, AOF incremental/rewrite in both formats,
full sync, subsequent writes/trim/delete, groups and dictionary rotation are
covered by focused checks.

An inherited upstream behavior was reproduced on the unmodified baseline:
plain AOF rewrite drops pending references to deleted entries (2 pending before,
1 after, in a two-entry reproducer). Deleted PEL preservation is tested with
RDB; the AOF fixture uses live pending entries. This prototype does not fix it.

## Storage access audit and remaining coverage

| Paths | Handling / coverage |
|---|---|
| XADD, IDMP/IDMPAUTO, module StreamAdd | Shared append path; compress only on rollover; ordinary and IDMP regression suites |
| XRANGE/XREVRANGE/XREAD | Shared iterator scratch; binary/repeated/empty fields, order and page checks; compressed blocking wakeup remains unverified |
| XREADGROUP/history/CLAIM, XCLAIM, XAUTOCLAIM, XNACK, XPENDING/XACK/XGROUP | Shared iterator or unchanged PEL metadata; focused delivery/history/deleted-null/claim tests and full group suite |
| XDEL/XDELEX/XACKDEL/XTRIM, XADD MAXLEN/MINID | Materialize before mutation and release refs; exact/approx trim, DELREF tests; not every compressed ACKED combination tested |
| XINFO/FULL, edge/entry-existence/last-ID helpers, DEBUG DIGEST, AOF rewrite | Iterator decodes; XINFO schema updated for optional compression map; FULL/group combinations not exhaustively tested compressed |
| RDB/DUMP/full sync/RESTORE and migration serialization | Save ordinary listpacks; focused restart/full-sync checks; cluster migration unverified |
| COPY, DEL/UNLINK, expire/flush/eviction | Copy registry independently, destructor releases refs; COPY/source deletion and free tested; compressed lazyfree/eviction stress unverified |
| MEMORY/slot accounting, fork memory dismissal, active defrag | Envelopes retain listpack-sized allocations; registry included in alloc_size; defrag moves stream/rax/envelopes, leaves registry allocations in place; active defrag not exercised on libc |
| Module stream iteration/deletion | Shared iterator; module regression suite uses default raw streams |
| Development streamLogListpackContent | Logs physical envelope if handed one; not a command/persistence path |

## Reproduce the bounded comparison

```sh
# Export and build an unmodified copy of the same upstream commit.
git archive --output=/tmp/redis-baseline.tar 68d9a2df0
mkdir -p /tmp/redis-baseline
tar -xf /tmp/redis-baseline.tar -C /tmp/redis-baseline
make -C /tmp/redis-baseline/src -j4 BUILD_COMPRESSION=yes MALLOC=libc OPT=-O2 REDIS_CFLAGS='-Werror -DREDIS_TEST' all
python3 tools/stream-compression-bench.py --baseline-server /tmp/redis-baseline/src/redis-server --output /tmp/stream-shared-results.json
```

Python 3.9+ and system libzstd are required. There are nine workloads and three
modes: unmodified Redis, ordinary block Zstd, shared-dictionary block Zstd.
Each case stores 5000 entries; training uses a separate seed and disjoint ID
range, at most 10000 records/16 MiB, with dictionary capacity 64 KiB. The resulting
dictionary may be smaller. Training happens once per workload and is excluded
from measured XADD latency. Record value bytes (not field names or exact listpack headers)
are training material. Intentionally constant JSON can have identical payloads
across training and test even though records are separately generated.

Workloads: constant JSON; varying purchase schema; native fields including a
repeated field and empty value; ~8 KiB repetitive records; zlib-compressed random
1024-byte values; purchase-to-telemetry drift; Korvet-style small log events;
Korvet-style events padded to at least 700 bytes; and Korvet-to-telemetry drift.
Korvet's synthetic logger/status/latency/host/trace field structure is reproduced,
not its exact Java RNG dataset. Actual payload sizes are recorded: 244 is a
lower-bound parameter, not an assertion that the full event is 244 bytes.
The Korvet-shaped server workloads use one value field; they do not add the
Kafka key/timestamp metadata fields of a complete broker produce path. Absolute
Redis memory numbers across the two projects are therefore not directly comparable.

All modes use the same held-out records. Node limits are 4096 bytes/100 entries.
The configuration connection is closed and replaced in every mode before timed
writes, so the 64 KiB dictionary-install query buffer is not retained in the
steady-state comparison. INFO is warmed before the empty baseline.
There is one client, concurrency 1, at most two disposable servers for replication,
256 MiB Redis maxmemory/noeviction per process, a 900-second overall timeout and
bounded readiness/rewrite/sync waits. maxmemory is not an OS RSS limit. No existing
Redis instance or dataset is accessed. No build/tests run alongside final timing.

XADD has 4999 timed serial round trips after initial key creation. XRANGE/XREAD
use 100 pages of 10 and 15 pages of 1000 entries. Group calls use 50 ten-entry
samples; XDEL uses 100 calls, plus one half-length trim and DEL. Round trips
include Python RESP parsing. Fifteen-sample scan p99 is just the observed maximum,
not a reliable tail estimate. SAVE/RDB restart verifies every byte and pending
metadata. AOF/full-sync timing is from a small correctness fixture, not each
5000-entry workload; plain/preamble AOF fixtures have different entry counts.

For Korvet shapes an additional **payload-only codec reference** compresses the
same held-out record values independently at level 3 with/without the shared
dictionary. These frame-byte counts isolate the per-record benefit from block
redundancy. They exclude Redis entry/registry memory and are not Redis memory
measurements or throughput estimates. The reference uses stock checksum-default
frames, like Korvet; server blocks explicitly enable checksums.

Raw results include total Redis used_memory, empty-server delta, MEMORY USAGE,
dictionary metadata/bytes, memory checkpoints at 100/500/1000/2500/5000 entries,
latency p50/p95/p99, throughput, RDB size/restart/trim/DEL, per-codec scratch peaks,
whole-lifetime peak child RSS from wait4, training/binary hashes and actual sizes.
Logical MEMORY USAGE includes allocator sizes of registry/dictionaries but only
encoded listpack lengths; used_memory delta also includes listpack slack and
connection effects. Peak RSS includes validation scans and reply buffers and
can be much higher than steady compressed storage.

## Measured results and review

Measurements used macOS 26.7.1 arm64, libc, Clang `-O2`, Zstd 1.5.7, and
`68d9a2df09ec7d4a5c03abe7c1424ad26e896b5d` as the unmodified reference.
Three independent normalized sweeps completed all 81 cases in 57.43 active
benchmark seconds total. Each used the stated 900-second limit; the serial
three-run driver additionally bounded each child at 280 seconds. The first
exploratory sweeps were discarded after detecting dictionary-install query-buffer
retention. The reported sweeps reconnect after configuration in all modes.

Tables report the **median of each per-run statistic**, not pooled percentiles.
This remains a small synthetic smoke benchmark with no capacity or confidence
interval claim. Scan p99 has only 15 samples per run. No build/test/reviewer
reproduction ran alongside the reported sweeps.

The complete normalized runs, summary, binary/training hashes, every read/group
latency percentile, RSS and scratch measurement are in
[stream-compression-shared-results.json](stream-compression-shared-results.json).
The top-level results are medians; `runs` contains the three original outputs.

### Memory including the shared registry

Stream B/entry is MEMORY USAGE / 5000, including dictionary bytes and registry
allocator overhead. Allocated dataset reduction compares `(used_memory - warmed
empty-server used_memory)` for the two modes and includes listpack allocator
slack and connection effects. Negative reductions mean dictionary mode is larger.

| Workload | Raw stream B/entry | Plain Zstd | Shared dictionary | Stream reduction vs Zstd | Allocated dataset reduction |
|---|---:|---:|---:|---:|---:|
| small-json | 95.6 | 7.9 | 7.0 | 10.9% | 5.9% |
| schema | 153.3 | 28.9 | 45.7 | -58.3% | -35.8% |
| native | 30.0 | 10.2 | 31.6 | -209.9% | -81.5% |
| large | 8714.0 | 292.8 | 204.5 | 30.2% | 25.7% |
| entropy | 1068.3 | 1068.3 | 1084.7 | -1.5% | -1.0% |
| drift | 415.3 | 153.5 | 189.5 | -23.5% | -20.5% |
| korvet-244 | 274.3 | 67.6 | 72.8 | -7.8% | -9.0% |
| korvet-700 | 726.9 | 196.4 | 131.0 | 33.3% | 26.7% |
| korvet-drift | 475.8 | 172.9 | 198.8 | -15.0% | -14.4% |

For the ~702-byte Korvet workload, the shared 65536-byte dictionary consumes
**81952 allocated bytes including its SDS allocation/slack and registry node**.
Even charging that whole allocation, it reduces stream memory 33.3% and the
allocated dataset delta 26.7%. Total Redis memory (including the empty-server
base) falls from approximately 2.16 MB to 1.86 MB. Actual per-record sizes are
about 257.9 B / 702.0 B for the two Korvet shape variants.

The small-record result explains the earlier apparent contradiction: ordinary
block Zstd already stores the ~258-byte events in 67.6 B/entry. The dictionary
reduces block bytes before its one-off registry cost, but at 5000 records that
fixed allocation takes total stream memory to 72.8 B/entry. The stale-dictionary
drift case is worse than ordinary block Zstd even at this sample size.

### Reproducing the per-record Korvet benefit

These are **measured frame bytes per independent record**, not Redis memory:

| Held-out workload | Input value | Plain frame | Dictionary frame |
|---|---:|---:|---:|
| korvet-244 | 257.9 | 213.9 | 62.1 |
| korvet-700 | 702.0 | 374.9 | 100.4 |
| korvet-drift | 455.7 | 259.5 | 198.8 |

The small-record payload-only reference reproduces Korvet’s effect: roughly
**258 → 214 B with plain Zstd, versus 62 B with a dictionary**. It uses the same
held-out values as the block experiment. Thus the dictionary helps per-record
compression substantially; the relevant baseline for transparent server storage
is stronger because plain block Zstd can already use cross-record redundancy.

Korvet’s original investigation reports ~244 → 64 B dictionary frames and a
3.4x backing-stream reduction on 10000 events, using a shared 64 KiB dictionary;
its stream measurement excludes separately stored dictionary keys and broker
contexts. These are reported source results, not reruns of the Java broker:
[investigation #1508](https://github.com/redis-field-engineering/korvet/issues/1508).

### Dictionary break-even

First observed win is the first sampled checkpoint among
100/500/1000/2500/5000 entries; it is not an exact crossing:

| Workload | Stream-memory first win | Allocated-dataset first win | Dictionary allocator bytes |
|---|---|---|---:|
| small-json | 500 | 500 | 256 |
| schema | Not observed through 5000 | Not observed through 5000 | 81952 |
| native | Not observed through 5000 | Not observed through 5000 | 81952 |
| large | 1000 | 1000 | 81952 |
| entropy | Not observed through 5000 | Not observed through 5000 | 81952 |
| drift | Not observed through 5000 | Not observed through 5000 | 81952 |
| korvet-244 | Not observed through 5000 | Not observed through 5000 | 81952 |
| korvet-700 | 1000 | 2500 | 81952 |
| korvet-drift | Not observed through 5000 | Not observed through 5000 | 81952 |

**Estimate, not a measurement:** the small Korvet stream saves about 11.14
block bytes per entry before the fixed registry allocation. Extrapolating that
slope gives 81952/11.14 ≈ 7357 entries for stream-memory break-even.
No stream longer than 5000 entries was run. Allocator effects, dictionary fit,
block boundaries and traffic changes can invalidate this extrapolation. No
production retention/capacity claim follows from it.

### Total Redis memory and append timing

| Workload | Mode | Total Redis KiB | Dataset B/entry | XADD ops/s | p50 / p95 / p99 µs |
|---|---|---:|---:|---:|---|
| small-json | baseline | 1544.5 | 106.8 | 87608 | 11.1 / 13.9 / 18.0 |
| small-json | zstd | 1140.9 | 24.2 | 86095 | 11.2 / 14.8 / 20.2 |
| small-json | dictionary | 1133.9 | 22.8 | 86241 | 11.1 / 14.7 / 22.3 |
| schema | baseline | 1862.3 | 171.9 | 88316 | 11.1 / 13.5 / 17.5 |
| schema | zstd | 1246.1 | 45.8 | 84553 | 11.2 / 17.5 / 24.6 |
| schema | dictionary | 1326.2 | 62.1 | 67294 | 11.2 / 18.0 / 106.1 |
| native | baseline | 1273.9 | 51.4 | 78550 | 12.5 / 14.8 / 18.2 |
| native | zstd | 1151.0 | 26.3 | 77714 | 12.3 / 15.2 / 24.2 |
| native | dictionary | 1255.5 | 47.7 | 71498 | 12.2 / 15.1 / 42.1 |
| large | baseline | 51304.6 | 10297.7 | 39634 | 25.2 / 28.5 / 33.0 |
| large | zstd | 2588.5 | 320.7 | 31874 | 30.7 / 35.1 / 39.0 |
| large | dictionary | 2185.6 | 238.2 | 11141 | 89.1 / 95.6 / 103.0 |
| entropy | baseline | 7808.8 | 1389.8 | 82348 | 11.7 / 14.9 / 19.7 |
| entropy | zstd | 7832.9 | 1394.7 | 67796 | 12.4 / 24.4 / 27.1 |
| entropy | dictionary | 7898.7 | 1408.2 | 17164 | 12.7 / 164.0 / 185.8 |
| drift | baseline | 3151.5 | 436.0 | 86333 | 11.2 / 14.0 / 18.9 |
| drift | zstd | 1918.9 | 183.5 | 74651 | 11.5 / 28.4 / 30.9 |
| drift | dictionary | 2103.0 | 221.2 | 45857 | 11.6 / 108.1 / 119.8 |
| korvet-244 | baseline | 2421.2 | 286.4 | 85604 | 11.3 / 14.4 / 19.0 |
| korvet-244 | zstd | 1446.3 | 86.7 | 83037 | 11.1 / 21.2 / 25.2 |
| korvet-244 | dictionary | 1484.6 | 94.6 | 56734 | 11.4 / 94.7 / 113.7 |
| korvet-700 | baseline | 5112.6 | 837.6 | 83479 | 11.6 / 15.0 / 20.5 |
| korvet-700 | zstd | 2107.8 | 222.2 | 69870 | 11.8 / 25.6 / 29.2 |
| korvet-700 | dictionary | 1817.9 | 162.8 | 33647 | 12.0 / 104.9 / 113.9 |
| korvet-drift | baseline | 3431.5 | 493.3 | 86561 | 11.2 / 13.8 / 17.4 |
| korvet-drift | zstd | 2019.1 | 204.1 | 71666 | 11.6 / 28.7 / 32.5 |
| korvet-drift | dictionary | 2162.8 | 233.5 | 43208 | 11.4 / 112.5 / 124.8 |

For ~702-byte events, dictionary mode roughly halves measured XADD throughput
(69.9k → 33.6k ops/s) and increases the median run p99 from 29.2 to 113.9 µs.
The implementation digests the dictionary into a newly allocated context at
each rollover; cached prepared dictionaries/contexts could change this tradeoff,
but their retained memory and latency impact have not been measured here.

### Reads and consumer groups on Korvet shapes

Serial round trips include Python parsing. Large pages are 1000 entries:

| Workload | Mode | XRANGE 10 ops/s | XRANGE 1000 entries/s | XREAD 10 ops/s | XREAD 1000 entries/s |
|---|---|---:|---:|---:|---:|
| korvet-244 | baseline | 34848 | 622708 | 34799 | 624205 |
| korvet-244 | zstd | 26073 | 530561 | 26826 | 536829 |
| korvet-244 | dictionary | 24368 | 488999 | 23521 | 485457 |
| korvet-700 | baseline | 34198 | 547662 | 32671 | 562317 |
| korvet-700 | zstd | 22970 | 369176 | 21101 | 365959 |
| korvet-700 | dictionary | 17347 | 280437 | 16760 | 286998 |
| korvet-drift | baseline | 34744 | 627076 | 33991 | 623450 |
| korvet-drift | zstd | 24387 | 538295 | 22653 | 538926 |
| korvet-drift | dictionary | 19604 | 485348 | 18842 | 488204 |

Group operations on ~702-byte events, ops/s / p99 µs (all workloads in JSON):

| Mode | Delivery | Pending reread | XCLAIM | XAUTOCLAIM |
|---|---:|---:|---:|---:|
| baseline | 27498 / 50.9 | 32217 / 37.7 | 27690 / 59.1 | 31364 / 54.8 |
| zstd | 19222 / 71.4 | 12114 / 105.1 | 8150 / 159.7 | 23936 / 59.6 |
| dictionary | 16091 / 79.5 | 8754 / 154.6 | 5208 / 220.3 | 20393 / 67.0 |

### Persistence, deletion and scratch

Triplets are raw / ordinary Zstd / shared dictionary. RDB contains ordinary
listpacks, so this memory prototype does not reduce RDB format size:

| Workload | RDB KiB | Restart ms | Half trim µs | DEL stream µs | Lifetime peak RSS MiB |
|---|---|---|---|---|---|
| small-json | 59.7 / 59.7 / 59.7 | 45.6 / 43.9 / 44.2 | 29.7 / 109.1 / 372.0 | 16.4 / 16.9 / 16.4 | 7.5 / 7.2 / 7.2 |
| schema | 228.4 / 228.4 / 228.4 | 38.1 / 43.1 / 44.5 | 39.1 / 406.9 / 613.2 | 22.6 / 20.2 / 20.1 | 8.0 / 7.9 / 8.5 |
| native | 94.5 / 94.5 / 94.5 | 44.8 / 43.9 / 45.1 | 20.9 / 173.3 / 234.1 | 13.1 / 15.0 / 14.5 | 7.1 / 7.3 / 7.8 |
| large | 1752.0 / 1752.0 / 1752.0 | 52.3 / 52.0 / 53.1 | 518.4 / 7634.0 / 12299.8 | 297.6 / 130.0 / 138.8 | 103.4 / 104.7 / 105.4 |
| entropy | 5197.6 / 5197.6 / 5197.6 | 50.3 / 51.3 / 51.1 | 169.0 / 179.8 / 182.7 | 60.0 / 66.0 / 98.8 | 24.4 / 24.8 / 25.9 |
| drift | 1146.6 / 1146.6 / 1146.6 | 49.4 / 46.6 / 46.8 | 35.9 / 394.2 / 604.5 | 36.8 / 36.7 / 91.0 | 12.0 / 11.2 / 12.0 |
| korvet-244 | 476.6 / 476.6 / 476.6 | 45.4 / 47.0 / 49.5 | 45.2 / 701.2 / 1096.9 | 20.9 / 25.5 / 25.3 | 9.8 / 9.1 / 10.2 |
| korvet-700 | 1594.2 / 1594.2 / 1594.2 | 46.6 / 47.1 / 46.0 | 89.9 / 2318.2 / 4574.5 | 35.1 / 47.6 / 42.5 | 17.4 / 14.9 / 15.1 |
| korvet-drift | 1270.9 / 1270.9 / 1270.9 | 45.2 / 47.1 / 49.0 | 46.5 / 692.8 / 1155.5 | 34.7 / 59.9 / 76.2 | 12.9 / 11.8 / 12.5 |

Measured codec heap/buffer peaks per operation, ordinary Zstd / dictionary.
Stored input, dictionary registry, stack and replies are excluded (see definitions
above); whole-command peaks can be larger with nested iterators and replies:

| Workload | Compression scratch KiB | Decompression scratch KiB |
|---|---|---|
| small-json | 107.0 / 87.2 | 100.0 / 100.0 |
| schema | 155.0 / 619.0 | 101.0 / 101.0 |
| native | 89.5 / 553.5 | 99.0 / 99.0 |
| large | 272.0 / 736.0 | 106.0 / 106.0 |
| entropy | 89.5 / 553.5 | 0.0 / 0.0 |
| drift | 155.0 / 619.0 | 101.0 / 101.0 |
| korvet-244 | 107.0 / 571.0 | 100.0 / 100.0 |
| korvet-700 | 106.0 / 570.0 | 100.0 / 100.0 |
| korvet-drift | 107.0 / 571.0 | 100.0 / 100.0 |

The small fixture full-synchronized 161 entries in a median 27.9 ms,
then verified configuration rotation, append, trim/delete and pending metadata.
Additional 34-entry regressions verify registry-cap replay fallback for an
already-online replica and incremental AOF under both base-file modes. A separate
reviewer independently reproduced dictionary-ID reuse/collision fallback for
live replication and plain AOF, checking every byte and enabled=0.

AOF fixture medians: plain AOF 132959 bytes / 37.3 ms restart;
RDB-preamble AOF 9161 bytes / 51.0 ms restart.
The fixtures grow between iterations, so these are correctness observations,
not a same-dataset comparison of the two formats. Large AOF throughput,
replication lag under sustained load and failover remain unmeasured.

### Decision and limitations

Shared dictionary compression **does provide additional savings** for the
homogeneous ~702-byte Korvet events and the large-record workload. It does not
uniformly beat ordinary block Zstd: small streams pay fixed dictionary cost,
some native/value-training distributions fit poorly, and schema drift can
remove the benefit. Training here uses raw record values rather than encoded
listpacks; training directly on representative listpacks is untested.

The earlier embedded-dictionary result was not evidence against dictionaries in
general. This shared experiment reproduces Korvet’s per-record gain and shows
where it translates into incremental savings beyond block compression. Its
current synchronous context/dictionary preparation cost is substantial. Further
work on prepared-context caching must include its memory footprint, lifecycle
and sustained tail latency before any production/default-setting decision.

### Validation and local review

- Zstd-enabled `-Werror` build and all 19 C codec/lifecycle tests passed: checksums,
  truncation, length bounds, missing dictionaries, version retention/collection,
  cap rejection/reclamation, ID collision and expansion fallback.
- Full affected stream, consumer-group and module-stream suites passed; the
  focused compressed suite passed again after replay handling changed.
- The benchmark correctness checks passed in each normalized sweep: rotation,
  independent COPY ownership/source deletion, RDB, incremental/rewrite AOF,
  full/live replication, groups and cap/no-op-trim divergence.
- Non-Zstd t_stream.c syntax compilation and the repository reply-schema linter
  passed. A complete non-Zstd build and active-defrag execution were not run.
- Separate read-only review found missing XINFO reply schemas and a physical-
  state-dependent cap/collision replay failure. Both were fixed and rechecked;
  the reviewer’s independent collision reproduction passed. No verified
  correctness finding remained. Registry allocations are not defragmented.
- Harness query-buffer normalization was checked after a direct measurement
  showed dictionary installation retaining a larger connection buffer; final
  three-run measurements use fresh identical data connections in every mode.
- `git diff --check` passed. No commit, push, PR or deployment was made;
  the pre-existing untracked deps/fast_float directory was preserved.

To repeat the statistical summary, run the command above three times with
different output files and take the median of each statistic. The archived
summary retains all three original outputs under `runs`. Results are a bounded
synthetic experiment, not production capacity estimates.
