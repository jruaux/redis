# Transparent block compression for Redis Streams

This experimental Redis server fork compresses retained stream data with plain
Zstandard. Applications keep using ordinary `XADD`, `XRANGE`, `XREAD`, and
consumer-group commands and receive the original bytes and entry IDs.

**Result:** substantial stream-memory savings, with substantial CPU and latency
costs in this implementation. It is most promising for memory-constrained,
infrequently read retained data with CPU headroom. These experiments do not
justify enabling it by default.

## Concept: compress blocks of entries

Redis Streams store entries in listpacks, indexed by entry ID in a radix tree.
This prototype compresses a sealed listpack as one unit; it does not compress
each field, payload, or entry separately.

```text
stream radix tree
  ├── older block: Zstd(listpack containing multiple entries)
  ├── older block: Zstd(listpack containing multiple entries)
  └── active append block: ordinary uncompressed listpack
```

Zstd can match repeated text across neighboring records within a block, such as
JSON field names, logger names, and common message fragments. Each block is
independent; it does not borrow history from other blocks.

- The active append block stays uncompressed. When an append seals it, Redis
  attempts compression once, avoiding recompressing the growing block on every
  append. Enabling compression does not backfill existing blocks.
- Default block limits are 4 KiB / 100 entries, with Redis's usual large-entry
  exception. Codec units are capped at 64 KiB; larger blocks stay raw.
- Zstd uses level 3 and checksums. A block stays raw if compression would not
  reduce its allocation, including the envelope.
- The envelope records encoding, original length, and dictionary identity
  (zero for plain Zstd). Decoding checks framing, bounds, checksum, exact length,
  and stream-listpack integrity.
- Reads decompress into iterator-owned scratch. A small page may decode more
  entries than it returns. Mutations materialize affected blocks; this prototype
  also performs avoidable decoding during trimming.

Field/value bytes, field order, repeated and empty fields, IDs, tombstones, and
consumer-group metadata retain their logical semantics. Compression is opt-in;
unconfigured streams keep the existing storage path.

## Build and use

Requires the existing Redis build prerequisites and libzstd. The measured build
used Zstd 1.5.7.

```sh
make -C src -j4 BUILD_COMPRESSION=yes MALLOC=libc OPT=-O2 REDIS_CFLAGS='-Werror -DREDIS_TEST' all
```

Create the stream before configuring it:

```redis
XADD events * message "first event"
XCFGSET events COMPRESSION ZSTD
XADD events * message "next event"
XRANGE events - +
XINFO STREAM events
XCFGSET events COMPRESSION OFF
```

`OFF` stops future compression; existing compressed blocks remain readable.
The level is fixed at 3. These compression options are prototype extensions,
not upstream Redis command options.

## Results: memory, producer, and consumer cost

The sustained experiment compared unmodified Redis, prototype OFF, and plain
Zstd on identical initial corpora. Compression factor is **upstream stream
memory divided by compressed stream memory**: 4× means one quarter of the
memory. It includes stream allocations and compression metadata, not total
Redis process memory. Whole-process measurements are retained in the raw results.

All arrows show **upstream → Zstd**. Throughput is rounded; `k` means thousand.
Exact bytes per entry and full measurements are in the [detailed report](STREAM-PLAIN-ZSTD.md#memory-savings-on-an-identical-corpus).

| Workload | Compression | CPU/XADD, µs | XADD/s | Reads/s, 10 entries | Reads/s, large page |
| --- | ---: | ---: | ---: | ---: | ---: |
| ~258 B logs | 4.1× | 1.37 → 2.78 | **725k → 360k** | 181k → 78k | 2.9k → 1.6k |
| ~702 B logs | 3.7× | 1.47 → 5.85 | **678k → 171k** | 163k → 48k | 2.2k → 700 |
| Native fields | 3.0× | 1.83 → 2.11 | **545k → 473k** | 122k → 65k | 1.5k → 1.3k |
| ~8 KiB repetitive | 29.8× | 4.94 → 20.96 | **202k → 48k** | 30k → 14k | 2.5k → 1.2k |
| Already compressed | 1.0× | 2.11 → 4.58 | **459k → 217k** | 132k → 123k | 1.8k → 1.7k |
| Mixed schemas | 2.8× | 1.42 → 4.58 | **701k → 218k** | 178k → 58k | 2.8k → 980 |

Writes include approximate retention (`XADD MAXLEN ~ 10000`), so these are
append-plus-retention costs, not isolated codec costs. For ~702-byte logs,
loaded XADD p99 increased from 58–59 to 222–237 µs. Prototype OFF was close to
upstream for append CPU, with roughly −3% to +3% variation; that does not prove
zero overhead for every operation.

Reads alternate `XRANGE` and `XREAD`; throughput counts requests, not entries.
Large pages contain 1,000 entries, except ~8 KiB records (119) and already
compressed values (901), to bound response size.
For ~702-byte logs, ten-entry read CPU rose from 5.85 to 20.45 µs/request and
XREAD p99 from 0.25–0.26 to 0.87–0.89 ms. Large-page read CPU rose from
444 to 1,409 µs/request and XREAD p99 from 15–18 to 53–55 ms.

CPU is averaged over both read commands. Latency ranges are the two runs'
percentiles, not confidence intervals. Consumer-group delivery, pending rereads,
claiming, and acknowledgment have correctness coverage; sustained group/ACK
performance was not measured in this experiment. Earlier serial-client group
timings are in the historical report and are not comparable to this load test.

For the same log workload, approximate `XTRIM` processing time rose from 0.58
to 8.89 µs/call; exact trimming rose from 0.77 to 9.05 µs/call. These are
command-processing wall times from mixed append/trim phases, not isolated CPU.

Plain-mode policy survives RDB, DUMP/RESTORE, AOF, and replication. RDB saves
ordinary listpacks plus policy, then recompresses older blocks on load. For
~702-byte logs, RDB size stayed about 3.23 MB, while startup-plus-load CPU rose
from 10.92 to 33.47 ms and warm restart wall time from 49.8 to 71.5 ms.
In-memory compression factors therefore do not predict persistence size savings.

## How representative are these results?

These are synthetic data shapes, not customer datasets. The log generators
model repeated schemas with varying values; the ~702-byte logs include padding.
The large records contain substantial repeated text, making their 29.8× result
optimistic. The mixed-schema corpus cycles rather than modeling a one-way
production distribution shift. Writers reuse a fixed corpus.

Bounds: six workloads, three modes, 10,000 retained entries, four clients,
pipeline depth 8, five 5-second phases, two passes, and 256 MiB Redis maxmemory
per process. At most two disposable servers ran during replication checks;
timed load used one server. The run completed 180 phase measurements in
about 939 seconds under a 20-minute limit. Maxmemory is not an OS RSS cap.

Hardware: Apple M4 Max, 36 GiB RAM, macOS arm64, libc, Clang `-O2`;
CPU placement was not pinned. Restart measurements used a warm filesystem.
These results are measurements of this prototype, not production sizing estimates.

## Dictionaries and next steps

Earlier experiments compared plain Zstd with trained, stream-owned dictionary
generations using separate training and measurement data. Dictionaries improved
some homogeneous workloads, but added memory, CPU, and lifecycle complexity;
they did not establish a general advantage worth pursuing first. Block Zstd
already exploits neighboring records. The current direction is plain Zstd.

Context reuse, lower compression levels, and avoiding unnecessary trim decoding
could reduce costs. They remain unmeasured hypotheses. Current code creates a
codec context per block and deeply validates every decoded listpack.

## Validation and limitations

The Zstd-enabled `-Werror` build, 19 codec/lifecycle tests, affected stream/group/
module suites, and both correctness harnesses passed before PR publication.
An independent local review found no blocking correctness issues.

- RDB object type 37 is private to this prototype; the RDB header remains version
  17. Older and non-Zstd builds reject it. Mixed-version replication/AOF is
  unsupported. Dictionary-mode RDB policy persistence differs from plain mode.
- Compression and decompression execute on the main thread. Per-block bounds
  do not bound a whole scan, trim, or restart independently of dataset size.
- Corrupt internal envelopes fail-stop explicitly rather than returning a
  recoverable command error.
- Active-defrag, eviction/lazyfree stress, cluster migration, failover, and
  partial-resynchronization stress remain unverified. A complete non-Zstd build
  was not run locally.

## Detailed reports and reproduction

- [Current plain-Zstd report and reproducible commands](STREAM-PLAIN-ZSTD.md)
- [Raw sustained results](stream-plain-results.json)
- [Historical dictionary design, results, and limitations](STREAM-COMPRESSION.md)
- [Shared-dictionary raw results](stream-compression-shared-results.json)
- [Initial historical raw results](stream-compression-results.json)

To rerun focused tests on the built prototype:

```sh
src/redis-server test stream-compression
./runtest --compression --single unit/type/stream-compression --single unit/type/stream --single unit/type/stream-cgroups --single unit/moduleapi/stream --clients 4 --timeout 180
```
