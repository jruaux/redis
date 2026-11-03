# Plain block Zstd: sustained CPU, latency and persistence

This is the current plain-Zstd report. Dictionary code and prior experimental
results are preserved, but dictionaries are not trained, configured or measured
in this experiment. The reference is unmodified upstream Redis at
`68d9a2df09ec7d4a5c03abe7c1424ad26e896b5d`.

## Integration and persistence

Compression remains opt-in per stream:

```text
XADD events * field value
XCFGSET events COMPRESSION ZSTD
```

Redis compresses each sealed listpack as one unit. The active append block
stays raw. Defaults are 4096 uncompressed listpack bytes / 100 entries, with
the usual large-entry exception. A codec operation accepts at most 64 KiB of
decoded data; larger blocks stay raw. Zstd level 3, checksums, fallback when
compression expands storage, iterator scratch, and materialization on mutation
are unchanged. Applications receive the original fields/values and IDs.

Plain-Zstd policy now survives RDB, DUMP/RESTORE, RDB-preamble AOF and full
replication synchronization. An experimental **RDB object type 37** stores
policy version 1 and level 3 before the existing stream-v5 body. The body
contains ordinary listpacks, so existing stream validation and metadata formats
are reused. Loading validates the policy and listpacks, then recompresses every
eligible block except the final append block. This restores memory savings and
future append behavior; it costs load-time CPU. No dictionary is involved.

Plain AOF rewrite emits XCFGSET after the first reconstructed XADD, enabling
compression while the remaining historical entries replay. Empty streams get
their policy after their normal empty-stream reconstruction. Existing incremental
AOF already records the configuration command. Consumer groups, pending entries,
IDs and tombstones retain the existing logical persistence behavior.

Type 37 is a **private prototype extension**, not an upstream allocation. Files
containing it require this Zstd-enabled build, including its redis-check-rdb.
Older servers and builds without Zstd reject it. RDB header version remains 17;
streams with compression OFF keep the upstream type 27. This is not mixed-version
replication compatibility. To export for upstream, disable compression and
resave/rewrite. Dictionary-policy persistence is unchanged and outside this work.

The RDB file is still compressed using Redis's normal RDB machinery, not stored
Zstd envelopes; its size therefore does not gain the in-memory Zstd ratio.
Restart recompression scans loaded blocks once. Per-block codec work is bounded,
but whole-stream restart/RESTORE cost is linear in dataset size.

## Build, correctness and reproduction

```sh
make -C src -j4 BUILD_COMPRESSION=yes MALLOC=libc OPT=-O2 REDIS_CFLAGS='-Werror -DREDIS_TEST' all
src/redis-server test stream-compression
./runtest --compression --single unit/type/stream-compression --single unit/type/stream --single unit/type/stream-cgroups --single unit/moduleapi/stream --clients 4 --timeout 180

git archive --output=/tmp/redis-baseline.tar 68d9a2df0
mkdir -p /tmp/redis-baseline
tar -xf /tmp/redis-baseline.tar -C /tmp/redis-baseline
make -C /tmp/redis-baseline/src -j4 BUILD_COMPRESSION=yes MALLOC=libc OPT=-O2 REDIS_CFLAGS='-Werror -DREDIS_TEST' all

python3 tools/stream-plain-bench.py --baseline-server /tmp/redis-baseline/src/redis-server --output /tmp/stream-plain-results.json
```

The Python script compiles the native driver using repository hiredis and
HdrHistogram libraries. It launches only disposable Redis servers in temporary
directories. No existing instance, user data or infrastructure is accessed.
`--check-only` runs policy tests without sustained load. A bounded readiness
smoke test can use `--duration .2 --repetitions 1 --workloads korvet-700 large`.

## Approved benchmark bounds

- Six workloads, three modes (unmodified, prototype OFF, plain Zstd), two passes.
- 10000 retained entries; four native client threads; pipeline depth eight.
- Five phases per case, five seconds each; 900 seconds of nominal timed load.
- 256 MiB Redis maxmemory/noeviction per process; at most two servers during
  correctness replication checks and one server during load measurements.
- 1200-second overall limit, 35-second native-driver subprocess timeout, bounded
  startup/AOF/replication waits. Expected wall duration was 16–19 minutes.
- Large read pages contain at most approximately 1 MiB of payload per response;
  the page count is 1000 for most workloads and lower for ~8 KiB records.

Redis maxmemory is not an OS RSS cap. The driver caps command-file storage at
128 MiB, uses four bounded pipelines, ten-second socket timeouts and HDR
histograms instead of retaining one sample per operation.

The six corpora use the prior representative generators: Korvet-style ~258-byte
logs, logs padded to ~702 bytes, native ordered fields with repeated names and
an empty value, repetitive ~8 KiB records, zlib-compressed pseudorandom values,
and mixed-schema log/telemetry records. These are synthetic shapes, not customer
data. Writers cycle a fixed 10000-record corpus; values are not generated afresh
on each append. The mixed corpus repeats, so it is not a one-way temporal drift
experiment. All three modes use the same corpus. The complete Kafka broker's
extra key/timestamp fields are not added to the one-value log workloads.

## Phases and measurement definitions

Each phase starts from a freshly populated 10000-entry stream. Every initial
record is verified byte-for-byte. The load driver runs after setup/validation,
so their CPU/time are excluded from the phase metrics. Configuration and loading
connections are replaced before timing, avoiding retained installation/query
buffers. The second pass reverses mode order to reduce fixed-order bias.

| Phase | Commands and retention |
|---|---|
| write-retain | Continuous XADD MAXLEN ~ 10000; includes approximate retention work |
| small-reads | Alternating XRANGE and XREAD, COUNT 10, varied offsets, static dataset |
| large-reads | Alternating XRANGE and XREAD, up to COUNT 1000, varied offsets, static dataset |
| approx-trim | Seven plain XADDs followed by XTRIM MAXLEN ~ 10000 per pipeline |
| exact-trim | Seven plain XADDs followed by XTRIM MAXLEN = 10000 per pipeline |

"Write" is intentionally bounded append-plus-retention, not an indefinitely
growing stream. Trim phases are mixed append/trim workloads; their process CPU
cannot be attributed entirely to XTRIM. The driver also reports XTRIM's own
command-processing time and latency separately. Approximate trim does not
necessarily delete entries on every call. Each phase checks retained length
and verifies original or newly written field/value bytes after the timed work.

**CPU:** the driver snapshots Redis INFO CPU immediately before releasing ready
workers and after joining/draining them. User+system CPU is the Redis process's
CPU, not client CPU. Reported µs/op divides that delta by completed commands;
µs/entry divides by delivered/read/appended entries. CPU percent is expressed
relative to one core (100%=one full CPU core), not all 14 host cores.

**Latency:** per-command enqueue-to-parsed-response time over Unix sockets,
including pipeline queuing, transport and hiredis parsing. HDR precision is
three significant digits; values are rounded up to microseconds. p50/p95/p99
at fixed four-client concurrency describe a loaded system, not isolated service
time or a constant offered arrival rate. This is a closed-loop load generator;
there is no coordinated-omission correction or open-loop SLO claim.

**Service time:** INFO commandstats gives Redis command wall time, reported as
µs/call. It excludes client queuing and transport, but it is not a per-command
CPU measurement and can include OS preemption. Process CPU and command wall time
are deliberately separate fields in the JSON.

**Throughput:** completed commands/second, plus entries/second. Trim phases have
seven appends per eight commands; read entries/second accounts for page size.
Measured throughput is specific to this machine, protocol/retention mix and
corpus. It is not production or cluster capacity.

**Memory:** MEMORY USAGE and Redis used_memory before/after phases, with actual
retained length. The codec scratch counters retain their prior heap/buffer
definition; native context allocation, checksum, deep listpack validation and
materialization are part of the measured implementation cost. The driver does
not isolate Zstd algorithm time from these other costs. Process peak RSS spans
setup, validation, SAVE/restart and all phases of a case, rather than one phase.

**Persistence:** every case separately SAVEs the 10000-entry corpus and restarts,
then verifies all bytes and plain policy. SAVE CPU is the process delta during
SAVE; restart CPU includes Redis startup plus loading, not loading alone. File
size and loaded stream memory are recorded. The smaller policy correctness
fixture also verifies consumer pending metadata, full sync and both AOF formats.

## Results

The approved run completed **180 phase measurements in 939.1 seconds
(15.65 minutes)**, below the 1200-second limit. All commands
completed without Redis/driver errors; commandstats call counts match native
histogram counts. Hardware: Apple M4 Max, 14 logical CPUs, 36 GiB RAM, macOS
26.7.1 arm64; libc, Clang -O2, Zstd 1.5.7. CPU placement was not pinned.
Persistence measurements used a warm filesystem: SAVE followed by immediate
restart. These are not cold-disk recovery times.

Full raw measurements, native/server hashes and a derived comparison summary:
[stream-plain-results.json](stream-plain-results.json). CPU and throughput below
pool counts/time from the two runs. Latency cells give the **range of the two
per-run percentiles**, not a pooled percentile or confidence interval.

### Memory savings on an identical corpus

Use the initial 10000-entry corpus, not the last retention window: four cyclic
writers can finish at different corpus positions, especially for mixed schemas.
The JSON retains ending memory as an observation, but those mixed-schema
windows are not an apples-to-apples memory comparison.

| Workload | Upstream B/entry | Zstd B/entry | Stream memory saved |
|---|---:|---:|---:|
| korvet-244 | 274.2 | 67.5 | 75.4% |
| korvet-700 | 726.8 | 196.3 | 73.0% |
| native | 30.0 | 10.0 | 66.8% |
| large | 8714.0 | 292.0 | 96.6% |
| entropy | 1068.2 | 1068.2 | -0.0% |
| korvet-drift | 475.8 | 172.8 | 63.7% |

### Sustained appends with approximate retention

Each XADD includes MAXLEN ~ 10000. CPU figures are Redis user+system CPU per
completed XADD; loaded p99 includes queuing at four clients/pipeline eight.

| Workload | CPU µs/XADD upstream | OFF | Zstd | Extra CPU | XADD/s upstream → Zstd | p99 µs upstream → Zstd |
|---|---:|---:|---:|---:|---|---|
| korvet-244 | 1.37 | 1.36 | 2.78 | +102% | 725461 → 359899 | 59–64 → 106–109 |
| korvet-700 | 1.47 | 1.51 | 5.85 | +297% | 678186 → 170593 | 58–59 → 222–237 |
| native | 1.83 | 1.85 | 2.11 | +15% | 544630 → 473389 | 69–81 → 103–105 |
| large | 4.94 | 4.96 | 20.96 | +324% | 202214 → 47579 | 179–195 → 692–736 |
| entropy | 2.11 | 2.05 | 4.58 | +117% | 458675 → 217438 | 77–86 → 165–172 |
| korvet-drift | 1.42 | 1.44 | 4.58 | +222% | 701221 → 217824 | 55–66 → 200–220 |

OFF is close to upstream for these append cases (approximately -3% to +3% CPU
variation). Thus most measured extra CPU comes from enabled compression, not
the presence of the prototype in the binary.

At ~702 bytes/record, memory falls 73%, but CPU/XADD rises from **1.47 to 5.85 µs**
(+4.38 µs, approximately 4x). Throughput falls about 75%, and loaded p99 rises
from 58–59 to 222–237 µs. Small logs cost about 2x CPU; native multi-field
entries amortize compression over more entries and cost about 15% extra CPU.
Large records save 96.6% stream memory but cost about 4.2x CPU per retained append.
High-entropy values save no memory while still costing about 2.2x write CPU.

### Sustained reads

Both commands alternate within each read phase. CPU/op is the process cost
averaged over that mix, not independently attributed XRANGE/XREAD CPU. Their
client latency histograms are separate. Page size is the same across modes:

| Workload | Page | CPU µs/op upstream / OFF / Zstd | Extra CPU | ops/s upstream → Zstd | XRANGE p99 µs upstream → Zstd | XREAD p99 µs upstream → Zstd |
|---|---:|---|---:|---|---|---|
| korvet-244 | 10 | 4.66 / 4.61 / 11.99 | +158% | 181205 → 77956 | 201–213 → 443–502 | 201–213 → 443–502 |
| korvet-244 | 1000 | 303.06 / 280.97 / 595.54 | +97% | 2873 → 1568 | 11847–13583 → 21087–25423 | 11855–13655 → 21039–25471 |
| korvet-700 | 10 | 5.85 / 5.95 / 20.45 | +250% | 163050 → 48175 | 248–255 → 881–884 | 247–255 → 871–888 |
| korvet-700 | 1000 | 444.09 / 414.89 / 1408.80 | +217% | 2151 → 700 | 14775–18015 → 51775–53887 | 14951–18127 → 52991–54943 |
| native | 10 | 7.81 / 7.81 / 15.23 | +95% | 121843 → 64820 | 313–322 → 572–583 | 312–322 → 571–582 |
| native | 1000 | 456.91 / 459.41 / 537.90 | +18% | 1477 → 1319 | 23071–25519 → 26751–27247 | 23055–25471 → 27007–27295 |
| large | 10 | 33.02 / 33.44 / 72.82 | +121% | 30016 → 13657 | 1423–1505 → 2641–3181 | 1428–1515 → 2653–3175 |
| large | 119 | 401.90 / 413.66 / 850.83 | +112% | 2473 → 1170 | 13391–17775 → 30207–32015 | 13391–17519 → 30239–33023 |
| entropy | 10 | 7.25 / 6.87 / 7.81 | +8% | 132380 → 122784 | 289–324 → 318–325 | 289–324 → 319–324 |
| entropy | 901 | 531.58 / 539.03 / 569.84 | +7% | 1839 → 1715 | 18751–20975 → 20383–21759 | 18367–20815 → 20511–21647 |
| korvet-drift | 10 | 5.19 / 5.13 / 16.89 | +225% | 178015 → 57709 | 229–257 → 729–863 | 229–258 → 730–868 |
| korvet-drift | 1000 | 333.10 / 357.45 / 996.19 | +199% | 2799 → 980 | 14999–15335 → 43679–46431 | 14855–15135 → 45119–46271 |

For ~702-byte values, ten-entry pages cost about **3.5x process CPU** and
large pages about **3.2x**. Native multi-field reads show the page-size tradeoff:
small pages cost about 95% extra CPU, while 1000-entry scans cost about 18% extra.
A small request can decode and validate a whole block containing more records
than it returns. High-entropy blocks remain raw and avoid most decode cost.

### Continuous appends plus trimming

Each pipeline has seven XADDs and one XTRIM. The first CPU column is the
**whole mixed phase** per command, including appends; the second is measured
**XTRIM processing wall time**, not isolated trim CPU. XTRIM client p99 includes
the preceding pipeline work. Neither column should be called pure trim CPU.

| Workload | Trim | Mixed CPU µs/op upstream → Zstd | Extra CPU | XTRIM processing µs/call upstream → Zstd | XTRIM p99 µs upstream → Zstd |
|---|---|---|---:|---|---|
| korvet-244 | approx-trim | 1.06 → 2.08 | +96% | 0.32 → 3.15 | 48–63 → 80–95 |
| korvet-244 | exact-trim | 1.13 → 2.17 | +93% | 0.91 → 3.77 | 60 → 82–97 |
| korvet-700 | approx-trim | 1.12 → 4.28 | +282% | 0.58 → 8.89 | 47–56 → 157–164 |
| korvet-700 | exact-trim | 1.14 → 4.29 | +276% | 0.77 → 9.05 | 49–59 → 160–162 |
| native | approx-trim | 1.43 → 1.62 | +14% | 0.15 → 0.74 | 56–57 → 78–79 |
| native | exact-trim | 2.00 → 2.20 | +10% | 4.80 → 5.43 | 81 → 95–98 |
| large | approx-trim | 4.52 → 14.46 | +220% | 2.10 → 30.90 | 185–240 → 520 |
| large | exact-trim | 4.38 → 14.44 | +230% | 1.97 → 30.83 | 173–211 → 511–517 |
| entropy | approx-trim | 1.18 → 3.30 | +179% | 0.76 → 0.87 | 51–57 → 133–143 |
| entropy | exact-trim | 1.16 → 3.32 | +186% | 0.83 → 0.95 | 49–50 → 130–145 |
| korvet-drift | approx-trim | 1.05 → 3.41 | +225% | 0.42 → 6.33 | 47–49 → 149–150 |
| korvet-drift | exact-trim | 1.08 → 3.45 | +218% | 0.81 → 6.70 | 48 → 152–153 |

Whole-block retention is not decode-free in this prototype: trim needs
listpack metadata and materializes visited blocks; first-ID bookkeeping can
also decode the new head. Thus the result is more expensive than measuring
compression alone on an append-only growing stream. No codec-only CPU profile
or unbounded-write capacity estimate is claimed.

### SAVE and restored compression

Values average the two cases. Startup CPU includes startup+load. Snapshot and
reload verify the exact 10000-entry corpus, and plain policy remains enabled.
RDB size triplets are upstream / OFF / Zstd; the extra two bytes carry policy:

| Workload | RDB bytes upstream / OFF / Zstd | SAVE CPU ms upstream → Zstd | Startup+load CPU ms upstream → Zstd | Restart wall ms upstream → Zstd |
|---|---|---|---|---|
| korvet-244 | 934816 / 934816 / 934818 | 5.86 → 10.41 | 7.33 → 14.42 | 45.0 → 45.7 |
| korvet-700 | 3226601 / 3226601 / 3226603 | 14.10 → 28.14 | 10.92 → 33.47 | 49.8 → 71.5 |
| native | 152269 / 152269 / 152271 | 1.42 → 2.27 | 6.33 → 6.77 | 35.1 → 29.5 |
| large | 3546837 / 3546837 / 3546839 | 38.20 → 75.06 | 20.00 → 98.37 | 62.0 → 125.5 |
| entropy | 10603483 / 10603483 / 10603485 | 44.95 → 36.30 | 12.54 → 36.96 | 48.0 → 68.6 |
| korvet-drift | 2561490 / 2561490 / 2561492 | 15.73 → 23.57 | 9.83 → 26.61 | 39.1 → 52.9 |

At ~702 bytes, restoring compressed memory costs about 33.5 ms CPU including
startup, versus 10.9 ms for upstream. The ~8 KiB workload costs about 98.4 ms
versus 20.0 ms. High-entropy startup also attempts compression and then falls
back to raw; it gains no memory. This is the cost of preserving policy by
recompressing ordinary saved listpacks rather than persisting memory frames.

### Verdict

**This level-3/4-KiB implementation is a substantial memory/CPU tradeoff, not a
generally cheap optimization.** It can make sense for memory-constrained streams
with CPU and latency headroom, particularly storage-heavy, less frequently read
data. It should not be enabled by default for high-throughput/latency-sensitive
streams based on these results, and it is a poor fit for already-compressed
values. The earlier serial Python experiment understated capacity overhead
because the client limited command rate; this native test exposes more server
cost. These statements apply to this prototype and workload mix, not all Zstd
implementations or production capacity.

CPU includes fresh codec contexts, checksums, deep listpack validation and
decode/materialization for retention. Reusing contexts, avoiding unnecessary
head-block decoding and tuning level/block size are follow-up hypotheses,
**not measured improvements**. They may materially change the tradeoff and
need their own memory, lifecycle and sustained-tail measurements.

## Review and remaining limitations

- Zstd-enabled `-Werror` build, 19 codec/lifecycle tests and the affected stream,
  group and module suites passed. The focused test now checks policy and memory
  after DUMP/RESTORE and RDB reload.
- Policy tests cover RDB and redis-check-rdb, invalid policy version/level,
  restart, consumer pending entries, full/live replication, incremental AOF and
  rewritten plain/RDB-preamble AOF. Both AOF base prefixes are checked explicitly.
- A separate read-only reviewer inspected persistence, ownership and native
  measurement code. One AOF fixture coverage finding was fixed: setting the
  preamble flag on an already enabled AOF did not rewrite its base; the fixture
  now explicitly rewrites and verifies each base format. No verified correctness
  or measurement finding remained before sustained timing.
- The generic DUMP, AOF-rewrite and RDB integration suites also passed with
  slow-tagged cases excluded. `git diff --check` passed.
- Non-Zstd syntax compilation of the changed stream/RDB code passed with the
  repository's warning suppressions; a complete non-Zstd build was not run.
- Context reuse, prepared dictionaries, larger block tuning and compression
  levels other than 3 are not evaluated. Corrupt internal envelopes still
  fail-stop rather than returning a recoverable command error.
- No active-defrag, eviction/lazyfree stress, cluster migration, failover,
  partial-resynchronization stress or real customer payload study was added.
- The inherited upstream plain-AOF behavior for pending references to deleted
  records remains unchanged; the AOF fixture uses live pending entries.
- Earlier dictionary code and results are retained as historical experiments;
  plain Zstd is the current prototype direction. The pre-existing untracked
  deps/fast_float directory is outside this change.
