# Filtering stream reads

`XRANGE`, `XREVRANGE`, and `XREAD` accept an optional `FILTER expression`.
The expression uses the same scalar language as vector sets, with selectors
resolved against the stream entry's native field/value pairs.

```text
XADD events * region eu amount 150
XRANGE events - + COUNT 100 FILTER '.region == "eu" and .amount > 100'
XREVRANGE events + - COUNT 100 FILTER '.amount > 100'
XREAD BLOCK 5000 FILTER '.region in ["eu", "apac"]' STREAMS events $
```

Place `FILTER` before `STREAMS` in `XREAD`. One predicate applies to every
requested stream. `XREADGROUP` does not support `FILTER`.

## Values and expressions

Selectors such as `.region` access top-level stream fields. The first occurrence
wins when an entry repeats a field name. Values are binary-safe strings, even
when listpack storage encodes a value as an integer. Returned entries retain all
original fields, their order, and their bytes.

The shared language supports comparisons, arithmetic, Boolean operators,
parentheses, array membership (`.region in ["eu", "us"]`), and substring matching
(`"error" in .message`). Numeric operators use double-precision conversion.
Two strings compare as strings with `==` and `!=`; mixed string/number equality
and numeric comparisons use the engine's existing conversion rules.

Compatibility with vector sets includes these potentially surprising rules:

- A missing selector rejects the entire expression. Boolean evaluation does not
  short-circuit, so `.region == "eu" or .missing == 1` rejects an entry even when
  `region` is `eu`.
- Failed numeric conversion becomes zero. For example, an `amount` value of
  `not-a-number` matches `.amount == 0`.
- Numeric conversions use doubles and do not guarantee exact large-integer
  comparisons. Compare identifier strings to quoted strings when appropriate.
- Selector names use the existing restricted syntax (letters, digits, `_`, `-`).
  Arbitrary binary field names cannot all be addressed by this syntax.
- Values containing JSON remain strings. `.order.amount` is not supported;
  nested objects and array paths are not traversed.

Invalid expressions, including expressions with embedded NUL bytes, produce an
error before scanning entries, including when the requested key does not exist.

## Read behavior, scan budget and continuation

Without `FILTER`, command syntax, replies and read limits are unchanged.
With `FILTER`, `MAXSCAN` limits entries examined, including rejected entries and
retained deleted entries. Its default is **1,000**. An explicit override must be
a positive signed 64-bit integer; `MAXSCAN` requires `FILTER`. There is no
unlimited setting. `XREADGROUP` supports neither option.

```text
XRANGE events - + FILTER '.amount > 100' MAXSCAN 2000
XREVRANGE events + - FILTER '.amount > 100' MAXSCAN 2000
XREAD BLOCK 5000 FILTER '.region == "eu"' MAXSCAN 2000 STREAMS events 0
```

Matching entries retain ID order, reversed for `XREVRANGE`. `COUNT` limits
matches and remains per-stream in `XREAD`. `MAXCOUNT` limits matches across
streams; `MAXSIZE` remains an approximate reply-byte threshold, allowing at
least one entry. Envelope metadata can exceed that threshold.

`MAXSCAN` is command-wide: multiple XREAD streams share one budget, in argument
order, and blocking wakeups consume the same remaining budget. On exhaustion,
even a blocking read returns immediately, including when there are no matches.
It never silently skips the unexamined remainder. A budget is an entry-work cap,
not a wall-clock deadline: entry sizes, expression complexity, field lookup,
and iterator positioning still affect execution time. Explicitly large overrides
allow correspondingly more work on the server thread.

### Filtered replies

Using `FILTER` selects a new reply schema. Existing clients need explicit
support or a raw decoder. RESP3 replies use maps; RESP2 encodes each map as an
alternating key/value array. Entries keep their usual `[id, [field, value, ...]]`
structure. The following examples show logical maps:

```json
{
  "entries": [["2-0", ["amount", "150"]]],
  "last_scanned": "3-0",
  "status": "budget_exhausted"
}
```

`XRANGE` and `XREVRANGE` return `entries`, `last_scanned` and `status`:

- `exhausted`: reached the end of the requested range.
- `count_reached`: reached the matching result limit (`COUNT 0` returns no entries).
- `budget_exhausted`: consumed the scan budget; more entries may remain.

`last_scanned` is the last examined ID, even when that entry was rejected or
deleted. It is null if no entry was examined, including absent keys and empty
ranges. Resume `XRANGE` with `(<last_scanned>` as the start, preserving the end;
resume `XREVRANGE` with `(<last_scanned>` as the first, upper bound, preserving
the lower bound. Do not resume using the last matching ID.

```json
{
  "streams": [
    {"key": "orders", "entries": [["2-0", ["amount", "150"]]], "last_scanned": "3-0"},
    {"key": "payments", "entries": [], "last_scanned": "0-0"}
  ],
  "status": "budget_exhausted"
}
```

`XREAD` returns an ordered `streams` array containing **every requested stream**,
including absent, empty, unvisited and duplicate keys. Each result contains
`key`, `entries` and `last_scanned`. Unvisited streams retain their resolved
input ID. Resume by supplying all these IDs in the same order to `XREAD`.
`$` resolves to the current last-generated ID (or `0-0` for an absent stream);
`+` resolves to the predecessor of the last live entry, so that entry remains
eligible when the stream has not yet been visited.

XREAD statuses are:

- `caught_up`: completed the available scan without reaching a result or scan limit.
- `result_limit`: a `COUNT`, `MAXCOUNT` or `MAXSIZE` limit stopped result delivery.
- `budget_exhausted`: consumed the shared scan budget.
- `timeout`: a blocked read timed out, including `CLIENT UNBLOCK ... TIMEOUT`.

Scan-budget exhaustion takes precedence over a simultaneous result limit. It is
conservative at the exact boundary: no lookahead is performed, so a continuation
may return no entries. Similarly, a result-limit status does not promise more
entries exist. A filtered read with no matches still returns an envelope.

A blocking read with available budget waits for a match or the original timeout.
Rejected entries advance its internal positions; repeated wakeups reuse the
compiled predicate and do not recheck earlier entries or restart the timeout.
A timeout returns an envelope with those positions. Within EXEC or Lua, a read
that would block instead returns immediately with `caught_up`. `CLIENT UNBLOCK
... ERROR` retains its normal error reply. Progress is transient; a new command
using the same old IDs scans again and receives a fresh budget.

Filtering adds no index, write amplification, or persistent stream metadata.
Continuation does not create a snapshot: concurrent trimming or deletion can
remove entries, as with ordinary stream reads.

## Implementation and validation

`src/expr.c` owns the shared parser/evaluator. `modules/vector-sets/expr.c` supplies
its existing JSON adapter; `src/t_stream.c` supplies the stream-field adapter.
A dispatch before scanning sends filtered reads to a separate loop. The original
unfiltered loop contains no predicate or scan-budget checks and does not allocate
field scratch space. Blocking filter ownership is tied to command completion, including
transactions, Lua, timeout, explicit unblocking, and client disconnection.

Run the stream regressions with:

```sh
./runtest --single unit/type/stream-filter --single unit/type/stream-filter-maxscan --single unit/type/stream --single unit/type/stream-cgroups --single unit/moduleapi/stream --clients 4 --timeout 180
```

### Compiled-filter leak regressions

`unit/type/stream-filter-leaks` runs 2,000 read/error cycles (30,000 compilation
attempts) and 200 blocked-reader cleanup cycles after warming caches and
connection buffers. It checks Redis allocator usage after each batch against a
64 KiB growth allowance. Coverage includes compile failures, failed field
resolution, absent/wrong-type keys, EXEC/Lua, repeated wakeups, successful
delivery, timeout, both CLIENT UNBLOCK modes, and disconnection. It uses
allocator usage rather than RSS so retained allocator arenas do not mask the
result. The normal macOS fixture also runs the system `leaks` checker.

```sh
./runtest --single unit/type/stream-filter-leaks --clients 1 --timeout 180
```

Run the filtering suites under AddressSanitizer with explicit flags (cached
build settings can otherwise override the sanitizer flags):

```sh
make -C src -j4 SANITIZER=address LD=clang BUILD_COMPRESSION=no MALLOC=libc OPT=-O1 CFLAGS='-fsanitize=address -fno-sanitize-recover=all -fno-omit-frame-pointer' LDFLAGS='-fsanitize=address' REDIS_CFLAGS='-Werror -DREDIS_TEST -DDEBUG_ASSERTIONS' all
./runtest --single unit/type/stream-filter --single unit/type/stream-filter-maxscan --single unit/type/stream-filter-leaks --clients 1 --timeout 180
```

ASAN detects invalid accesses and frees. Apple's runtime does not support
LeakSanitizer on this platform; the explicit allocator-growth assertions and
native `leaks` run provide separate leak coverage.
