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

## Read behavior and cost

Matching entries retain ID order, reversed for `XREVRANGE`. `COUNT` limits
matching entries; in `XREAD` it remains a per-stream limit. `MAXCOUNT` limits
matches across streams and `MAXSIZE` limits reply bytes as usual. `XREAD` omits
streams with no matches and returns its normal null response if none match.
Range commands return an empty array for no matches, with existing `COUNT 0`
behavior preserved.

A blocking `XREAD` waits for at least one match or the original timeout. Rejected
entries advance only that command's internal position. Repeated wakeups reuse
the compiled predicate and do not recheck earlier entries or restart the timeout.
`$` resolves once when the command begins. `+` initially examines only the last
live entry; if it does not match, a blocking read waits for subsequent entries.
Progress is transient; a new command using the same old ID scans again.

Filtering adds no index, write amplification, or persistent stream metadata. It
still examines each candidate entry and resolves fields by linear lookup. It
saves reply bandwidth and client processing, but selective predicates can scan
a large backlog and occupy the server thread. Result limits do not impose a scan-work budget, and
there is no new continuation token.

## Implementation and validation

`src/expr.c` owns the shared parser/evaluator. `modules/vector-sets/expr.c` supplies
its existing JSON adapter; `src/t_stream.c` supplies the stream-field adapter.
The unfiltered read path does not allocate field scratch space or evaluate a
predicate. Blocking filter ownership is tied to command completion, including
transactions, Lua, timeout, explicit unblocking, and client disconnection.

Run the stream regressions with:

```sh
./runtest --single unit/type/stream-filter --single unit/type/stream --single unit/type/stream-cgroups --single unit/moduleapi/stream --clients 4 --timeout 180
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
./runtest --single unit/type/stream-filter --single unit/type/stream-filter-leaks --clients 1 --timeout 180
```

ASAN detects invalid accesses and frees. Apple's runtime does not support
LeakSanitizer on this platform; the explicit allocator-growth assertions and
native `leaks` run provide separate leak coverage.
