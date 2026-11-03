#!/usr/bin/env python3
"""Bounded local benchmark/checks. Standard library + system libzstd only.

Uses disposable servers, never an existing instance. See STREAM-COMPRESSION.md.
"""
import argparse
import ctypes as C
import ctypes.util
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import random
import signal
import socket
import subprocess
import tempfile
import time
import zlib


class RedisError(Exception):
    pass


class Client:
    def __init__(self, path):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(15)
        self.sock.connect(str(path))
        self.file = self.sock.makefile("rb")

    def close(self):
        self.file.close()
        self.sock.close()

    def read(self):
        line = self.file.readline()
        if not line:
            raise EOFError("Redis closed the connection")
        kind, value = line[:1], line[1:-2]
        if kind == b"-":
            raise RedisError(value.decode())
        if kind == b"+":
            return value
        n = int(value)
        if kind == b":":
            return n
        if n == -1:
            return None
        if kind == b"$":
            data = self.file.read(n)
            if self.file.read(2) != b"\r\n":
                raise ValueError("Invalid RESP bulk string")
            return data
        if kind == b"*":
            return [self.read() for _ in range(n)]
        raise ValueError(f"Unsupported RESP type {kind!r}")

    def call(self, *args):
        values = [x if isinstance(x, bytes) else str(x).encode() for x in args]
        self.sock.sendall(b"*%d\r\n" % len(values) + b"".join(
            b"$%d\r\n" % len(x) + x + b"\r\n" for x in values))
        return self.read()

    def info(self, section):
        return dict(line.split(":", 1) for line in self.call("INFO", section).decode().splitlines()
                    if line and not line.startswith("#"))


class Server:
    def __init__(self, binary, root, port=0, node_entries=100):
        self.binary = str(Path(binary).resolve())
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.port = port
        self.node_entries = node_entries
        self.client = None
        self.process = None
        self.peak_rss = 0
        self.aof = False
        self.start()

    def start(self):
        try:
            return self._start()
        except BaseException:
            # Also covers startup before the caller can enter its finally,
            # failed restarts, and the overall SIGALRM deadline.
            self.abort()
            raise

    def abort(self):
        if self.client:
            self.client.close()
            self.client = None
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        if hasattr(self, "log"):
            self.log.close()

    def _start(self):
        self.log = open(self.root / "server.log", "ab")
        started = time.perf_counter()
        self.process = subprocess.Popen([
            self.binary, "--port", str(self.port), "--bind", "127.0.0.1",
            "--unixsocket", str(self.root / "r.sock"), "--save", "",
            "--appendonly", "yes" if self.aof else "no", "--dir", str(self.root), "--maxmemory", "256mb",
            "--maxmemory-policy", "noeviction", "--enable-debug-command", "yes",
            "--stream-node-max-entries", str(self.node_entries),
            "--repl-diskless-sync-delay", "0"], stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError((self.root / "server.log").read_text())
            try:
                self.client = Client(self.root / "r.sock")
                assert self.client.call("PING") == b"PONG"
                return (time.perf_counter() - started) * 1000
            except RedisError as error:
                if not str(error).startswith("LOADING "):
                    raise
                self.client.close()
                self.client = None
                time.sleep(.01)
            except (OSError, EOFError):
                if self.client:
                    self.client.close()
                    self.client = None
                time.sleep(.01)
        raise TimeoutError("Redis startup exceeded 20 seconds")

    def stop(self):
        if self.client:
            try:
                self.client.call("SHUTDOWN", "NOSAVE")
            except (EOFError, OSError, RedisError):
                pass
            self.client.close()
            self.client = None
        if self.process and self.process.returncode is None:
            # wait4 reports this child's actual lifetime high-water RSS. It
            # includes startup, codec scratch, reply buffers and persistence.
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                pid, status, usage = os.wait4(self.process.pid, os.WNOHANG)
                if pid:
                    self.process.returncode = os.waitstatus_to_exitcode(status)
                    rss = usage.ru_maxrss * (1 if platform.system() == "Darwin" else 1024)
                    self.peak_rss = max(self.peak_rss, rss)
                    break
                time.sleep(.01)
            else:
                self.process.kill()
                self.process.wait(timeout=5)
                raise TimeoutError("Redis shutdown exceeded 10 seconds")
        self.log.close()


def train(samples, capacity=1024):
    lib = C.CDLL(ctypes.util.find_library("zstd") or "libzstd.so")
    lib.ZDICT_trainFromBuffer.argtypes = [C.c_void_p, C.c_size_t, C.c_void_p,
                                         C.POINTER(C.c_size_t), C.c_uint]
    lib.ZDICT_trainFromBuffer.restype = C.c_size_t
    lib.ZDICT_isError.argtypes = [C.c_size_t]
    lib.ZDICT_isError.restype = C.c_uint
    lib.ZDICT_getErrorName.argtypes = [C.c_size_t]
    lib.ZDICT_getErrorName.restype = C.c_char_p
    output = C.create_string_buffer(capacity)
    sizes = (C.c_size_t * len(samples))(*(len(x) for x in samples))
    joined = b"".join(samples)
    length = lib.ZDICT_trainFromBuffer(output, capacity, joined, sizes, len(samples))
    if lib.ZDICT_isError(length):
        raise RuntimeError(lib.ZDICT_getErrorName(length).decode())
    return output.raw[:length]


def fields(kind, i, seed, shifted=False):
    rng = random.Random(seed * 1000000 + i)
    if kind.startswith("korvet") and not shifted:
        # Same field/vocabulary shape as Korvet's DictionaryCompressionBenchmark.
        # Its 244/700 parameters are lower bounds: the complete event may be larger.
        record = {"@timestamp": f"2026-08-14T{rng.randrange(24):02}:{rng.randrange(60):02}:"
                  f"{rng.randrange(60):02}.{rng.randrange(1000):03}Z",
                  "level": rng.choice(["INFO", "WARN", "ERROR", "DEBUG"]),
                  "logger": "com.example.service.RequestHandler", "message": "request handled",
                  "status": 200+rng.randrange(300), "latency_ms": rng.randrange(2000),
                  "host": f"app-{rng.randrange(32):02}.prod.internal", "service": "checkout",
                  "trace_id": f"{rng.getrandbits(128):032x}"}
        value = json.dumps(record, separators=(",", ":")).encode()
        target = 700 if kind == "korvet-700" else 244
        while len(value) < target:
            value = value[:-1] + f',"pad_{len(value)}":{rng.randrange(10)}}}'.encode()
        return [b"value", value]
    if kind == "entropy":
        return [b"data", zlib.compress(rng.randbytes(1024))]
    if kind == "native":
        return [b"sensor", str(i % 200).encode(), b"site", b"west", b"reading",
                str(rng.randrange(100000)).encode(), b"site", b"east", b"empty", b""]
    if shifted and kind in ("drift", "korvet-drift"):
        return [b"telemetry-v2", json.dumps({f"metric_{j}": rng.getrandbits(48)
                                            for j in range(24)}, separators=(",", ":")).encode()]
    record = {"event": "purchase", "tenant": "acme", "region": "west",
              "status": "accepted", "schema": 1}
    if kind != "small-json":
        record.update(user=i, amount=rng.randrange(100000), trace=f"{rng.getrandbits(64):016x}")
    if kind == "large":
        record["description"] = "recorded application event with repeated attributes; " * 160
    return [b"json", json.dumps(record, separators=(",", ":")).encode()]


def timing(call, args):
    start = time.perf_counter_ns()
    reply = call(*args)
    return reply, (time.perf_counter_ns() - start) / 1000


def stats(times, entries_per_op=1):
    ordered = sorted(times)
    return {"operations": len(times), "ops_s": len(times) * 1e6 / sum(times),
            "entries_s": len(times) * entries_per_op * 1e6 / sum(times),
            **{f"p{p}_us": ordered[min(len(times)-1, math.ceil(len(times)*p/100)-1)]
               for p in (50, 95, 99)}}


def configure(c, mode, dictionary):
    if mode != "baseline":
        args = ["XCFGSET", "s", "COMPRESSION", "ZSTD"]
        if mode == "dictionary":
            args += ["DICTIONARY", dictionary]
        assert c.call(*args) == b"OK"


def compression_info(c, key="s"):
    reply = c.call("XINFO", "STREAM", key)
    info = dict(zip(reply[::2], reply[1::2])).get(b"compression", [])
    return {k.decode(): v for k, v in zip(info[::2], info[1::2])}


def exercise_registry_cap(c, dictionary, peer=None):
    """Physical refs differ after an unpropagated no-op trim; replay must work."""
    c.call("CONFIG", "SET", "stream-node-max-entries", 2)
    if peer:
        peer.call("CONFIG", "SET", "stream-node-max-entries", 2)
    c.call("XADD", "cap", "1-0", "f", b"x"*1000)
    for generation in range(16):
        blob = dictionary[:4] + (1000+generation).to_bytes(4, "little") + dictionary[8:]
        c.call("XCFGSET", "cap", "COMPRESSION", "ZSTD", "DICTIONARY", blob)
        for ident in (2+2*generation, 3+2*generation):
            c.call("XADD", "cap", f"{ident}-0", "f", b"x"*1000)
    assert compression_info(c, "cap")["dictionary-generations"] == 16
    if peer:
        assert c.call("WAIT", 1, 5000) == 1
        assert compression_info(peer, "cap")["dictionary-generations"] == 16
    assert c.call("XTRIM", "cap", "MAXLEN", "~", 32) == 0
    assert compression_info(c, "cap")["dictionary-generations"] == 15
    blob = dictionary[:4] + (1016).to_bytes(4, "little") + dictionary[8:]
    c.call("XCFGSET", "cap", "COMPRESSION", "ZSTD", "DICTIONARY", blob)
    c.call("XADD", "cap", "34-0", "f", b"x"*1000)
    expected = c.call("XRANGE", "cap", "-", "+")
    if peer:
        assert c.call("WAIT", 1, 5000) == 1
        assert compression_info(peer, "cap")["enabled"] == 0
        assert peer.call("XRANGE", "cap", "-", "+") == expected
    return expected


def check(binary, root):
    """Dictionary rotation, groups, partial trim, restart, AOF and replication."""
    dictionaries = [train([b"\0".join(fields("schema", i, seed)) for i in range(1000)])
                    for seed in (71, 89)]
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    primary = Server(binary, root / "p", port, node_entries=2)
    replica = None
    try:
        c = primary.client
        c.call("CONFIG", "SET", "stream-node-max-entries", 2)
        c.call("XADD", "s", "1-0", "seed", "")
        expected = [[b"1-0", [b"seed", b""]]]
        for phase, dictionary in enumerate(dictionaries):
            configure(c, "dictionary", dictionary)
            for i in range(2 + phase*80, 82 + phase*80):
                entry = [b"f", b"a"*800+b"\x00\xff", b"f", b"", b"", b"001"]
                ident = f"{i}-0".encode()
                assert c.call("XADD", "s", ident, *entry) == ident
                expected.append([ident, entry])
        assert c.call("XRANGE", "s", "-", "+") == expected
        assert c.call("XREVRANGE", "s", "+", "-") == expected[::-1]
        assert compression_info(c)["dictionary-generations"] == 2
        assert compression_info(c)["dictionary-block-references"] > 0
        c.call("XCFGSET", "s", "COMPRESSION", "OFF")
        assert compression_info(c)["dictionary-generations"] == 2
        assert c.call("XRANGE", "s", "-", "+") == expected
        configure(c, "dictionary", dictionaries[1])
        c.call("XGROUP", "CREATE", "s", "g", "0")
        assert c.call("XREADGROUP", "GROUP", "g", "a", "COUNT", 10, "STREAMS", "s", ">")[0][1] == expected[:10]
        assert c.call("XREADGROUP", "GROUP", "g", "a", "COUNT", 10, "STREAMS", "s", "0")[0][1] == expected[:10]
        assert c.call("XCLAIM", "s", "g", "b", 0, "2-0") == expected[1:2]
        c.call("COPY", "s", "other")
        c.call("DEL", "other")
        assert c.call("XRANGE", "s", "-", "+") == expected
        c.call("COPY", "s", "survivor")
        c.call("DEL", "s")
        c.call("RENAME", "survivor", "s")
        assert c.call("XRANGE", "s", "-", "+") == expected
        assert compression_info(c)["dictionary-generations"] == 2
        # Full synchronization goes through the unmodified RDB format.
        replica = Server(binary, root / "r", node_entries=2)
        sync_start = time.perf_counter()
        replica.client.call("REPLICAOF", "127.0.0.1", port)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if replica.client.info("replication").get("master_link_status") == "up":
                break
            time.sleep(.02)
        else:
            raise TimeoutError("Replication full sync exceeded 20 seconds")
        assert c.call("WAIT", 1, 5000) == 1
        assert replica.client.call("XRANGE", "s", "-", "+") == expected
        assert replica.client.call("XPENDING", "s", "g")[0] == 10
        measurements = {"replication_full_sync_ms": (time.perf_counter()-sync_start)*1000,
                        "replication_full_sync_entries": len(expected)}
        exercise_registry_cap(c, dictionaries[0], replica.client)
        c.call("DEL", "cap")
        assert c.call("WAIT", 1, 5000) == 1
        configure(c, "dictionary", dictionaries[0])  # Incremental configuration replication.
        c.call("XDEL", "s", "2-0")
        expected = [x for x in expected if x[0] != b"2-0"]
        assert c.call("XREADGROUP", "GROUP", "g", "b", "STREAMS", "s", "0")[0][1] == [[b"2-0", None]]
        c.call("XTRIM", "s", "MAXLEN", "=", 100)
        expected = expected[-100:]
        c.call("XADD", "s", "200-0", "tail", "new")
        expected.append([b"200-0", [b"tail", b"new"]])
        assert c.call("WAIT", 1, 5000) == 1
        assert replica.client.call("XRANGE", "s", "-", "+") == expected
        replica.stop(); replica = None
        deadline = time.monotonic() + 20
        while c.info("persistence")["rdb_bgsave_in_progress"] != "0":
            if time.monotonic() >= deadline:
                raise TimeoutError("Replication RDB child did not exit")
            time.sleep(.02)
        c.call("SAVE")
        primary.stop(); primary.start(); c = primary.client
        assert c.call("XRANGE", "s", "-", "+") == expected
        assert c.call("XPENDING", "s", "g")[0] == 10
        # Upstream plain-AOF rewrite drops PEL references to deleted records
        # (also reproduced on the unmodified baseline). Verify deleted PELs
        # with RDB above; use live pending entries for AOF parity below.
        c.call("XACK", "s", "g", *[f"{i}-0" for i in range(1, 11)])
        c.call("XREADGROUP", "GROUP", "g", "a", "COUNT", 10, "STREAMS", "s", ">")
        # Test both plain AOF and an RDB-preamble AOF; each starts from a
        # compressed dictionary stream and compares all bytes after restart.
        for preamble in ("no", "yes"):
            c.call("CONFIG", "SET", "aof-use-rdb-preamble", preamble)
            c.call("CONFIG", "SET", "appendonly", "yes")
            wait_aof(c)
            cap_expected = exercise_registry_cap(c, dictionaries[0])
            configure(c, "dictionary", dictionaries[1])
            for _ in range(40): c.call("XADD", "s", "*", "aof", b"b"*1000)
            expected = c.call("XRANGE", "s", "-", "+")
            # Test incremental AOF replay before the explicit rewrite.
            primary.aof = True
            primary.stop(); primary.start(); c = primary.client
            assert c.call("XRANGE", "cap", "-", "+") == cap_expected
            assert compression_info(c, "cap")["enabled"] == 0
            c.call("DEL", "cap")
            assert c.call("XRANGE", "s", "-", "+") == expected
            assert c.call("XPENDING", "s", "g")[0] == 10
            c.call("CONFIG", "SET", "aof-use-rdb-preamble", preamble)
            started = time.perf_counter()
            c.call("BGREWRITEAOF")
            wait_aof(c)
            measurements[f"aof_{preamble}_rewrite_ms"] = (time.perf_counter()-started)*1000
            measurements[f"aof_{preamble}_bytes"] = sum(
                x.stat().st_size for x in (primary.root/"appendonlydir").glob("*") if x.is_file())
            primary.stop()
            measurements[f"aof_{preamble}_restart_ms"] = primary.start()
            c = primary.client
            assert c.call("XRANGE", "s", "-", "+") == expected
            assert c.call("XPENDING", "s", "g")[0] == 10
        return measurements
    finally:
        if replica: replica.stop()
        primary.stop()


def wait_aof(c):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        info = c.info("persistence")
        if info["aof_rewrite_in_progress"] == "0" and info["aof_rewrite_scheduled"] == "0":
            assert info["aof_last_bgrewrite_status"] == "ok"
            return
        time.sleep(.02)
    raise TimeoutError("AOF rewrite exceeded 20 seconds")


def training_data(kind):
    training, size = [], 0
    for i in range(10000):
        sample = b"\0".join(fields(kind, i, 17)[1::2])
        if size + len(sample) > 16*1024*1024:
            break
        training.append(sample)
        size += len(sample)
    training_start = time.perf_counter_ns()
    dictionary = train(training, 65536)
    training_ms = (time.perf_counter_ns()-training_start)/1e6
    return training, dictionary, training_ms


def record_codec_reference(kind, dictionary, n):
    """Payload bytes only, no Redis: attribute dictionary gains vs block gains.

    Uses the exact same held-out values as the stream runs at level 3. These
    figures exclude frames' Redis entry overhead and are not memory estimates.
    """
    lib = C.CDLL(ctypes.util.find_library("zstd") or "libzstd.so")
    lib.ZSTD_createCCtx.restype = C.c_void_p
    lib.ZSTD_freeCCtx.argtypes = [C.c_void_p]
    lib.ZSTD_compressBound.argtypes = [C.c_size_t]
    lib.ZSTD_compressBound.restype = C.c_size_t
    lib.ZSTD_compress_usingDict.argtypes = [C.c_void_p, C.c_void_p, C.c_size_t,
            C.c_void_p, C.c_size_t, C.c_void_p, C.c_size_t, C.c_int]
    lib.ZSTD_compress_usingDict.restype = C.c_size_t
    lib.ZSTD_isError.argtypes = [C.c_size_t]
    lib.ZSTD_isError.restype = C.c_uint
    context = lib.ZSTD_createCCtx()
    if not context:
        raise MemoryError("Cannot allocate reference compressor")
    raw_bytes = plain_bytes = dict_bytes = 0
    try:
        for i in range(n):
            value = fields(kind, i+100000, 991, kind == "korvet-drift" and i >= n//2)[1]
            output = C.create_string_buffer(lib.ZSTD_compressBound(len(value)))
            lengths = []
            for d in (b"", dictionary):
                length = lib.ZSTD_compress_usingDict(context, output, len(output),
                        value, len(value), d, len(d), 3)
                if lib.ZSTD_isError(length):
                    raise ValueError("Reference compression failed")
                lengths.append(length)
            raw_bytes += len(value); plain_bytes += lengths[0]; dict_bytes += lengths[1]
    finally:
        lib.ZSTD_freeCCtx(context)
    return {"entries": n, "raw_bytes_per_record": raw_bytes/n,
            "plain_frame_bytes_per_record": plain_bytes/n,
            "dictionary_frame_bytes_per_record": dict_bytes/n,
            "dictionary_storage_bytes": len(dictionary)}


def benchmark(binary, root, kind, mode, n, training_bundle):
    training, dictionary, training_ms = training_bundle
    rows = [[f"{i+1}-0".encode(), fields(kind, i+100000, 991,
             kind in ("drift", "korvet-drift") and i >= n//2)]
            for i in range(n)]
    server = Server(binary, root)
    try:
        c = server.client
        c.info("memory")  # Warm the INFO response buffer before the empty baseline.
        initial = int(c.info("memory")["used_memory"])
        c.call("XADD", "s", rows[0][0], *rows[0][1])
        configure(c, mode, dictionary)
        # A 64 KiB DICTIONARY command grows its connection's query buffer.
        # Reconnect in every mode so installation traffic cannot masquerade
        # as retained dictionary/data memory in the steady-state comparison.
        c.close()
        c = server.client = Client(server.root / "r.sock")
        c.call("PING")
        latencies, checkpoints = [], {}
        for count, (ident, entry) in enumerate(rows[1:], 2):
            latencies.append(timing(c.call, ["XADD", "s", ident, *entry])[1])
            if count in (100, 500, 1000, 2500, n):
                checkpoints[count] = {"stream_memory": c.call("MEMORY", "USAGE", "s"),
                                      "used_memory_delta": int(c.info("memory")["used_memory"])-initial}
        memory = c.info("memory")
        result = {"workload": kind, "mode": mode, "entries": n,
                  "dictionary_bytes": len(dictionary) if mode == "dictionary" else 0,
                  "training_entries": len(training),
                  "training_bytes": sum(map(len, training)),
                  "payload_min_bytes": min(sum(map(len, entry[1::2])) for _, entry in rows),
                  "payload_max_bytes": max(sum(map(len, entry[1::2])) for _, entry in rows),
                  "payload_mean_bytes": sum(sum(map(len, entry[1::2])) for _, entry in rows)/n,
                  "training_ms": training_ms,
                  "memory_checkpoints": checkpoints,
                  "training_sha256": hashlib.sha256(b"".join(training)).hexdigest(),
                  "dictionary_sha256": hashlib.sha256(dictionary).hexdigest(),
                  "used_memory": int(memory["used_memory"]),
                  "mem_clients_normal": int(memory["mem_clients_normal"]),
                  "used_memory_delta": int(memory["used_memory"])-initial,
                  "stream_memory": c.call("MEMORY", "USAGE", "s"),
                  "redis_peak_memory": int(memory["used_memory_peak"]),
                  "xadd": stats(latencies)}
        for page, iterations in ((10, 100), (1000, 15)):
            for command in ("XRANGE", "XREAD"):
                times = []
                for j in range(iterations):
                    offset = (j * 97) % (n-page)
                    if command == "XRANGE":
                        args = [command, "s", rows[offset][0], "+", "COUNT", page]
                    else:
                        args = [command, "COUNT", page, "STREAMS", "s", f"{offset}-0"]
                    reply, elapsed = timing(c.call, args)
                    if command == "XREAD": reply = reply[0][1]
                    assert reply == rows[offset:offset+page]
                    times.append(elapsed)
                result[f"{command.lower()}_{page}"] = stats(times, page)
        result["compression_storage_and_scratch"] = compression_info(c)
        c.call("XGROUP", "CREATE", "s", "g", "0")
        for name, args in (
            ("group_delivery", ["XREADGROUP", "GROUP", "g", "a", "COUNT", 10, "STREAMS", "s", ">"]),
            ("pending_reread", ["XREADGROUP", "GROUP", "g", "a", "COUNT", 10, "STREAMS", "s", "0"]),
            ("claim", ["XCLAIM", "s", "g", "b", 0, *[x[0] for x in rows[:10]]]),
            ("autoclaim", ["XAUTOCLAIM", "s", "g", "a", 0, "0-0", "COUNT", 10])):
            times = [timing(c.call, args)[1] for _ in range(50)]
            result[name] = stats(times, 10)
        _, result["save_us"] = timing(c.call, ["SAVE"])
        result["rdb_bytes"] = (root / "dump.rdb").stat().st_size
        primary_data = c.call("XRANGE", "s", "-", "+")
        primary_pending = c.call("XPENDING", "s", "g")[0]
        server.stop()
        result["restart_ms"] = server.start()
        c = server.client
        assert c.call("XRANGE", "s", "-", "+") == primary_data
        assert c.call("XPENDING", "s", "g")[0] == primary_pending
        # Restart materializes blocks. Rebuild a compressed stream to measure
        # deletion/trim on compressed storage, rather than the restored raw one.
        c.call("DEL", "s")
        c.call("XADD", "s", rows[0][0], *rows[0][1]); configure(c, mode, dictionary)
        for ident, entry in rows[1:]: c.call("XADD", "s", ident, *entry)
        result["xdel"] = stats([timing(c.call, ["XDEL", "s", x[0]])[1] for x in rows[:100]])
        _, result["trim_us"] = timing(c.call, ["XTRIM", "s", "MAXLEN", "=", n//2])
        _, result["delete_stream_us"] = timing(c.call, ["DEL", "s"])
        server.stop()
        result["process_peak_rss_bytes"] = server.peak_rss
        return result
    finally:
        server.stop()


def main():
    started = time.perf_counter()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--server", default="src/redis-server")
    p.add_argument("--baseline-server", required=True)
    p.add_argument("--entries", type=int, default=5000)
    p.add_argument("--output", type=Path, default=Path("stream-compression-shared-results.json"))
    p.add_argument("--check-only", action="store_true")
    args = p.parse_args()
    if not 1100 <= args.entries <= 20000:
        p.error("entries must be between 1100 and 20000")
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("900 second run limit")))
    signal.alarm(900)
    with tempfile.TemporaryDirectory(prefix="stream-zstd-", dir="/tmp") as scratch:
        root = Path(scratch)
        checks = check(args.server, root / "check")
        print("correctness: passed", flush=True)
        results = {"platform": platform.platform(), "entries_per_case": args.entries,
                   "training_entry_limit": 10000, "training_byte_limit": 16777216,
                   "dictionary_capacity": 65536, "compression_level": 3, "encoding": "SZ02",
                   "concurrency": 1,
                   "configuration_connection_replaced": True,
                   "maxmemory_per_server": 268435456, "timeout_seconds": 900,
                   "baseline_server": str(Path(args.baseline_server).resolve()),
                   "server_sha256": hashlib.sha256(Path(args.server).read_bytes()).hexdigest(),
                   "baseline_sha256": hashlib.sha256(Path(args.baseline_server).read_bytes()).hexdigest(),
                   "server_version": subprocess.check_output([args.server, "--version"], text=True).strip(),
                   "correctness": checks, "record_codec_references": {}, "results": []}
        if not args.check_only:
            for kind in ("small-json", "schema", "native", "large", "entropy", "drift",
                         "korvet-244", "korvet-700", "korvet-drift"):
                training_bundle = training_data(kind)
                if kind.startswith("korvet"):
                    results["record_codec_references"][kind] = record_codec_reference(
                        kind, training_bundle[1], args.entries)
                for mode in ("baseline", "zstd", "dictionary"):
                    binary = args.baseline_server if mode == "baseline" else args.server
                    result = benchmark(binary, root / f"{kind}-{mode}", kind, mode, args.entries, training_bundle)
                    results["results"].append(result)
                    args.output.write_text(json.dumps(results, indent=2) + "\n")
                    print(f"{kind:12} {mode:10} {result['stream_memory']/args.entries:8.1f} B/entry "
                          f"{result['xadd']['ops_s']:8.0f} XADD/s", flush=True)
        results["elapsed_seconds"] = time.perf_counter()-started
        args.output.write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
