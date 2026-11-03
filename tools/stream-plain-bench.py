#!/usr/bin/env python3
"""Sustained plain-block Zstd experiment. Disposable servers only; no training.

Default: 6 workloads x 3 modes x 5 phases x 5 s x 2 repetitions, 4 clients,
pipeline 8, 10000 retained entries, 256 MiB maxmemory, 1200 s overall deadline.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import platform
import signal
import socket
import statistics
import struct
import subprocess
import tempfile
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
spec = importlib.util.spec_from_file_location("stream_helpers", HERE / "stream-compression-bench.py")
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
WORKLOADS = ("korvet-244", "korvet-700", "native", "large", "entropy", "korvet-drift")
PHASES = ("write-retain", "small-reads", "large-reads", "approx-trim", "exact-trim")


def encode(args):
    values = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(values) + b"".join(b"$%d\r\n" % len(a)+a+b"\r\n" for a in values)


def reconnect(server):
    server.client.close()
    server.client = helper.Client(server.root / "r.sock")
    assert server.client.call("PING") == b"PONG"
    return server.client


def populate(server, rows, mode):
    c = server.client
    c.call("FLUSHDB", "SYNC")
    c.call("XADD", "s", "1-0", *rows[0])
    if mode == "zstd":
        c.call("XCFGSET", "s", "COMPRESSION", "ZSTD")
    for offset in range(1, len(rows), 32):
        batch = rows[offset:offset+32]
        c.sock.sendall(b"".join(encode(["XADD", "s", f"{offset+i+1}-0", *row])
                                 for i, row in enumerate(batch)))
        for i in range(len(batch)):
            assert c.read() == f"{offset+i+1}-0".encode()
    verify_rows(c,rows)
    return reconnect(server)


def verify_rows(c,rows):
    # Validate every record without a single giant reply allocation.
    for offset in range(0, len(rows), 100):
        reply = c.call("XRANGE", "s", f"{offset+1}-0", "+", "COUNT", 100)
        expected = [[f"{offset+i+1}-0".encode(), row]
                    for i, row in enumerate(rows[offset:offset+100])]
        assert reply == expected


def commands_file(path, rows, retain):
    with path.open("wb") as f:
        f.write(struct.pack(">I", len(rows)))
        for row in rows:
            args = ["XADD", "s"] + (["MAXLEN", "~", 10000] if retain else []) + ["*", *row]
            command = encode(args)
            f.write(struct.pack(">I", len(command)))
            f.write(command)


def wait_child(c, field):
    deadline = time.monotonic()+20
    while c.info("persistence")[field] != "0":
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Waiting for {field}")
        time.sleep(.01)


def policy_check(binary, root):
    """Plain policy and materialized compression survive every supported format."""
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1",0))
        port = reservation.getsockname()[1]
    s = helper.Server(binary, root, port, node_entries=8)
    replica = None
    try:
        c = s.client
        rows = [[b"f", b"abc"*180+b"\x00\xff", b"f", b"", b"", str(i).encode()]
                for i in range(1000)]
        populate(s, rows, "zstd"); c = s.client
        c.call("XGROUP", "CREATE", "s", "g", "0")
        c.call("XREADGROUP", "GROUP", "g", "a", "COUNT", 10, "STREAMS", "s", ">")
        c.call("XDEL", "s", "50-0")
        expected = c.call("XRANGE", "s", "-", "+")
        compressed_memory = c.call("MEMORY", "USAGE", "s")
        c.call("RESTORE", "restored", 0, c.call("DUMP", "s"))
        assert helper.compression_info(c, "restored")["enabled"] == 1
        assert c.call("XRANGE", "restored", "-", "+") == expected
        c.call("DEL", "restored")
        # Explicitly reject unrecognized policy versions/levels at the boundary.
        c.call("CONFIG", "SET", "sanitize-dump-payload", "yes")
        c.call("DEBUG", "SET-SKIP-CHECKSUM-VALIDATION", 1)
        payload = c.call("DUMP", "s")
        assert payload[:3] == bytes([37, 1, 3])
        for offset, value in ((1, 2), (2, 22)):
            bad = payload[:offset]+bytes([value])+payload[offset+1:]
            try: c.call("RESTORE", "invalid", 0, bad)
            except helper.RedisError: pass
            else: raise AssertionError("Invalid policy was accepted")
        c.call("DEBUG", "SET-SKIP-CHECKSUM-VALIDATION", 0)
        c.call("SAVE")
        s.stop(); rdb_restart = s.start(); c = s.client
        assert helper.compression_info(c)["enabled"] == 1
        assert c.call("XRANGE", "s", "-", "+") == expected
        assert c.call("XPENDING", "s", "g")[0] == 10
        assert c.call("MEMORY", "USAGE", "s") <= compressed_memory+4096
        observations = {"rdb_restart_ms": rdb_restart,
                        "rdb_bytes": (s.root/"dump.rdb").stat().st_size}
        check = subprocess.run([str(Path(binary).resolve().with_name("redis-check-rdb")),
                                str(s.root/"dump.rdb")], capture_output=True, text=True, timeout=20)
        if check.returncode:
            raise AssertionError(check.stdout+check.stderr)
        replica = helper.Server(binary, root/"replica", node_entries=8)
        replica.client.call("REPLICAOF","127.0.0.1",port)
        deadline = time.monotonic()+20
        while replica.client.info("replication").get("master_link_status") != "up":
            if time.monotonic() >= deadline: raise TimeoutError("Plain policy full sync")
            time.sleep(.02)
        assert c.call("WAIT",1,5000) == 1
        assert helper.compression_info(replica.client)["enabled"] == 1
        assert replica.client.call("XRANGE","s","-","+") == expected
        assert replica.client.call("MEMORY","USAGE","s") < 200000
        c.call("XADD","s","*","replication",b"r"*700)
        expected = c.call("XRANGE","s","-","+")
        assert c.call("WAIT",1,5000) == 1
        assert replica.client.call("XRANGE","s","-","+") == expected
        replica.stop(); replica = None
        wait_child(c,"rdb_bgsave_in_progress")
        for preamble in ("no", "yes"):
            c.call("CONFIG", "SET", "aof-use-rdb-preamble", preamble)
            c.call("CONFIG", "SET", "appendonly", "yes")
            helper.wait_aof(c)
            c.call("BGREWRITEAOF")
            helper.wait_aof(c)
            manifest = (s.root/"appendonlydir/appendonly.aof.manifest").read_text()
            base = next(line.split()[1] for line in manifest.splitlines() if line.split()[-1] == "b")
            with (s.root/"appendonlydir"/base).open("rb") as f:
                prefix = f.read(5)
            assert prefix.startswith(b"REDIS") if preamble == "yes" else prefix.startswith(b"*")
            s.aof = True
            s.stop(); s.start(); c = s.client
            assert helper.compression_info(c)["enabled"] == 1
            assert c.call("MEMORY", "USAGE", "s") < 200000
            assert c.call("XRANGE", "s", "-", "+") == expected
            assert c.call("XPENDING", "s", "g")[0] == 10
            c.call("XADD", "s", "*", "next", b"n"*700)
            expected = c.call("XRANGE", "s", "-", "+")
            s.stop(); observations[f"aof_{preamble}_restart_ms"] = s.start(); c = s.client
            assert helper.compression_info(c)["enabled"] == 1
            assert c.call("XRANGE", "s", "-", "+") == expected
        return observations
    finally:
        if replica: replica.stop()
        s.stop()


def run_case(binary, mode, kind, rows, root, driver, files, duration, repetition):
    s = helper.Server(binary, root)
    output = []
    try:
        c = populate(s,rows,mode)
        before_cpu = c.info("cpu")
        started = time.perf_counter()
        c.call("SAVE")
        save_ms = (time.perf_counter()-started)*1000
        after_cpu = c.info("cpu")
        save_cpu = sum(float(after_cpu[k])-float(before_cpu[k]) for k in ("used_cpu_user","used_cpu_sys"))
        rdb_bytes = (s.root/"dump.rdb").stat().st_size
        s.stop(); restart_ms = s.start(); c = s.client
        restart_cpu = c.info("cpu")
        if mode == "zstd": assert helper.compression_info(c)["enabled"] == 1
        else: assert "enabled" not in helper.compression_info(c)
        loaded_memory = c.call("MEMORY","USAGE","s")
        verify_rows(c,rows)
        persistence = dict(save_ms=save_ms, save_server_cpu_s=save_cpu, rdb_bytes=rdb_bytes,
                           restart_ms=restart_ms, restart_server_cpu_s=sum(float(restart_cpu[k])
                           for k in ("used_cpu_user","used_cpu_sys")), loaded_stream_memory=loaded_memory)
        for phase in PHASES:
            c = populate(s, rows, mode)
            before = c.info("memory")
            initial_size = c.call("MEMORY", "USAGE", "s")
            max_payload = max(sum(map(len, row[1::2])) for row in rows)
            page = 10 if phase == "small-reads" else max(10, min(1000, 1048576//(max_payload+128)))
            datafile = "-" if "reads" in phase else files[phase == "write-retain"]
            args = [str(driver), str(s.root/"r.sock"), str(datafile), phase,
                    str(duration), "4", "8", "10000", str(page)]
            process = subprocess.run(args, capture_output=True, text=True, timeout=35)
            if process.returncode:
                raise RuntimeError(process.stderr+process.stdout)
            result = json.loads(process.stdout)
            after = c.info("memory")
            length = c.call("XLEN", "s")
            assert 10000 <= length <= 10150
            if "reads" in phase:
                assert c.call("XRANGE", "s", "1-0", "10-0") == [
                    [f"{i+1}-0".encode(), row] for i, row in enumerate(rows[:10])]
            else:
                allowed = {tuple(row) for row in rows}
                sample = c.call("XREVRANGE", "s", "+", "-", "COUNT", 100)
                assert all(tuple(fields) in allowed for _, fields in sample)
            result.update(workload=kind, mode=mode, repetition=repetition, retained=length,
                          clients=4, pipeline=8, page_entries=page,
                          stream_memory_before=initial_size,
                          stream_memory_after=c.call("MEMORY", "USAGE", "s"),
                          used_memory_before=int(before["used_memory"]),
                          used_memory_after=int(after["used_memory"]),
                          redis_peak_memory=int(after["used_memory_peak"]),
                          persistence=persistence,
                          compression=helper.compression_info(c) if mode == "zstd" else {})
            output.append(result)
            print(f"run={repetition} {kind:12} {mode:8} {phase:12} "
                  f"{result['ops_s']:9.0f} ops/s CPU {result['server_cpu_us_per_op']:.2f} us/op", flush=True)
        s.stop()
        for result in output:
            result["process_lifetime_peak_rss_bytes"] = s.peak_rss
        return output
    finally:
        s.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--server", type=Path, default=REPO/"src/redis-server")
    p.add_argument("--baseline-server", type=Path, required=True)
    p.add_argument("--output", type=Path, default=REPO/"stream-plain-results.json")
    p.add_argument("--duration", type=float, default=5)
    p.add_argument("--repetitions", type=int, default=2)
    p.add_argument("--workloads", nargs="+", choices=WORKLOADS, default=list(WORKLOADS))
    p.add_argument("--check-only", action="store_true")
    args = p.parse_args()
    if not .2 <= args.duration <= 5 or not 1 <= args.repetitions <= 2:
        p.error("duration must be .2–5 seconds, repetitions 1–2")
    signal.signal(signal.SIGALRM, lambda *_: (_ for _ in ()).throw(TimeoutError("1200 second overall limit")))
    signal.alarm(1200)
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="stream-plain-", dir="/tmp") as td:
        root = Path(td)
        driver = root/"load"
        subprocess.run(["cc", "-O2", "-std=gnu11", "-Wall", "-Wextra", "-Werror",
                        "-I", str(REPO/"deps/hiredis"), "-I", str(REPO/"deps/hdr_histogram"),
                        str(HERE/"stream-plain-load.c"), str(REPO/"deps/hiredis/libhiredis.a"),
                        str(REPO/"deps/hdr_histogram/libhdrhistogram.a"), "-pthread", "-lm", "-o", str(driver)],
                       check=True, timeout=30)
        checks = policy_check(args.server, root/"policy")
        print("plain policy correctness: passed", flush=True)
        result = {"platform": platform.platform(), "source_commit": "68d9a2df09ec7d4a5c03abe7c1424ad26e896b5d",
                  "server_sha256": hashlib.sha256(args.server.read_bytes()).hexdigest(),
                  "baseline_sha256": hashlib.sha256(args.baseline_server.read_bytes()).hexdigest(),
                  "native_driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
                  "duration_seconds_per_phase": args.duration, "repetitions": args.repetitions,
                  "retained_entries": 10000, "clients": 4, "pipeline": 8,
                  "maxmemory_per_server": 268435456, "timeout_seconds": 1200,
                  "workloads": args.workloads, "policy_checks": checks, "results": []}
        if not args.check_only:
            for repetition in range(1, args.repetitions+1):
                for kind in args.workloads:
                    rows = [helper.fields(kind, i+100000, 991,
                            kind == "korvet-drift" and i >= 5000) for i in range(10000)]
                    files = {False: root/"append.resp", True: root/"retain.resp"}
                    for retain, path in files.items(): commands_file(path, rows, retain)
                    # Alternate ordering on the second pass to reduce order bias.
                    modes = ("baseline", "off", "zstd") if repetition == 1 else ("zstd", "off", "baseline")
                    for mode in modes:
                        binary = args.baseline_server if mode == "baseline" else args.server
                        result["results"] += run_case(binary, mode, kind, rows,
                                root/f"{repetition}-{kind}-{mode}", driver, files, args.duration, repetition)
                        args.output.write_text(json.dumps(result, indent=2)+"\n")
        result["elapsed_seconds"] = time.monotonic()-started
        args.output.write_text(json.dumps(result, indent=2)+"\n")


if __name__ == "__main__":
    main()
