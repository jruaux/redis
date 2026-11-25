# FILTER shares the vector-set scalar expression language, resolving native
# stream values as strings. No nested JSON traversal or consumer-group filtering.
proc filtered_stream_ids {entries} {
    lmap entry $entries {lindex $entry 0}
}

start_server {tags {"stream"}} {
    r config set stream-node-max-entries 2

    test {Stream FILTER ranges preserve ordering, boundaries and matching COUNT} {
        r xadd events 1-0 region us amount 90
        r xadd events 2-0 region eu amount 150
        r xadd events 3-0 region us amount 250
        r xadd events 4-0 region eu amount 300
        assert_equal {2-0 4-0} [filtered_stream_ids [r xrange events - + filter {.region == "eu"}]]
        assert_equal {4-0 2-0} [filtered_stream_ids [r xrevrange events + - filter {.region == "eu"}]]
        assert_equal {2-0} [filtered_stream_ids [r xrange events - + count 1 filter {.amount > 100}]]
        assert_equal {4-0} [filtered_stream_ids [r xrevrange events + - filter {.amount > 100} count 1]]
        assert_equal {3-0} [filtered_stream_ids [r xrange events (2-0 (4-0 filter {.amount > 100}]]
        assert_equal {} [r xrange events - + filter {.amount > 999}]
        assert_equal {} [r xrevrange events + - filter {.amount > 999}]
        assert_equal {} [r xrange absent - + filter {1}]
        assert_equal {} [r xrange events + - filter {1}]
        assert_equal {} [r xrange events - + count 0 filter {1}]
    }

    test {Stream FILTER supports arithmetic, membership, substring and negation} {
        assert_equal {2-0 4-0} [filtered_stream_ids [r xrange events - + filter {
            .region in ["eu", "apac"] and .amount * 2 >= 300
        }]]
        assert_equal {1-0 3-0} [filtered_stream_ids [r xrange events - + filter {not (.region == "eu")}]]
        assert_equal {2-0 4-0} [filtered_stream_ids [r xrange events - + filter {"e" in .region}]]
    }

    test {Stream FILTER preserves vector-set coercion and missing-field semantics} {
        r xadd types 1-0 amount not-a-number region eu
        r xadd types 2-0 amount 12 region eu
        r xadd types 3-0 amount {} region us
        assert_equal {1-0 3-0} [filtered_stream_ids [r xrange types - + filter {.amount == 0}]]
        assert_equal {2-0} [filtered_stream_ids [r xrange types - + filter {.amount > 0}]]
        assert_equal {} [r xrange types - + filter {.region == "eu" or .missing == 1}]
        assert_equal {} [r xrange types - + filter {.missing == null}]
        r xadd nested 1-0 order {{"amount":150}}
        assert_error {*syntax error in FILTER*} {r xrange nested - + filter {.order.amount > 100}}
    }

    test {Stream FILTER preserves duplicates, empty fields, integer buffers and binary values} {
        set binary "a\x00b\xff"
        r xadd bytes 1-0 amount 150 amount 0 123 456 {} {} binary $binary tail 789
        r xadd bytes 2-0 amount 0 amount 150 123 999 {} {} binary other tail 123
        set original [r xrange bytes 1-0 1-0]
        assert_equal $original [r xrange bytes - + filter {.amount > 100}]
        assert_equal $original [r xrevrange bytes + - filter {.amount > 100}]
        assert_equal $original [r xrange bytes - + filter {.binary != "a"} count 1]
        r xadd bytes 3-0 amount 200 123 111 binary $binary tail 321
        assert_equal [r xrange bytes 3-0 3-0] [r xrange bytes - + filter {.amount > 175}]
    }

    test {Stream FILTER validates expressions and options, including absent keys} {
        foreach command {xrange xrevrange} {
            set bounds [expr {$command eq "xrange" ? {- +} : {+ -}}]
            assert_error {*syntax error in FILTER*} {r $command absent {*}$bounds filter {.amount >}}
            assert_error {*NUL byte*} {r $command events {*}$bounds filter "1\x00 or 0"}
            assert_error {*syntax*} {r $command events {*}$bounds filter 1 filter 0}
            assert_error {*syntax*} {r $command events {*}$bounds filter}
        }
        assert_error {*syntax error in FILTER*} {r xread filter {.amount >} streams absent 0}
        assert_error {*NUL byte*} {r xread filter "1\x00" streams events 0}
        assert_error {*syntax*} {r xread filter 1 filter 0 streams events 0}
        assert_error {*not supported by XREADGROUP*} {r xreadgroup group g c filter 1 streams events >}
        r set wrongtype value
        assert_error {*WRONGTYPE*} {r xrange wrongtype - + filter 1}
        assert_error {*WRONGTYPE*} {r xread filter 1 streams wrongtype 0}
        assert_equal PONG [r ping]
    }

    test {XREAD FILTER omits empty streams and counts matches per stream and globally} {
        r xadd other 1-0 region us amount 500
        assert_equal {{events {{2-0 {region eu amount 150}} {4-0 {region eu amount 300}}}}} \
            [r xread filter {.region == "eu"} streams other events 0 0]
        assert_equal {} [r xread filter 0 streams events other 0 0]
        assert_equal {} [r xread filter 1 streams absent 0]
        set res [r xread count 1 filter {.amount > 100} streams events other 0 0]
        assert_equal {2-0} [filtered_stream_ids [lindex $res 0 1]]
        assert_equal {1-0} [filtered_stream_ids [lindex $res 1 1]]
        set res [r xread maxcount 2 filter {.amount > 100} streams events other 0 0]
        assert_equal 1 [llength $res]
        assert_equal {2-0 3-0} [filtered_stream_ids [lindex $res 0 1]]
        set res [r xread maxsize 1 filter {.amount > 100} streams events other 0 0]
        assert_equal {2-0} [filtered_stream_ids [lindex $res 0 1]]
        assert_equal {events other} [r command getkeys xread filter STREAMS streams events other 0 0]
        assert_equal {events} [r command getkeys xrange events - + filter 1]
    }

    test {XREAD FILTER RESP3, transactions, Lua and deleted entries} {
        r hello 3
        set res [r xread filter {.region == "eu"} streams other events 0 0]
        assert_equal {events} [dict keys $res]
        assert_equal {2-0 4-0} [filtered_stream_ids [dict get $res events]]
        assert_equal {} [r xread filter 0 streams events 0]
        assert_equal {} [r xrange events - + filter 0]
        r hello 2
        r multi
        r xrange events - + filter {.amount > 200}
        r xread block 0 filter 0 streams events 0
        set res [r exec]
        assert_equal {3-0 4-0} [filtered_stream_ids [lindex $res 0]]
        assert_equal {} [lindex $res 1]
        r multi
        r xread filter 0 streams events 0
        r xread filter 1 streams events 0
        r xread streams events 0
        r xgroup create events g 0
        r xreadgroup group g c streams events >
        set res [r exec]
        assert_equal {} [lindex $res 0]
        foreach index {1 2 4} {
            assert_equal {1-0 2-0 3-0 4-0} [filtered_stream_ids [lindex $res $index 0 1]]
        }
        set res [r eval {
            local a = redis.call('XREAD','FILTER','0','STREAMS',KEYS[1],'0')
            local b = redis.call('XREAD','FILTER','1','STREAMS',KEYS[1],'0')
            return {a,b}
        } 1 events]
        assert_equal {} [lindex $res 0]
        assert_equal {1-0 2-0 3-0 4-0} [filtered_stream_ids [lindex $res 1 0 1]]
        assert_equal {2-0 4-0} [filtered_stream_ids [r eval {
            return redis.call('XRANGE',KEYS[1],'-','+','FILTER','.region == "eu"')
        } 1 events]]
        r xdel events 2-0
        assert_equal {4-0} [filtered_stream_ids [r xrange events - + filter {.region == "eu"}]]
    }

    test {Blocking XREAD FILTER scans existing nonmatches and wakes only for a match} {
        set rd [redis_deferring_client]
        r xadd live 1-0 region us
        $rd xread block 5000 filter {.region == "eu"} streams live 0
        wait_for_blocked_clients_count 1
        r xadd live 2-0 region us
        wait_for_blocked_clients_count 1
        r xadd live 3-0 region eu
        wait_for_blocked_clients_count 0
        assert_equal {{live {{3-0 {region eu}}}}} [$rd read]
        $rd ping
        assert_equal PONG [$rd read]
        $rd close
    }

    test {Blocking XREAD FILTER handles dollar, plus and multiple streams independently} {
        foreach start {$ +} {
            r del live1 live2
            r xadd live1 1-0 region us
            r xadd live2 1-0 region us
            set rd [redis_deferring_client]
            $rd xread block 5000 filter {.region == "eu"} streams live1 live2 $start $start
            wait_for_blocked_clients_count 1
            r xadd live1 2-0 region us
            r xadd live2 2-0 region us
            wait_for_blocked_clients_count 1
            r xadd live2 3-0 region eu
            wait_for_blocked_clients_count 0
            assert_equal {{live2 {{3-0 {region eu}}}}} [$rd read]
            $rd close
        }
    }

    test {Blocking XREAD FILTER supports RESP3 and duplicate stream keys} {
        r del resp-stream
        set rd [redis_deferring_client]
        $rd hello 3
        $rd read
        $rd xread block 5000 filter {.region == "eu"} streams resp-stream resp-stream 0 0
        wait_for_blocked_clients_count 1
        r xadd resp-stream 1-0 region us
        wait_for_blocked_clients_count 1
        r xadd resp-stream 2-0 region eu
        wait_for_blocked_clients_count 0
        # RESP3 can encode repeated map keys; the Tcl client keeps the last.
        set res [$rd read]
        assert_equal {{2-0 {region eu}}} [dict get $res resp-stream]
        $rd close
        set res [r xread filter 1 streams resp-stream resp-stream 1-0 1-0]
        assert_equal 2 [llength $res]
        foreach part $res {assert_equal {{2-0 {region eu}}} [lindex $part 1]}
    }

    test {Blocking XREAD FILTER retains original timeout despite nonmatching appends} {
        set rd [redis_deferring_client]
        $rd xread block 150 filter {.region == "eu"} streams timeout-stream 0
        wait_for_blocked_clients_count 1
        # Each poll appends another nonmatch. A restarted deadline never expires.
        wait_for_condition 100 10 {
            [r xadd timeout-stream * region us] ne {} && [s blocked_clients] == 0
        } else {
            fail "Filtered XREAD restarted its timeout"
        }
        assert_equal {} [$rd read]
        $rd xread filter 1 streams timeout-stream 0
        assert {[llength [lindex [$rd read] 0 1]] > 0}
        $rd close
    }

    test {Blocking XREAD FILTER survives key replacement and cleans up on unblock or disconnect} {
        set rd [redis_deferring_client]
        $rd client id
        set id [$rd read]
        $rd xread block 0 filter {.region == "eu"} streams replacement 0
        wait_for_blocked_clients_count 1
        r xadd replacement 1-0 region us
        r del replacement
        r set replacement wrongtype
        r del replacement
        r xadd replacement 2-0 region eu
        wait_for_blocked_clients_count 0
        assert_equal {{replacement {{2-0 {region eu}}}}} [$rd read]
        $rd xread block 0 filter 0 streams replacement 0
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id timeout]
        assert_equal {} [$rd read]
        $rd xread filter 1 streams replacement 0
        assert_equal {{replacement {{2-0 {region eu}}}}} [$rd read]
        $rd xread block 0 filter 0 streams replacement 0
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id error]
        assert_error {*UNBLOCKED*} {$rd read}
        $rd xread filter 1 streams replacement 0
        assert_equal {{replacement {{2-0 {region eu}}}}} [$rd read]
        $rd xread block 0 filter 0 streams replacement 0
        wait_for_blocked_clients_count 1
        $rd close
        wait_for_blocked_clients_count 0
    }

    if {$::compression} {
        test {Stream FILTER reads compressed blocks with the same bytes and results} {
            r del packed unpacked
            foreach key {packed unpacked} {r xadd $key 1-0 region us amount 0}
            r xcfgset packed compression zstd
            for {set i 2} {$i <= 80} {incr i} {
                foreach key {packed unpacked} {
                    r xadd $key $i-0 region [expr {$i % 2 ? "us" : "eu"}] amount $i \
                        data [string repeat abcdef 70]
                }
            }
            set predicate {.region == "eu" and .amount > 20}
            assert_equal [r xrange unpacked - + filter $predicate] [r xrange packed - + filter $predicate]
            assert_equal [r xrevrange unpacked + - filter $predicate] [r xrevrange packed + - filter $predicate]
            assert_equal [lindex [r xread filter $predicate streams unpacked 0] 0 1] \
                [lindex [r xread filter $predicate streams packed 0] 0 1]
        }
    }
}
