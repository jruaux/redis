# Normalize only in the older predicate/ownership regressions; MAXSCAN tests
# below assert the wire contract directly.
proc stream_filter_legacy_reply {reply} {
    if {[catch {dict exists $reply status} present] || !$present} {return $reply}
    if {[dict exists $reply entries]} {return [dict get $reply entries]}
    set result {}
    foreach part [dict get $reply streams] {
        if {[llength [dict get $part entries]]} {
            lappend result [list [dict get $part key] [dict get $part entries]]
        }
    }
    return $result
}
proc stream_filter_legacy {args} {
    return [stream_filter_legacy_reply [r {*}$args]]
}

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
        assert_equal {2-0 4-0} [filtered_stream_ids [stream_filter_legacy xrange events - + filter {.region == "eu"}]]
        assert_equal {4-0 2-0} [filtered_stream_ids [stream_filter_legacy xrevrange events + - filter {.region == "eu"}]]
        assert_equal {2-0} [filtered_stream_ids [stream_filter_legacy xrange events - + count 1 filter {.amount > 100}]]
        assert_equal {4-0} [filtered_stream_ids [stream_filter_legacy xrevrange events + - filter {.amount > 100} count 1]]
        assert_equal {3-0} [filtered_stream_ids [stream_filter_legacy xrange events (2-0 (4-0 filter {.amount > 100}]]
        assert_equal {} [stream_filter_legacy xrange events - + filter {.amount > 999}]
        assert_equal {} [stream_filter_legacy xrevrange events + - filter {.amount > 999}]
        assert_equal {} [stream_filter_legacy xrange absent - + filter {1}]
        assert_equal {} [stream_filter_legacy xrange events + - filter {1}]
        assert_equal {} [stream_filter_legacy xrange events - + count 0 filter {1}]
    }

    test {Stream FILTER supports arithmetic, membership, substring and negation} {
        assert_equal {2-0 4-0} [filtered_stream_ids [stream_filter_legacy xrange events - + filter {
            .region in ["eu", "apac"] and .amount * 2 >= 300
        }]]
        assert_equal {1-0 3-0} [filtered_stream_ids [stream_filter_legacy xrange events - + filter {not (.region == "eu")}]]
        assert_equal {2-0 4-0} [filtered_stream_ids [stream_filter_legacy xrange events - + filter {"e" in .region}]]
    }

    test {Stream FILTER preserves vector-set coercion and missing-field semantics} {
        r xadd types 1-0 amount not-a-number region eu
        r xadd types 2-0 amount 12 region eu
        r xadd types 3-0 amount {} region us
        assert_equal {1-0 3-0} [filtered_stream_ids [stream_filter_legacy xrange types - + filter {.amount == 0}]]
        assert_equal {2-0} [filtered_stream_ids [stream_filter_legacy xrange types - + filter {.amount > 0}]]
        assert_equal {} [stream_filter_legacy xrange types - + filter {.region == "eu" or .missing == 1}]
        assert_equal {} [stream_filter_legacy xrange types - + filter {.missing == null}]
        r xadd nested 1-0 order {{"amount":150}}
        assert_error {*syntax error in FILTER*} {stream_filter_legacy xrange nested - + filter {.order.amount > 100}}
    }

    test {Stream FILTER preserves duplicates, empty fields, integer buffers and binary values} {
        set binary "a\x00b\xff"
        r xadd bytes 1-0 amount 150 amount 0 123 456 {} {} binary $binary tail 789
        r xadd bytes 2-0 amount 0 amount 150 123 999 {} {} binary other tail 123
        set original [stream_filter_legacy xrange bytes 1-0 1-0]
        assert_equal $original [stream_filter_legacy xrange bytes - + filter {.amount > 100}]
        assert_equal $original [stream_filter_legacy xrevrange bytes + - filter {.amount > 100}]
        assert_equal $original [stream_filter_legacy xrange bytes - + filter {.binary != "a"} count 1]
        r xadd bytes 3-0 amount 200 123 111 binary $binary tail 321
        assert_equal [stream_filter_legacy xrange bytes 3-0 3-0] [stream_filter_legacy xrange bytes - + filter {.amount > 175}]
    }

    test {Stream FILTER validates expressions and options, including absent keys} {
        foreach command {xrange xrevrange} {
            set bounds [expr {$command eq "xrange" ? {- +} : {+ -}}]
            assert_error {*syntax error in FILTER*} {r $command absent {*}$bounds filter {.amount >}}
            assert_error {*NUL byte*} {r $command events {*}$bounds filter "1\x00 or 0"}
            assert_error {*syntax*} {r $command events {*}$bounds filter 1 filter 0}
            assert_error {*syntax*} {r $command events {*}$bounds filter}
        }
        assert_error {*syntax error in FILTER*} {stream_filter_legacy xread filter {.amount >} streams absent 0}
        assert_error {*NUL byte*} {stream_filter_legacy xread filter "1\x00" streams events 0}
        assert_error {*syntax*} {stream_filter_legacy xread filter 1 filter 0 streams events 0}
        assert_error {*not supported by XREADGROUP*} {r xreadgroup group g c filter 1 streams events >}
        r set wrongtype value
        assert_error {*WRONGTYPE*} {stream_filter_legacy xrange wrongtype - + filter 1}
        assert_error {*WRONGTYPE*} {stream_filter_legacy xread filter 1 streams wrongtype 0}
        assert_equal PONG [r ping]
    }

    test {XREAD FILTER omits empty streams and counts matches per stream and globally} {
        r xadd other 1-0 region us amount 500
        assert_equal {{events {{2-0 {region eu amount 150}} {4-0 {region eu amount 300}}}}} \
            [stream_filter_legacy xread filter {.region == "eu"} streams other events 0 0]
        assert_equal {} [stream_filter_legacy xread filter 0 streams events other 0 0]
        assert_equal {} [stream_filter_legacy xread filter 1 streams absent 0]
        set res [stream_filter_legacy xread count 1 filter {.amount > 100} streams events other 0 0]
        assert_equal {2-0} [filtered_stream_ids [lindex $res 0 1]]
        assert_equal {1-0} [filtered_stream_ids [lindex $res 1 1]]
        set res [stream_filter_legacy xread maxcount 2 filter {.amount > 100} streams events other 0 0]
        assert_equal 1 [llength $res]
        assert_equal {2-0 3-0} [filtered_stream_ids [lindex $res 0 1]]
        set res [stream_filter_legacy xread maxsize 1 filter {.amount > 100} streams events other 0 0]
        assert_equal {2-0} [filtered_stream_ids [lindex $res 0 1]]
        assert_equal {events other} [r command getkeys xread filter STREAMS streams events other 0 0]
        assert_equal {events} [r command getkeys xrange events - + filter 1]
    }

    test {XREAD FILTER RESP3, transactions, Lua and deleted entries} {
        r hello 3
        set res [stream_filter_legacy xread filter {.region == "eu"} streams other events 0 0]
        assert_equal events [lindex $res 0 0]
        assert_equal {2-0 4-0} [filtered_stream_ids [lindex $res 0 1]]
        assert_equal {} [stream_filter_legacy xread filter 0 streams events 0]
        assert_equal {} [stream_filter_legacy xrange events - + filter 0]
        r hello 2
        r multi
        stream_filter_legacy xrange events - + filter {.amount > 200}
        stream_filter_legacy xread block 0 filter 0 streams events 0
        set res [lmap reply [r exec] {stream_filter_legacy_reply $reply}]
        assert_equal {3-0 4-0} [filtered_stream_ids [lindex $res 0]]
        assert_equal {} [lindex $res 1]
        r multi
        stream_filter_legacy xread filter 0 streams events 0
        stream_filter_legacy xread filter 1 streams events 0
        stream_filter_legacy xread streams events 0
        r xgroup create events g 0
        r xreadgroup group g c streams events >
        set res [lmap reply [r exec] {stream_filter_legacy_reply $reply}]
        assert_equal {} [lindex $res 0]
        foreach index {1 2 4} {
            assert_equal {1-0 2-0 3-0 4-0} [filtered_stream_ids [lindex $res $index 0 1]]
        }
        set res [r eval {
            local a = redis.call('XREAD','FILTER','0','STREAMS',KEYS[1],'0')
            local b = redis.call('XREAD','FILTER','1','STREAMS',KEYS[1],'0')
            return {a,b}
        } 1 events]
        set res [lmap reply $res {stream_filter_legacy_reply $reply}]
        assert_equal {} [lindex $res 0]
        assert_equal {1-0 2-0 3-0 4-0} [filtered_stream_ids [lindex $res 1 0 1]]
        assert_equal {2-0 4-0} [filtered_stream_ids [stream_filter_legacy_reply [r eval {
            return redis.call('XRANGE',KEYS[1],'-','+','FILTER','.region == "eu"')
        } 1 events]]]
        r xdel events 2-0
        assert_equal {4-0} [filtered_stream_ids [stream_filter_legacy xrange events - + filter {.region == "eu"}]]
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
        assert_equal {{live {{3-0 {region eu}}}}} [stream_filter_legacy_reply [$rd read]]
        $rd ping
        assert_equal PONG [stream_filter_legacy_reply [$rd read]]
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
            assert_equal {{live2 {{3-0 {region eu}}}}} [stream_filter_legacy_reply [$rd read]]
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
        set res [stream_filter_legacy_reply [$rd read]]
        assert_equal 2 [llength $res]
        foreach part $res {assert_equal {{2-0 {region eu}}} [lindex $part 1]}
        $rd close
        set res [stream_filter_legacy xread filter 1 streams resp-stream resp-stream 1-0 1-0]
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
        assert_equal {} [stream_filter_legacy_reply [$rd read]]
        $rd xread filter 1 streams timeout-stream 0
        assert {[llength [lindex [stream_filter_legacy_reply [$rd read]] 0 1]] > 0}
        $rd close
    }

    test {Blocking XREAD FILTER survives key replacement and cleans up on unblock or disconnect} {
        set rd [redis_deferring_client]
        $rd client id
        set id [stream_filter_legacy_reply [$rd read]]
        $rd xread block 0 filter {.region == "eu"} streams replacement 0
        wait_for_blocked_clients_count 1
        r xadd replacement 1-0 region us
        r del replacement
        r set replacement wrongtype
        r del replacement
        r xadd replacement 2-0 region eu
        wait_for_blocked_clients_count 0
        assert_equal {{replacement {{2-0 {region eu}}}}} [stream_filter_legacy_reply [$rd read]]
        $rd xread block 0 filter 0 streams replacement 0
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id timeout]
        assert_equal {} [stream_filter_legacy_reply [$rd read]]
        $rd xread filter 1 streams replacement 0
        assert_equal {{replacement {{2-0 {region eu}}}}} [stream_filter_legacy_reply [$rd read]]
        $rd xread block 0 filter 0 streams replacement 0
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id error]
        assert_error {*UNBLOCKED*} {$rd read}
        $rd xread filter 1 streams replacement 0
        assert_equal {{replacement {{2-0 {region eu}}}}} [stream_filter_legacy_reply [$rd read]]
        $rd xread block 0 filter 0 streams replacement 0
        wait_for_blocked_clients_count 1
        $rd close
        wait_for_blocked_clients_count 0
    }

}
