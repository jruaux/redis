proc maxscan_ids {entries} {lmap entry $entries {lindex $entry 0}}

start_server {tags {"stream"}} {
    r config set stream-node-max-entries 100
    for {set i 1} {$i <= 1002} {incr i} {r xadd backlog $i-0 amount $i}
    for {set i 1} {$i <= 4} {incr i} {r xadd short $i-0 amount $i}

    foreach protocol {2 3} {
        r hello $protocol
        test "FILTER defaults to 1000 examined entries in RESP$protocol" {
            set res [r xrange backlog - + filter 0]
            assert_equal {entries last_scanned status} [dict keys $res]
            assert_equal {} [dict get $res entries]
            assert_equal 1000-0 [dict get $res last_scanned]
            assert_equal budget_exhausted [dict get $res status]
            set res [r xrevrange backlog + - filter 0]
            assert_equal 3-0 [dict get $res last_scanned]
            assert_equal budget_exhausted [dict get $res status]
            set res [r xread filter 0 streams backlog 0]
            assert_equal budget_exhausted [dict get $res status]
            assert_equal 1000-0 [dict get [lindex [dict get $res streams] 0] last_scanned]
        }

        test "Filtered range continuation skips rejected entries in RESP$protocol" {
            set res [r xrange short - + maxscan 3 filter {.amount == 1}]
            assert_equal {1-0} [maxscan_ids [dict get $res entries]]
            assert_equal 3-0 [dict get $res last_scanned]
            assert_equal budget_exhausted [dict get $res status]
            set cursor [dict get $res last_scanned]
            set res [r xrange short ($cursor + filter 1 maxscan 3]
            assert_equal {4-0} [maxscan_ids [dict get $res entries]]
            assert_equal exhausted [dict get $res status]
            set res [r xrevrange short + - maxscan 3 filter {.amount == 4}]
            assert_equal {4-0} [maxscan_ids [dict get $res entries]]
            assert_equal 2-0 [dict get $res last_scanned]
            set cursor [dict get $res last_scanned]
            set res [r xrevrange short ($cursor - filter 1 maxscan 3]
            assert_equal {1-0} [maxscan_ids [dict get $res entries]]
            assert_equal exhausted [dict get $res status]
        }

        test "Filtered range empty and result-limit responses in RESP$protocol" {
            foreach command {xrange xrevrange} {
                set bounds [expr {$command eq "xrange" ? {- +} : {+ -}}]
                foreach key {absent short} {
                    set res [r $command $key {*}$bounds filter 0 maxscan 10]
                    assert_equal {} [dict get $res entries]
                    assert_equal exhausted [dict get $res status]
                    if {$key eq "absent"} {assert_equal {} [dict get $res last_scanned]}
                }
                set res [r $command short {*}$bounds filter 1 count 1 maxscan 10]
                assert_equal count_reached [dict get $res status]
                assert_equal 1 [llength [dict get $res entries]]
                set res [r $command short {*}$bounds filter 1 count 0]
                assert_equal count_reached [dict get $res status]
                assert_equal {} [dict get $res last_scanned]
            }
            # Budget exhaustion is conservative at the exact end; no lookahead.
            assert_equal budget_exhausted [dict get [r xrange short - + filter 0 maxscan 4] status]
        }

        test "XREAD shares MAXSCAN and preserves every stream cursor in RESP$protocol" {
            set res [r xread filter {.amount == 2} maxscan 6 streams absent short backlog 7 0 0]
            assert_equal budget_exhausted [dict get $res status]
            set parts [dict get $res streams]
            assert_equal 3 [llength $parts]
            assert_equal {absent short backlog} [lmap p $parts {dict get $p key}]
            assert_equal {7-0 4-0 2-0} [lmap p $parts {dict get $p last_scanned}]
            assert_equal {2-0} [maxscan_ids [dict get [lindex $parts 1] entries]]
            assert_equal {2-0} [maxscan_ids [dict get [lindex $parts 2] entries]]
            set res [r xread filter 0 maxscan 2 streams short backlog 0 9]
            assert_equal {2-0 9-0} [lmap p [dict get $res streams] {dict get $p last_scanned}]
            set res [r xread filter 1 maxscan 3 streams short short 0 0]
            assert_equal 2 [llength [dict get $res streams]]
            assert_equal {3-0 0-0} [lmap p [dict get $res streams] {dict get $p last_scanned}]
        }

        test "XREAD filtered continuation and result limits in RESP$protocol" {
            set res [r xread filter {.amount == 1} maxscan 3 streams short 0]
            set part [lindex [dict get $res streams] 0]
            assert_equal {1-0} [maxscan_ids [dict get $part entries]]
            set res [r xread filter 1 maxscan 3 streams short [dict get $part last_scanned]]
            assert_equal {4-0} [maxscan_ids [dict get [lindex [dict get $res streams] 0] entries]]
            assert_equal caught_up [dict get $res status]
            foreach limit {{count 1} {maxcount 1} {maxsize 1}} {
                set res [r xread {*}$limit filter 1 maxscan 10 streams short backlog 0 0]
                assert_equal result_limit [dict get $res status]
                assert_equal {1-0} [maxscan_ids [dict get [lindex [dict get $res streams] 0] entries]]
            }
            set res [r xread filter 0 maxscan 10 streams short absent 0 42]
            assert_equal caught_up [dict get $res status]
            assert_equal {4-0 42-0} [lmap p [dict get $res streams] {dict get $p last_scanned}]
        }

        test "Symbolic and maximum XREAD cursors survive unvisited streams in RESP$protocol" {
            set res [r xread filter 0 maxscan 1 streams short backlog absent short 0 $ $ +]
            assert_equal {1-0 1002-0 0-0 3-18446744073709551615} \
                [lmap p [dict get $res streams] {dict get $p last_scanned}]
            set maxid 18446744073709551615-18446744073709551615
            set res [r xread filter 1 streams short $maxid]
            assert_equal caught_up [dict get $res status]
            assert_equal $maxid [dict get [lindex [dict get $res streams] 0] last_scanned]
        }
    }
    r hello 2

    test {MAXSCAN validates positive integers and requires FILTER} {
        foreach value {0 -1 x 9223372036854775808} {
            assert_error {ERR *} {r xrange short - + filter 1 maxscan $value}
            assert_error {ERR *} {r xread filter 1 maxscan $value streams short 0}
        }
        assert_error {*requires FILTER*} {r xrange short - + maxscan 1}
        assert_error {*requires FILTER*} {r xrevrange short + - maxscan 1}
        assert_error {*requires FILTER*} {r xread maxscan 1 streams short 0}
        assert_error {*syntax*} {r xrange short - + filter 1 maxscan 1 maxscan 2}
        assert_error {*syntax*} {r xread filter 1 maxscan 1 maxscan 2 streams short 0}
        assert_error {*syntax*} {r xrange short - + filter 1 maxscan}
        assert_error {*syntax*} {r xread filter 1 maxscan}
        assert_error {*not supported by XREADGROUP*} {r xreadgroup group g c filter 1 maxscan 1 streams short >}
        assert_equal short [r command getkeys xread maxscan 1 filter STREAMS streams short 0]
    }

    test {Deleted entries consume MAXSCAN and advance range and XREAD cursors} {
        for {set i 1} {$i <= 5} {incr i} {r xadd deleted $i-0 amount $i}
        r xdel deleted 2-0 3-0 4-0
        foreach command {xrange xrevrange} {
            set bounds [expr {$command eq "xrange" ? {- +} : {+ -}}]
            set res [r $command deleted {*}$bounds filter 0 maxscan 3]
            assert_equal 3-0 [dict get $res last_scanned]
            assert_equal budget_exhausted [dict get $res status]
        }
        set res [r xread filter 0 maxscan 3 streams deleted 0]
        assert_equal budget_exhausted [dict get $res status]
        assert_equal 3-0 [dict get [lindex [dict get $res streams] 0] last_scanned]
        assert_equal {5-0} [maxscan_ids [dict get [lindex [dict get [r xread filter 1 maxscan 3 streams deleted 3-0] streams] 0] entries]]
    }

    test {Blocking FILTER returns immediately on budget exhaustion without matches} {
        set rd [redis_deferring_client]
        $rd xread block 0 filter 0 maxscan 2 streams short 0
        set res [$rd read]
        assert_equal budget_exhausted [dict get $res status]
        assert_equal 2-0 [dict get [lindex [dict get $res streams] 0] last_scanned]
        assert_equal 0 [s blocked_clients]
        $rd close
    }

    test {Blocking FILTER retains its scan budget across wakeups and resets next command} {
        set rd [redis_deferring_client]
        r xadd live 1-0 region us
        $rd xread block 5000 filter {.region == "eu"} maxscan 3 streams live 0
        wait_for_blocked_clients_count 1
        r xadd live 2-0 region us
        wait_for_blocked_clients_count 1
        r xadd live 3-0 region us
        wait_for_blocked_clients_count 0
        set res [$rd read]
        assert_equal budget_exhausted [dict get $res status]
        assert_equal 3-0 [dict get [lindex [dict get $res streams] 0] last_scanned]
        $rd xread filter 1 maxscan 2 streams live 0
        assert_equal {1-0 2-0} [maxscan_ids [dict get [lindex [dict get [$rd read] streams] 0] entries]]
        $rd close
    }

    foreach protocol {2 3} {
        test "Blocking FILTER timeout returns progress in RESP$protocol" {
            set rd [redis_deferring_client]
            $rd hello $protocol
            $rd read
            $rd xread block 150 filter 0 maxscan 10 streams live absent 0 $
            wait_for_blocked_clients_count 1
            wait_for_blocked_clients_count 0
            set res [$rd read]
            assert_equal timeout [dict get $res status]
            assert_equal {3-0 0-0} [lmap p [dict get $res streams] {dict get $p last_scanned}]
            $rd close
        }
    }

    test {CLIENT UNBLOCK TIMEOUT returns filtered progress and preserves connection reuse} {
        set rd [redis_deferring_client]
        $rd client id
        set id [$rd read]
        $rd xread block 0 filter 0 maxscan 10 streams live absent 0 19
        wait_for_blocked_clients_count 1
        assert_equal 1 [r client unblock $id timeout]
        set res [$rd read]
        assert_equal timeout [dict get $res status]
        assert_equal {3-0 19-0} [lmap p [dict get $res streams] {dict get $p last_scanned}]
        $rd xread filter 1 maxscan 1 streams live 0
        assert_equal budget_exhausted [dict get [$rd read] status]
        $rd close
    }

    test {A blocking match on the last budgeted entry is returned with exhaustion} {
        set rd [redis_deferring_client]
        r xadd last-match 1-0 region us
        $rd xread block 5000 filter {.region == "eu"} maxscan 2 streams last-match 0
        wait_for_blocked_clients_count 1
        r xadd last-match 2-0 region eu
        wait_for_blocked_clients_count 0
        set res [$rd read]
        assert_equal budget_exhausted [dict get $res status]
        set part [lindex [dict get $res streams] 0]
        assert_equal 2-0 [dict get $part last_scanned]
        assert_equal {2-0} [maxscan_ids [dict get $part entries]]
        $rd close
    }

    test {Filtered envelopes use RESP2 arrays and RESP3 maps on the wire} {
        set rd [redis_deferring_client]
        foreach protocol {2 3} {
            $rd hello $protocol
            $rd read
            foreach {command pairs} {{xrange absent - + filter 0} 3 {xread filter 0 streams absent 0} 2} {
                $rd readraw 1
                $rd {*}$command
                set header [$rd read]
                if {$protocol == 2} {
                    assert_equal "*[expr {$pairs * 2}]" $header
                } else {
                    assert_equal "%$pairs" $header
                }
                $rd readraw 0
                # Read each map key/value separately after consuming its header.
                set body {}
                for {set i 0} {$i < $pairs * 2} {incr i} {lappend body [$rd read]}
                assert {[dict exists $body status]}
            }
        }
        $rd close
    }

    test {FILTER envelopes work in EXEC and Lua without leaking scan budgets} {
        r multi
        r xrange short - + filter 0 maxscan 2
        r xread filter 1 maxscan 1 streams short 0
        r xread streams short 0
        set res [r exec]
        assert_equal 2-0 [dict get [lindex $res 0] last_scanned]
        assert_equal budget_exhausted [dict get [lindex $res 1] status]
        assert_equal {1-0 2-0 3-0 4-0} [maxscan_ids [lindex $res 2 0 1]]
        set res [r eval {
            return {redis.call('XREAD','FILTER','0','MAXSCAN',2,'STREAMS',KEYS[1],0),
                    redis.call('XREAD','FILTER','1','MAXSCAN',1,'STREAMS',KEYS[1],0)}
        } 1 short]
        assert_equal 2-0 [dict get [lindex [dict get [lindex $res 0] streams] 0] last_scanned]
        assert_equal {1-0} [maxscan_ids [dict get [lindex [dict get [lindex $res 1] streams] 0] entries]]
    }

    test {Unfiltered reads preserve their original reply structure and unlimited scan} {
        assert_equal 1002 [llength [r xrange backlog - +]]
        assert_equal 1002 [llength [r xrevrange backlog + -]]
        assert_equal 1002 [llength [lindex [r xread streams backlog 0] 0 1]]
        assert_equal {} [r xread streams backlog 1002-0]
    }
}
