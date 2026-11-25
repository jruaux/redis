# Exercise compiled-predicate ownership repeatedly, with explicit allocator
# growth assertions even when no external leak detector is available.
proc stream_filter_leak_reads {iterations} {
    set predicate {(.region in ["eu", "apac"] and .amount > 100) or .region == "us"}
    for {set i 0} {$i < $iterations} {incr i} {
        assert_equal 1 [llength [r xrange leak-source - + filter $predicate]]
        assert_equal 1 [llength [r xrevrange leak-source + - count 1 filter $predicate]]
        assert_equal 1 [llength [lindex [r xread filter $predicate streams leak-source 0] 0 1]]
        # Fail resolution after pushing a borrowed string onto the VM stack.
        assert_equal {} [r xrange leak-source - + filter {.region and .missing}]
        assert_equal {} [r xread filter {.region and .missing} streams leak-source 0]
        # These paths compile a predicate but never deliver an entry.
        assert_equal {} [r xrange leak-absent - + filter $predicate]
        assert_equal {} [r xread filter $predicate streams leak-absent 0]
        assert_error {*WRONGTYPE*} {r xrange leak-wrongtype - + filter $predicate}
        assert_error {*WRONGTYPE*} {r xread filter $predicate streams leak-wrongtype 0}
        # Compile failure after creating tokens, tuple values and program state.
        assert_error {*syntax error in FILTER*} {r xrange leak-source - + filter "$predicate and ("}
        assert_error {*syntax error in FILTER*} {r xread filter "$predicate and (" streams leak-source 0}
        r multi
        r xread filter $predicate streams leak-source 0
        r xread filter 0 streams leak-source 0
        set result [r exec]
        assert_equal 1 [llength [lindex $result 0]]
        assert_equal {} [lindex $result 1]
        set result [r eval {
            return {redis.call('XREAD', 'FILTER', ARGV[1], 'STREAMS', KEYS[1], '0'),
                    redis.call('XREAD', 'FILTER', '0', 'STREAMS', KEYS[1], '0')}
        } 1 leak-source $predicate]
        assert_equal 1 [llength [lindex $result 0]]
        assert_equal {} [lindex $result 1]
    }
}

proc stream_filter_leak_blocked {mode} {
    set clients [s connected_clients]
    r xadd leak-live 1-0 region us amount 10
    set rd [redis_deferring_client]
    $rd client id
    set id [$rd read]
    # All waits are bounded, even when a regression prevents completion.
    set timeout [expr {$mode eq "timeout" ? 500 : 5000}]
    $rd xread block $timeout filter {.region in ["eu", "apac"] and .amount >= 500} streams leak-live 0
    wait_for_blocked_clients_count 1
    for {set i 2} {$i <= 4} {incr i} {
        r xadd leak-live $i-0 region us amount 900
        assert_equal 1 [s blocked_clients]
    }
    switch -- $mode {
        success {
            r xadd leak-live 5-0 region eu amount 600
            wait_for_blocked_clients_count 0
            assert_equal {{leak-live {{5-0 {region eu amount 600}}}}} [$rd read]
        }
        timeout {
            wait_for_blocked_clients_count 0
            assert_equal {} [$rd read]
        }
        unblock-timeout {
            assert_equal 1 [r client unblock $id timeout]
            assert_equal {} [$rd read]
        }
        unblock-error {
            assert_equal 1 [r client unblock $id error]
            assert_error {*UNBLOCKED*} {$rd read}
        }
        disconnect {}
        default {fail "Unknown cleanup mode $mode"}
    }
    if {$mode ne "disconnect"} {
        # Reuse the connection to catch stale predicates as well as lost ones.
        $rd xread count 1 filter 1 streams leak-live 0
        assert_equal {{leak-live {{1-0 {region us amount 10}}}}} [$rd read]
    }
    $rd close
    wait_for_blocked_clients_count 0
    wait_for_condition 100 10 {[s connected_clients] == $clients} else {
        fail "Filtered reader connection was not freed"
    }
    r del leak-live
}

start_server {tags {"stream" "external:skip" "cluster:skip"}} {
    # Avoid spending a cron interval on every repeated timeout.
    r config set hz 100
    r xadd leak-source 1-0 region eu amount 150
    r set leak-wrongtype string

    test {Compiled stream filters release memory after 2000 read and error cycles} {
        # Populate command/error statistics and stabilize the query/reply buffers
        # before taking the allocator baseline. RSS would include retained arenas.
        stream_filter_leak_reads 20
        set before [s used_memory]
        for {set batch 0} {$batch < 8} {incr batch} {
            stream_filter_leak_reads 250
            set growth [expr {[s used_memory] - $before}]
            assert_lessthan $growth 65536 "Compiled filter memory grew by $growth bytes"
        }
    }

    test {Compiled stream filters release memory after 200 blocked cleanup cycles} {
        set modes {success timeout unblock-timeout unblock-error disconnect}
        foreach mode $modes {stream_filter_leak_blocked $mode}
        set before [s used_memory]
        for {set batch 0} {$batch < 4} {incr batch} {
            for {set i 0} {$i < 10} {incr i} {
                foreach mode $modes {stream_filter_leak_blocked $mode}
            }
            set growth [expr {[s used_memory] - $before}]
            assert_lessthan $growth 65536 "Blocked filter memory grew by $growth bytes"
        }
    }
}
