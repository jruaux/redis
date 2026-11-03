# Requires a BUILD_COMPRESSION=yes server. Offline-trained dictionary and
# persistence/replication tests are also in tools/stream-compression-bench.py.
if {!$::compression} {return}

start_server {tags {"stream" "needs:debug" "needs:save"}} {
    r config set stream-node-max-entries 8
    r config set stream-node-max-bytes 4096

    test {Stream compression preserves bytes, field order, repeated and empty fields} {
        r xadd compressed 1-0 seed value
        r xadd plain 1-0 seed value
        r xcfgset compressed COMPRESSION ZSTD
        set value "[string repeat abcdef 80]\x00\xff"
        for {set i 2} {$i <= 100} {incr i} {
            foreach key {compressed plain} {
                r xadd $key $i-0 f $value f {} {} "00$i"
            }
        }
        assert_equal [r xrange plain - +] [r xrange compressed - +]
        assert_equal [r xrevrange plain + -] [r xrevrange compressed + -]
        assert_equal [lindex [r xread count 9 streams plain 0-0] 0 1] \
                     [lindex [r xread count 9 streams compressed 0-0] 0 1]
        assert {[r memory usage compressed] < [r memory usage plain]}
    }

    test {Compressed stream copy, tombstones, trimming and tail recovery} {
        r copy compressed copied
        assert_equal [r xrange compressed - +] [r xrange copied - +]
        foreach key {compressed plain} {
            assert_equal 3 [r xdel $key 2-0 9-0 100-0]
            r xtrim $key MAXLEN = 40
            r xadd $key 101-0 next [string repeat x 500]
        }
        assert_equal [r xrange plain - +] [r xrange compressed - +]
        foreach key {compressed plain} {r xtrim $key MINID ~ 90-0}
        assert_equal [r xrange plain - +] [r xrange compressed - +]
    }

    test {Compressed consumer delivery, pending reread, claim and deleted pending entry} {
        r xgroup create copied g 0
        set delivered [r xreadgroup group g alice count 10 streams copied >]
        assert_equal 10 [llength [lindex $delivered 0 1]]
        assert_equal [lindex $delivered 0 1] [lindex [r xreadgroup group g alice count 10 streams copied 0] 0 1]
        set claimed [r xclaim copied g bob 0 2-0]
        assert_equal [r xrange copied 2-0 2-0] $claimed
        r xdel copied 2-0
        assert_equal {{2-0 {}}} [lindex [r xreadgroup group g bob streams copied 0] 0 1]
        r xack copied g 2-0
        r xautoclaim copied g bob 0 0-0 count 20
        assert_equal 9 [lindex [r xpending copied g] 0]
    }

    test {Compression configuration validates atomically and OFF retains old data} {
        set before [r xrange copied - +]
        assert_error {*Invalid trained Zstd dictionary*} {r xcfgset copied COMPRESSION ZSTD DICTIONARY invalid}
        assert_error {*syntax*} {r xcfgset copied COMPRESSION invalid}
        r xcfgset copied COMPRESSION OFF
        assert_equal $before [r xrange copied - +]
    }

    test {Compressed reference-aware deletion and IDMP configuration retain semantics} {
        r xadd policies 1-0 f [string repeat x 500]
        r xcfgset policies COMPRESSION ZSTD
        for {set i 2} {$i <= 50} {incr i} {r xadd policies $i-0 f [string repeat x 500]}
        r xgroup create policies g 0
        r xreadgroup group g a count 10 streams policies >
        assert_equal {1} [r xdelex policies DELREF IDS 1 3-0]
        assert_equal {1} [r xackdel policies g DELREF IDS 1 4-0]
        assert_equal 8 [lindex [r xpending policies g] 0]
        assert_equal 48 [r xlen policies]
        r xnack policies g FAIL IDS 1 5-0
        assert_equal 1 [llength [lindex [r xreadgroup group g b count 1 claim 0 streams policies >] 0 1]]
        r xtrim policies MAXLEN = 10 DELREF
        assert_equal 10 [r xlen policies]
        r del policies

        r xadd idmp IDMP producer request-1 * f value
        r xcfgset idmp COMPRESSION ZSTD
        r xadd idmp IDMP producer request-1 * f value
        assert_equal 1 [r xlen idmp]
        assert_equal 1 [dict get [r xinfo stream idmp] iids-tracked]
        r del idmp
    }

    test {Plain Zstd policy and compressed memory survive DUMP RESTORE and RDB reload} {
        set compressed_before [r xrange compressed - +]
        r restore restoredplain 0 [r dump compressed]
        assert_equal 1 [dict get [dict get [r xinfo stream restoredplain] compression] enabled]
        assert_equal $compressed_before [r xrange restoredplain - +]
        set before [r xrange copied - +]
        r restore restored 0 [r dump copied]
        assert_equal $before [r xrange restored - +]
        r debug reload
        assert_equal 1 [dict get [dict get [r xinfo stream compressed] compression] enabled]
        assert_equal $compressed_before [r xrange restoredplain - +]
        assert {[r memory usage compressed] < [r memory usage plain]}
        assert_equal $before [r xrange copied - +]
        assert_equal [r xrange plain - +] [r xrange compressed - +]
        assert_equal 5 [r del copied restored restoredplain compressed plain]
    }
}
