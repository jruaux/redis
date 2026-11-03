/* Bounded native load driver for stream-plain-bench.py. Uses repository
 * hiredis and HdrHistogram; reports enqueue-to-parsed-reply latency and the
 * Redis process CPU counters, separately from client CPU. */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>
#include <math.h>
#include <errno.h>
#include <limits.h>
#include <hiredis.h>
#include "hdr_histogram.h"

/* The repository's histogram library expects these allocator symbols. */
void *zmalloc(size_t n) { return malloc(n); }
void *zcalloc_num(size_t n, size_t size) { return calloc(n,size); }
void *zrealloc(void *p, size_t n) { return realloc(p,n); }
void zfree(void *p) { free(p); }

enum {ADD, RANGE, READ, TRIM, OPS};
static const char *names[] = {"xadd","xrange","xread","xtrim"};
typedef struct { char *bytes; uint32_t len; } command;
typedef struct {
    unsigned index;
    struct hdr_histogram *lat[OPS];
    uint64_t entries, deleted;
} worker;
static command *records;
static unsigned record_count, clients, depth, retained, page;
static const char *socket_path, *phase;
static double seconds;
static uint64_t deadline;
static pthread_mutex_t gate_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t gate_cond = PTHREAD_COND_INITIALIZER;
static unsigned ready;
static int go;
static atomic_int failed;

static uint64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC,&t);
    return (uint64_t)t.tv_sec*1000000000ULL+t.tv_nsec;
}

static void fail(const char *message) {
    if (!atomic_exchange(&failed,1)) fprintf(stderr,"%s\n",message);
}

static redisContext *connect_server(void) {
    struct timeval timeout = {10,0};
    redisContext *c = redisConnectUnixWithTimeout(socket_path,timeout);
    if (!c || c->err || redisSetTimeout(c,timeout) != REDIS_OK) {
        fail(c ? c->errstr : "Cannot allocate hiredis context");
        if (c) redisFree(c);
        return NULL;
    }
    return c;
}

static uint32_t read32(FILE *f) {
    unsigned char p[4];
    if (fread(p,1,4,f) != 4) { fprintf(stderr,"Truncated command file\n"); exit(2); }
    return ((uint32_t)p[0]<<24)|((uint32_t)p[1]<<16)|((uint32_t)p[2]<<8)|p[3];
}

static void load_records(const char *path) {
    FILE *f = fopen(path,"rb");
    if (!f) { perror(path); exit(2); }
    record_count = read32(f);
    if (!record_count || record_count > 10000) { fprintf(stderr,"Invalid record count\n"); exit(2); }
    records = calloc(record_count,sizeof(*records));
    if (!records) exit(2);
    size_t total = 0;
    for (unsigned i=0; i<record_count; i++) {
        uint32_t len = read32(f);
        total += len;
        if (!len || len > 65536 || total > 128*1024*1024) {
            fprintf(stderr,"Command file exceeds bounds\n"); exit(2);
        }
        records[i].len = len;
        records[i].bytes = malloc(len);
        if (!records[i].bytes || fread(records[i].bytes,1,len,f) != len) exit(2);
    }
    if (fgetc(f) != EOF) { fprintf(stderr,"Trailing command file bytes\n"); exit(2); }
    fclose(f);
}

static void *run_worker(void *opaque) {
    worker *w = opaque;
    redisContext *c = connect_server();
    for (int i=0; i<OPS; i++)
        if (hdr_init(1,60000000,3,&w->lat[i])) fail("Cannot allocate histogram");
    pthread_mutex_lock(&gate_mutex);
    ready++;
    pthread_cond_broadcast(&gate_cond);
    while (!go) pthread_cond_wait(&gate_cond,&gate_mutex);
    pthread_mutex_unlock(&gate_mutex);
    uint64_t sequence = w->index*997;
    int reading = !strcmp(phase,"small-reads") || !strcmp(phase,"large-reads");
    int trimming = !strcmp(phase,"approx-trim") || !strcmp(phase,"exact-trim");
    while (!atomic_load(&failed) && now_ns() < deadline) {
        uint64_t starts[32];
        int kinds[32];
        for (unsigned j=0; j<depth; j++) {
            int rc;
            starts[j] = now_ns();
            if (reading) {
                unsigned offset = (sequence++*97) % (retained-page);
                kinds[j] = (sequence&1) ? RANGE : READ;
                if (kinds[j] == RANGE)
                    rc = redisAppendCommand(c,"XRANGE s %u-0 + COUNT %u",offset+1,page);
                else
                    rc = redisAppendCommand(c,"XREAD COUNT %u STREAMS s %u-0",page,offset);
            } else if (trimming && j == depth-1) {
                kinds[j] = TRIM;
                rc = redisAppendCommand(c,"XTRIM s MAXLEN %s %u",
                        !strcmp(phase,"exact-trim") ? "=" : "~",retained);
            } else {
                kinds[j] = ADD;
                command *record = &records[sequence++ % record_count];
                rc = redisAppendFormattedCommand(c,record->bytes,record->len);
            }
            if (rc != REDIS_OK) { fail(c->errstr); break; }
        }
        if (atomic_load(&failed)) break;
        for (unsigned j=0; j<depth; j++) {
            redisReply *reply = NULL;
            if (redisGetReply(c,(void**)&reply) != REDIS_OK || !reply) {
                fail(c->errstr); break;
            }
            uint64_t us = (now_ns()-starts[j]+999)/1000;
            hdr_record_value_capped(w->lat[kinds[j]],(int64_t)us);
            if (reply->type == REDIS_REPLY_ERROR) fail(reply->str);
            if (kinds[j] == ADD) {
                if (reply->type != REDIS_REPLY_STRING) fail("Invalid XADD reply");
                w->entries++;
            } else if (kinds[j] == TRIM) {
                if (reply->type != REDIS_REPLY_INTEGER || reply->integer < 0) fail("Invalid XTRIM reply");
                else w->deleted += reply->integer;
            } else {
                redisReply *entries = reply;
                if (kinds[j] == READ) {
                    if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 1 ||
                        reply->element[0]->type != REDIS_REPLY_ARRAY || reply->element[0]->elements != 2)
                        entries = NULL;
                    else entries = reply->element[0]->element[1];
                }
                if (!entries || entries->type != REDIS_REPLY_ARRAY || entries->elements != page)
                    fail("Unexpected read page length");
                else w->entries += entries->elements;
            }
            freeReplyObject(reply);
        }
    }
    if (c) redisFree(c);
    return NULL;
}

static char *info(redisContext *c, const char *section) {
    redisReply *r = redisCommand(c,"INFO %s",section);
    if (!r || r->type != REDIS_REPLY_STRING) {
        fprintf(stderr,"Cannot read INFO %s\n",section); exit(2);
    }
    char *copy = strdup(r->str);
    freeReplyObject(r);
    if (!copy) exit(2);
    return copy;
}

static double number(const char *text, const char *key) {
    const char *p = strstr(text,key);
    if (!p) { fprintf(stderr,"Missing INFO field %s\n",key); exit(2); }
    return strtod(p+strlen(key),NULL);
}

static double stat(const char *text, const char *name, const char *field) {
    char key[80];
    snprintf(key,sizeof(key),"cmdstat_%s:",name);
    const char *p = strstr(text,key);
    if (!p) return 0;
    const char *end = strchr(p,'\n');
    p = strstr(p,field);
    return p && (!end || p < end) ? strtod(p+strlen(field),NULL) : 0;
}

static unsigned uint_argument(const char *s) {
    char *end;
    errno=0;
    unsigned long n=strtoul(s,&end,10);
    if (errno || !*s || *end || *s=='-' || n>UINT_MAX) exit(2);
    return n;
}

int main(int argc, char **argv) {
    if (argc != 9) {
        fprintf(stderr,"usage: %s socket commands phase seconds clients pipeline retained page\n",argv[0]); return 2;
    }
    char *end;
    errno=0;
    socket_path=argv[1]; phase=argv[3]; seconds=strtod(argv[4],&end);
    if (errno || !*argv[4] || *end) return 2;
    clients=uint_argument(argv[5]); depth=uint_argument(argv[6]);
    retained=uint_argument(argv[7]); page=uint_argument(argv[8]);
    if (!isfinite(seconds) || seconds<0.2 || seconds>5 || !clients || clients>4 ||
        depth<2 || depth>8 || retained!=10000 || !page || page>1000 ||
        (strcmp(phase,"write-retain") && strcmp(phase,"small-reads") &&
         strcmp(phase,"large-reads") && strcmp(phase,"approx-trim") && strcmp(phase,"exact-trim"))) return 2;
    if (strcmp(argv[2],"-")) load_records(argv[2]);
    if ((!strcmp(phase,"write-retain") || strstr(phase,"trim")) && !records) return 2;
    pthread_t threads[4];
    worker workers[4] = {{0}};
    redisContext *control = connect_server();
    if (!control) return 2;
    for (unsigned i=0; i<clients; i++) {
        workers[i].index=i;
        if (pthread_create(&threads[i],NULL,run_worker,&workers[i])) return 2;
    }
    pthread_mutex_lock(&gate_mutex);
    while (ready != clients) pthread_cond_wait(&gate_cond,&gate_mutex);
    char *cpu_before=info(control,"cpu"), *stats_before=info(control,"commandstats");
    uint64_t start=now_ns();
    deadline=start+(uint64_t)(seconds*1e9);
    go=1;
    pthread_cond_broadcast(&gate_cond);
    pthread_mutex_unlock(&gate_mutex);
    for (unsigned i=0; i<clients; i++) pthread_join(threads[i],NULL);
    double wall=(now_ns()-start)/1e9;
    char *cpu_after=info(control,"cpu"), *stats_after=info(control,"commandstats");
    redisFree(control);
    if (atomic_load(&failed)) return 1;
    double user=number(cpu_after,"used_cpu_user:")-number(cpu_before,"used_cpu_user:");
    double sys=number(cpu_after,"used_cpu_sys:")-number(cpu_before,"used_cpu_sys:");
    uint64_t total=0, entries=0, deleted=0;
    for (unsigned i=0; i<clients; i++) {
        entries+=workers[i].entries; deleted+=workers[i].deleted;
        for (int op=0; op<OPS; op++) total+=workers[i].lat[op]->total_count;
    }
    printf("{\"phase\":\"%s\",\"wall_seconds\":%.9f,\"operations\":%llu,\"entries\":%llu,"
           "\"deleted_entries\":%llu,\"ops_s\":%.6f,\"entries_s\":%.6f,"
           "\"server_cpu_user_s\":%.9f,\"server_cpu_sys_s\":%.9f,\"server_cpu_percent_core\":%.6f,"
           "\"server_cpu_us_per_op\":%.6f,\"server_cpu_us_per_entry\":%.6f,\"commands\":{",
           phase,wall,(unsigned long long)total,(unsigned long long)entries,(unsigned long long)deleted,
           total/wall,entries/wall,user,sys,(user+sys)/wall*100,(user+sys)*1e6/total,
           entries ? (user+sys)*1e6/entries : 0);
    int separator=0;
    for (int op=0; op<OPS; op++) {
        struct hdr_histogram *merged=NULL;
        if (hdr_init(1,60000000,3,&merged)) return 2;
        for (unsigned i=0; i<clients; i++) hdr_add(merged,workers[i].lat[op]);
        if (merged->total_count) {
            double calls=stat(stats_after,names[op],"calls=")-stat(stats_before,names[op],"calls=");
            double usec=stat(stats_after,names[op],"usec=")-stat(stats_before,names[op],"usec=");
            printf("%s\"%s\":{\"calls\":%lld,\"p50_us\":%lld,\"p95_us\":%lld,\"p99_us\":%lld,"
                   "\"max_us\":%lld,\"mean_us\":%.6f,\"server_calls\":%.0f,\"server_command_wall_us\":%.0f,"
                   "\"server_command_wall_us_per_call\":%.6f}",separator++ ? "," : "",names[op],
                   (long long)merged->total_count,(long long)hdr_value_at_percentile(merged,50),
                   (long long)hdr_value_at_percentile(merged,95),(long long)hdr_value_at_percentile(merged,99),
                   (long long)hdr_max(merged),hdr_mean(merged),calls,usec,calls ? usec/calls : 0);
        }
        hdr_close(merged);
    }
    printf("}}\n");
    for (unsigned i=0; i<clients; i++) for (int op=0; op<OPS; op++) hdr_close(workers[i].lat[op]);
    for (unsigned i=0; i<record_count; i++) free(records[i].bytes);
    free(records); free(cpu_before); free(cpu_after); free(stats_before); free(stats_after);
    return 0;
}
