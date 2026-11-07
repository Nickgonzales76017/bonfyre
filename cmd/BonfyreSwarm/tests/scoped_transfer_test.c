/* Real native peers, temporary grants and bytes, IPv4 loopback only.
 * The fixture's MessagePack packer/reader and frame handling are its own, so
 * the native packet and proof contract are exercised independently of the
 * organ's codec. Proofs use keyed BLAKE2b (digest 32) from bf_crypto.
 *   scoped_transfer_test BONFYRE_SWARM_BINARY */
#define _DEFAULT_SOURCE
#define _DARWIN_C_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include "bf_crypto.h"
#include "bonfyre.h"

static const char *binary;
static char root[512], source_path[600], key_path[600];
static unsigned char key[32], *data; static size_t data_len;
static char opt_scope[65], opt_recipient[65], opt_sha[65], opt_size[32], opt_transfer[33], opt_expires[32], opt_key[600];
static int failures, checks, serial;
#define CHECK(cond, ...) do { checks++; if (!(cond)) { failures++; fprintf(stderr, "FAIL %s:%d ", __func__, __LINE__); fprintf(stderr, __VA_ARGS__); fputc('\n', stderr); } } while (0)

/* ---------- fixture MessagePack ---------- */
typedef struct { unsigned char *b; size_t n, cap; } Buf;
static void put(Buf *o, const void *p, size_t n) { if (o->n + n > o->cap) { o->cap = (o->n + n) * 2 + 64; o->b = realloc(o->b, o->cap); } memcpy(o->b + o->n, p, n); o->n += n; }
static void put8(Buf *o, unsigned v) { unsigned char c = (unsigned char)v; put(o, &c, 1); }
static void p_uint(Buf *o, uint64_t v) { if (v < 128) { put8(o, (unsigned)v); return; } put8(o, 0xcf); for (int i = 7; i >= 0; i--) put8(o, (unsigned)(v >> (i * 8)) & 255); }
static void p_str(Buf *o, const char *s) { size_t n = strlen(s); if (n < 32) put8(o, 0xa0 + (unsigned)n); else { put8(o, 0xda); put8(o, (unsigned)(n >> 8)); put8(o, (unsigned)n & 255); } put(o, s, n); }
static void p_bin(Buf *o, const unsigned char *p, size_t n) { put8(o, 0xc5); put8(o, (unsigned)(n >> 8)); put8(o, (unsigned)n & 255); put(o, p, n); }
static void p_arr(Buf *o, unsigned n) { put8(o, 0x90 + n); }

typedef enum { V_INT, V_BOOL, V_STR, V_BIN, V_ARR, V_MAP } VType;
typedef struct Val { VType t; uint64_t i; unsigned char *s; size_t n; struct Val *items; struct Val *keys; size_t count; } Val;
static const unsigned char *rp, *re;
static int rd(size_t n) { return (size_t)(re - rp) >= n; }
static uint64_t num(int n) { uint64_t v = 0; for (int i = 0; i < n; i++) v = v * 256 + *rp++; return v; }
static int one(Val *v) {
    unsigned t; if (!rd(1)) return 0; t = *rp++; memset(v, 0, sizeof *v);
    if (t < 128) { v->t = V_INT; v->i = t; return 1; }
    if (t == 0xc2 || t == 0xc3) { v->t = V_BOOL; v->i = t == 0xc3; return 1; }
    if (t >= 0xcc && t <= 0xcf) { int n = 1 << (t - 0xcc); if (!rd((size_t)n)) return 0; v->t = V_INT; v->i = num(n); return 1; }
    if ((t >= 0xa0 && t <= 0xbf) || (t >= 0xd9 && t <= 0xdb) || (t >= 0xc4 && t <= 0xc6)) {
        size_t n; v->t = (t >= 0xc4 && t <= 0xc6) ? V_BIN : V_STR;
        if (t >= 0xa0 && t <= 0xbf) n = t - 0xa0; else { int w = 1 << ((t >= 0xd9 ? t - 0xd9 : t - 0xc4)); if (!rd((size_t)w)) return 0; n = (size_t)num(w); }
        if (!rd(n)) return 0; v->s = malloc(n + 1); memcpy(v->s, rp, n); v->s[n] = 0; v->n = n; rp += n; return 1; }
    if (t >= 0x90 && t <= 0x9f) { v->t = V_ARR; v->count = t - 0x90; v->items = calloc(v->count + 1, sizeof(Val)); for (size_t i = 0; i < v->count; i++) if (!one(&v->items[i])) return 0; return 1; }
    if (t >= 0x80 && t <= 0x8f) { v->t = V_MAP; v->count = t - 0x80; v->keys = calloc(v->count + 1, sizeof(Val)); v->items = calloc(v->count + 1, sizeof(Val)); for (size_t i = 0; i < v->count; i++) if (!one(&v->keys[i]) || !one(&v->items[i])) return 0; return 1; }
    return 0;
}
static int unpack(const unsigned char *b, size_t n, Val *v) { rp = b; re = b + n; return one(v) && rp == re; }
static int val_eq(const Val *a, const Val *b) {
    if (a->t != b->t || a->count != b->count) return 0;
    if (a->t == V_INT || a->t == V_BOOL) return a->i == b->i;
    if (a->t == V_STR || a->t == V_BIN) return a->n == b->n && !memcmp(a->s, b->s, a->n);
    for (size_t i = 0; i < a->count; i++) if (!val_eq(&a->items[i], &b->items[i]) || (a->keys && !val_eq(&a->keys[i], &b->keys[i]))) return 0;
    return 1;
}
static Val *get(Val *m, const char *k) { if (!m || m->t != V_MAP) return NULL; for (size_t i = 0; i < m->count; i++) if (m->keys[i].t == V_STR && !strcmp((char *)m->keys[i].s, k)) return &m->items[i]; return NULL; }
static const char *sget(Val *m, const char *k) { Val *v = get(m, k); return v && v->t == V_STR ? (const char *)v->s : ""; }
static long long iget(Val *m, const char *k) { Val *v = get(m, k); return v && (v->t == V_INT || v->t == V_BOOL) ? (long long)v->i : -1; }

/* ---------- helpers ---------- */
static void hex2bin(const char *h, unsigned char *o, size_t n) { for (size_t i = 0; i < n; i++) sscanf(h + 2 * i, "%2hhx", &o[i]); }
static void sha_hex(const void *p, size_t n, char out[65]) { bf_sha256_hex((const uint8_t *)p, n, out); }
static void rnd(unsigned char *p, size_t n) { bf_crypto_random_buf(p, n); }
static int contains(const unsigned char *h, size_t hn, const void *nd, size_t nn) { if (!nn || hn < nn) return 0; for (size_t i = 0; i + nn <= hn; i++) if (!memcmp(h + i, nd, nn)) return 1; return 0; }
static int exists(const char *p) { struct stat s; return lstat(p, &s) == 0; }
static int tmp_files(void) { char cmd[700]; snprintf(cmd, sizeof cmd, "ls -a '%s' | grep -c '^\\.swarm-.*\\.tmp$' >/dev/null", root); return system(cmd) == 0; }
static char *db_one(const char *path, const char *q) {
    static char out[256]; sqlite3 *db; sqlite3_stmt *st; out[0] = 0;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { sqlite3_close(db); return out; }
    if (sqlite3_prepare_v2(db, q, -1, &st, NULL) == SQLITE_OK && sqlite3_step(st) == SQLITE_ROW && sqlite3_column_text(st, 0)) snprintf(out, sizeof out, "%s", sqlite3_column_text(st, 0));
    sqlite3_finalize(st); sqlite3_close(db); return out;
}

/* Options with per-call overrides as NAME=VALUE pairs. */
typedef struct { const char *k, *v; } Kv;
static int build_argv(const char *mode, const Kv *ch, size_t nch, const Kv *extra, size_t nx, char **argv) {
    static char store[64][700]; int a = 0, s = 0;
    const Kv base[] = {{"scope", opt_scope}, {"recipient", opt_recipient}, {"sha256", opt_sha}, {"size", opt_size},
                       {"transfer-id", opt_transfer}, {"key-file", opt_key}, {"expires", opt_expires}};
    argv[a++] = (char *)binary; argv[a++] = (char *)mode;
    for (size_t i = 0; i < 7; i++) { const char *v = base[i].v; for (size_t j = 0; j < nch; j++) if (!strcmp(ch[j].k, base[i].k)) v = ch[j].v;
        snprintf(store[s], 700, "--%s", base[i].k); argv[a++] = store[s++]; snprintf(store[s], 700, "%s", v); argv[a++] = store[s++]; }
    for (size_t j = 0; j < nx; j++) { int known = 0; for (size_t i = 0; i < 7; i++) if (!strcmp(extra[j].k, base[i].k)) known = 1; if (known) continue;
        snprintf(store[s], 700, "--%s", extra[j].k); argv[a++] = store[s++]; snprintf(store[s], 700, "%s", extra[j].v); argv[a++] = store[s++]; }
    argv[a] = NULL; return a;
}
typedef struct { int rc; unsigned char out[65536]; size_t on; char err[8192]; size_t en; Val pkt; } Run;
static void drain(int fd, unsigned char *b, size_t cap, size_t *n) { ssize_t r; *n = 0; while ((r = read(fd, b + *n, cap - 1 - *n)) > 0) { *n += (size_t)r; if (*n >= cap - 1) break; } b[*n] = 0; }
static void run_argv(char **argv, Run *r) {
    int o[2], e[2]; pid_t pid; int st; pipe(o); pipe(e); memset(r, 0, sizeof *r);
    if ((pid = fork()) == 0) { dup2(o[1], 1); dup2(e[1], 2); close(o[0]); close(e[0]); execv(argv[0], argv); _exit(127); }
    close(o[1]); close(e[1]); drain(o[0], r->out, sizeof r->out, &r->on); drain(e[0], (unsigned char *)r->err, sizeof r->err, &r->en);
    close(o[0]); close(e[0]); waitpid(pid, &st, 0); r->rc = WIFEXITED(st) ? WEXITSTATUS(st) : 128;
}
static Val *native(Run *r) { /* packet [1, ok, receipt] with exit_status and no key or source disclosure */
    CHECK(unpack(r->out, r->on, &r->pkt), "native reply is MessagePack");
    CHECK(r->pkt.t == V_ARR && r->pkt.count == 3 && r->pkt.items[0].i == 1, "packet version");
    CHECK(r->pkt.count > 1 && (int)r->pkt.items[1].i == (r->rc == 0), "packet ok matches exit");
    CHECK(r->pkt.count > 2 && iget(&r->pkt.items[2], "exit_status") == r->rc, "exit_status matches");
    CHECK(!contains(r->out, r->on, key, 32) && !contains((unsigned char *)r->err, r->en, key, 32), "key not disclosed");
    CHECK(!contains(r->out, r->on, source_path, strlen(source_path)), "source path not disclosed");
    return r->pkt.count > 2 ? &r->pkt.items[2] : NULL;
}
typedef struct { pid_t pid; int out, err; int port; } Seed;
static Seed start_seed(const Kv *ch, size_t n) {
    Seed s = {0}; char *argv[64], port0[] = "0", idx[640]; int o[2], e[2]; char line[256] = ""; size_t ln = 0; struct pollfd p;
    snprintf(idx, sizeof idx, "%s/sender.db", root);
    Kv extra[] = {{"artifact", source_path}, {"port", port0}, {"index", idx}};
    Kv merged[16]; size_t m = 0; for (size_t i = 0; i < 3; i++) { merged[m] = extra[i]; for (size_t j = 0; j < n; j++) if (!strcmp(ch[j].k, extra[i].k)) merged[m].v = ch[j].v; m++; }
    build_argv("scoped-seed", ch, n, merged, m, argv); pipe(o); pipe(e);
    if ((s.pid = fork()) == 0) { dup2(o[1], 1); dup2(e[1], 2); close(o[0]); close(e[0]); execv(argv[0], argv); _exit(127); }
    close(o[1]); close(e[1]); s.out = o[0]; s.err = e[0];
    p.fd = s.err; p.events = POLLIN;
    while (ln < sizeof line - 1 && poll(&p, 1, 4000) > 0) { char c; if (read(s.err, &c, 1) != 1) break; if (c == '\n') break; line[ln++] = c; }
    line[ln] = 0;
    CHECK(!strncmp(line, "SWARM_READY 127.0.0.1 ", 22), "seed ready line: %s", line);
    s.port = atoi(strrchr(line, ' ') ? strrchr(line, ' ') + 1 : "0");
    return s;
}
static int finish_seed(Seed *s, Val *receipt_out, int *rc_out) {
    Run r; int st; memset(&r, 0, sizeof r);
    drain(s->out, r.out, sizeof r.out, &r.on); drain(s->err, (unsigned char *)r.err, sizeof r.err, &r.en);
    close(s->out); close(s->err); waitpid(s->pid, &st, 0); r.rc = WIFEXITED(st) ? WEXITSTATUS(st) : 128;
    CHECK(!contains(r.out, r.on, key, 32) && !contains((unsigned char *)r.err, r.en, key, 32), "seed key not disclosed");
    Val v; int ok = unpack(r.out, r.on, &v) && v.t == V_ARR && v.count == 3;
    CHECK(ok, "seed final packet"); if (ok) { CHECK(iget(&v.items[2], "exit_status") == r.rc, "seed exit_status"); if (receipt_out) *receipt_out = v.items[2]; }
    if (rc_out) *rc_out = r.rc; return r.rc;
}
typedef struct { int rc; Val *receipt; char out[640], index[640]; Run run; } Fetch;
static Fetch *fetch(int port, const Kv *ch, size_t n) {
    static Fetch f; char *argv[64], ports[16]; memset(&f, 0, sizeof f); serial++;
    snprintf(ports, sizeof ports, "%d", port); snprintf(f.out, sizeof f.out, "%s/received-%d.bin", root, serial); snprintf(f.index, sizeof f.index, "%s/receiver-%d.db", root, serial);
    Kv extra[] = {{"port", ports}, {"out", f.out}, {"index", f.index}}; Kv merged[16]; size_t m = 0;
    for (size_t i = 0; i < 3; i++) { merged[m] = extra[i]; for (size_t j = 0; j < n; j++) if (!strcmp(ch[j].k, extra[i].k)) merged[m].v = ch[j].v; m++; }
    for (size_t j = 0; j < n; j++) { int dup = 0; for (size_t i = 0; i < 3; i++) if (!strcmp(ch[j].k, extra[i].k)) dup = 1; if (!dup) merged[m++] = ch[j]; }
    for (size_t i = 0; i < 3; i++) if (!strcmp(extra[i].k, "out")) for (size_t j = 0; j < n; j++) if (!strcmp(ch[j].k, "out")) snprintf(f.out, sizeof f.out, "%s", ch[j].v);
    for (size_t j = 0; j < n; j++) if (!strcmp(ch[j].k, "index")) snprintf(f.index, sizeof f.index, "%s", ch[j].v);
    build_argv("scoped-fetch", ch, n, merged, m, argv); run_argv(argv, &f.run); f.rc = f.run.rc; f.receipt = native(&f.run); return &f;
}
static void proof(const char *kind, const unsigned char nonce[32], const char *scope, const char *recipient, const char *sha, unsigned char out[32]) {
    unsigned char m[24 + 32 + 16 + 32 * 3 + 16]; size_t n = 0; char label[24] = {0}; uint64_t size = strtoull(opt_size, NULL, 10), exp = strtoull(opt_expires, NULL, 10);
    snprintf(label, sizeof label, "bf.swarm.v1.%s", kind); memcpy(m, label, 24); n = 24; memcpy(m + n, nonce, 32); n += 32;
    hex2bin(opt_transfer, m + n, 16); n += 16; hex2bin(scope, m + n, 32); n += 32; hex2bin(recipient, m + n, 32); n += 32; hex2bin(sha, m + n, 32); n += 32;
    for (int i = 7; i >= 0; i--) m[n++] = (unsigned char)(size >> (i * 8)); for (int i = 7; i >= 0; i--) m[n++] = (unsigned char)(exp >> (i * 8));
    bf_crypto_hash(out, m, n, key, 32);
}
static void request(Buf *o, const unsigned char nonce[32]) {
    unsigned char t[16], s[32], r[32], h[32], pr[32]; hex2bin(opt_transfer, t, 16); hex2bin(opt_scope, s, 32); hex2bin(opt_recipient, r, 32); hex2bin(opt_sha, h, 32);
    proof("request", nonce, opt_scope, opt_recipient, opt_sha, pr);
    p_arr(o, 8); p_uint(o, 1); p_str(o, "request"); p_bin(o, t, 16); p_bin(o, s, 32); p_bin(o, r, 32); p_bin(o, h, 32); p_uint(o, strtoull(opt_size, NULL, 10)); p_bin(o, pr, 32);
}
static int recv_all(int fd, unsigned char *b, size_t n) { size_t got = 0; while (got < n) { ssize_t r = recv(fd, b + got, n - got, 0); if (r <= 0) return 0; got += (size_t)r; } return 1; }
static int read_frame(int fd, Val *v, unsigned char **raw, size_t *rawn) {
    unsigned char h[4]; uint32_t len; if (!recv_all(fd, h, 4)) return 0; len = ((uint32_t)h[0] << 24) | (h[1] << 16) | (h[2] << 8) | h[3];
    if (!len || len > 1024) return 0; *raw = malloc(len); *rawn = len; if (!recv_all(fd, *raw, len)) return 0; return unpack(*raw, len, v);
}
static void send_frame(int fd, const Buf *p, int fragmented) {
    unsigned char h[4] = {(unsigned char)(p->n >> 24), (unsigned char)(p->n >> 16), (unsigned char)(p->n >> 8), (unsigned char)p->n};
    if (fragmented) { for (int i = 0; i < 4; i++) send(fd, h + i, 1, 0); for (size_t i = 0; i < p->n; i++) send(fd, p->b + i, 1, 0); }
    else { send(fd, h, 4, 0); send(fd, p->b, p->n, 0); }
}
static int connect_port(int port) { int fd = socket(AF_INET, SOCK_STREAM, 0); struct sockaddr_in a = {0}; struct timeval tv = {5, 0}; a.sin_family = AF_INET; a.sin_port = htons((uint16_t)port); a.sin_addr.s_addr = htonl(INADDR_LOOPBACK); setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv); if (connect(fd, (struct sockaddr *)&a, sizeof a)) { close(fd); return -1; } return fd; }
static int peer(int port, unsigned char nonce[32]) {
    int fd = connect_port(port); Val v; unsigned char *raw = NULL, t[16]; size_t rn = 0; hex2bin(opt_transfer, t, 16);
    CHECK(fd >= 0 && read_frame(fd, &v, &raw, &rn), "challenge frame");
    CHECK(v.t == V_ARR && v.count == 4 && v.items[0].i == 1 && !strcmp((char *)v.items[1].s, "challenge") && v.items[2].n == 16 && !memcmp(v.items[2].s, t, 16), "challenge shape");
    CHECK(!contains(raw, rn, key, 32) && v.items[3].n == 32, "challenge nonce, no key");
    if (v.count == 4 && v.items[3].n == 32) memcpy(nonce, v.items[3].s, 32); free(raw); return fd;
}
static const char *frame_kind(Val *v) { return v->t == V_ARR && v->count > 1 && v->items[1].t == V_STR ? (const char *)v->items[1].s : ""; }

/* Fake untrusted server: a forked child plays one behavior on one connection. */
typedef void (*Behavior)(int fd);
static int fake_server(Behavior b, pid_t *child) {
    int l = socket(AF_INET, SOCK_STREAM, 0); struct sockaddr_in a = {0}; socklen_t al = sizeof a; a.sin_family = AF_INET; a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind(l, (struct sockaddr *)&a, sizeof a); listen(l, 1); getsockname(l, (struct sockaddr *)&a, &al);
    if ((*child = fork()) == 0) { struct timeval tv = {5, 0}; int c = accept(l, NULL, NULL); setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv); failures = 0; b(c); close(c); _exit(failures ? 1 : 0); }
    close(l); return ntohs(a.sin_port);
}
static int join(pid_t child) { int st; waitpid(child, &st, 0); return WIFEXITED(st) && WEXITSTATUS(st) == 0; }
static void fake_accept(int fd, int valid, unsigned char nonce[32]) {
    Buf b = {0}; Val v; unsigned char *raw = NULL, t[16], pr[32]; size_t rn = 0; Buf want = {0};
    rnd(nonce, 32); hex2bin(opt_transfer, t, 16);
    p_arr(&b, 4); p_uint(&b, 1); p_str(&b, "challenge"); p_bin(&b, t, 16); p_bin(&b, nonce, 32); send_frame(fd, &b, 0);
    CHECK(read_frame(fd, &v, &raw, &rn), "request frame"); CHECK(!contains(raw, rn, key, 32), "request has no key");
    request(&want, nonce); Val wv; CHECK(unpack(want.b, want.n, &wv) && val_eq(&v, &wv), "request equals fixture request");
    if (valid) proof("accepted", nonce, opt_scope, opt_recipient, opt_sha, pr); else memset(pr, 0, 32);
    b.n = 0; p_arr(&b, 5); p_uint(&b, 1); p_str(&b, "accepted"); p_bin(&b, t, 16); p_uint(&b, data_len); p_bin(&b, pr, 32); send_frame(fd, &b, 0);
}
static void beh_bad_proof(int fd) { unsigned char n[32]; fake_accept(fd, 0, n); }
static void beh_tamper(int fd) { unsigned char n[32]; fake_accept(fd, 1, n); unsigned char first = data[0] ^ 1; send(fd, &first, 1, 0); send(fd, data + 1, data_len - 1, 0); }
static void beh_truncate(int fd) { unsigned char n[32]; fake_accept(fd, 1, n); send(fd, data, 12345, 0); }
static void beh_lost_confirm(int fd) {
    unsigned char n[32], t[16], pr[32], *raw = NULL; size_t rn = 0; Val v; fake_accept(fd, 1, n); send(fd, data, data_len, 0);
    hex2bin(opt_transfer, t, 16); proof("stored", n, opt_scope, opt_recipient, opt_sha, pr);
    CHECK(read_frame(fd, &v, &raw, &rn), "stored frame");
    CHECK(v.t == V_ARR && v.count == 4 && !strcmp(frame_kind(&v), "stored") && v.items[2].n == 16 && !memcmp(v.items[2].s, t, 16) && v.items[3].n == 32 && !memcmp(v.items[3].s, pr, 32), "stored proof");
}

static void setup(void) {
    char tmpl[] = "/tmp/bonfyre-scoped-peer.XXXXXX"; snprintf(root, sizeof root, "%s", mkdtemp(tmpl));
    static const unsigned char unit[] = "admitted test output\0\xff\n"; size_t u = sizeof unit - 1; data_len = u * 17003; free(data); data = malloc(data_len);
    for (size_t i = 0; i < 17003; i++) memcpy(data + i * u, unit, u);
    snprintf(source_path, sizeof source_path, "%s/source.bin", root); FILE *f = fopen(source_path, "wb"); fwrite(data, 1, data_len, f); fclose(f);
    rnd(key, 32); snprintf(key_path, sizeof key_path, "%s/grant.key", root); f = fopen(key_path, "wb"); fwrite(key, 1, 32, f); fclose(f); chmod(key_path, 0600);
    sha_hex("test-account/test-org", 21, opt_scope); sha_hex("test-recipient", 14, opt_recipient); sha_hex(data, data_len, opt_sha);
    snprintf(opt_size, sizeof opt_size, "%zu", data_len); unsigned char t[16]; rnd(t, 16); for (int i = 0; i < 16; i++) sprintf(opt_transfer + 2 * i, "%02x", t[i]);
    snprintf(opt_key, sizeof opt_key, "%s", key_path); snprintf(opt_expires, sizeof opt_expires, "%lld", (long long)time(NULL) + 30); serial = 0;
}
static void teardown(void) { char cmd[700]; snprintf(cmd, sizeof cmd, "rm -rf '%s'", root); system(cmd); }
#define T(name) static void name(void)

T(capabilities_are_bounded_and_truthful) { char *argv[] = {(char *)binary, "transfer-capabilities", NULL}; Run r; run_argv(argv, &r); Val v;
    CHECK(r.rc == 0 && unpack(r.out, r.on, &v) && v.count == 3 && v.items[0].i == 1 && v.items[1].i == 1, "capabilities packet");
    Val *m = &v.items[2]; CHECK(!strcmp(sget(m, "transport"), "loopback-tcp") && iget(m, "remote_transport") == 0 && iget(m, "distributed_inference") == 0 && iget(m, "automatic_discovery") == 0 && iget(m, "max_bytes") == 64LL * 1024 * 1024, "capabilities truthful"); }
T(two_native_peers_verify_publish_and_confirm) { Seed s = start_seed(NULL, 0); Fetch *f = fetch(s.port, NULL, 0); struct stat st;
    CHECK(f->rc == 0 && !strcmp(sget(f->receipt, "state"), "stored_confirmed"), "fetch stored_confirmed (rc %d)", f->rc);
    FILE *o = fopen(f->out, "rb"); unsigned char *got = malloc(data_len + 1); size_t gn = o ? fread(got, 1, data_len + 1, o) : 0; if (o) fclose(o);
    CHECK(gn == data_len && !memcmp(got, data, data_len), "output bytes exact"); free(got);
    CHECK(stat(f->out, &st) == 0 && (st.st_mode & 0777) == 0600, "output mode 0600");
    CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0");
    char want[400]; snprintf(want, sizeof want, "%s|%s|%s|%zu|stored_confirmed", opt_scope, opt_recipient, opt_sha, data_len);
    CHECK(!strcmp(db_one(f->index, "SELECT scope_hash||'|'||recipient_hash||'|'||sha256||'|'||size_bytes||'|'||state FROM swarm_scoped_transfers"), want), "receiver row");
    char kh[65]; for (int i = 0; i < 32; i++) sprintf(kh + 2 * i, "%02x", key[i]);
    char q[200]; snprintf(q, sizeof q, "SELECT count(*) FROM swarm_scoped_transfers WHERE instr(lower(hex(transfer_id))||scope_hash||recipient_hash||sha256||state,'%s')>0", kh);
    CHECK(!strcmp(db_one(f->index, q), "0"), "key not stored in receiver index");
    char idx[640]; snprintf(idx, sizeof idx, "%s/sender.db", root); CHECK(!strcmp(db_one(idx, "SELECT state FROM swarm_scoped_transfers"), "receiver_confirmed"), "sender state");
    CHECK(!tmp_files(), "no temporary files"); }
T(wrong_scope_recipient_digest_size_expiry_and_key_are_denied) { Seed s = start_seed(NULL, 0); char other[640], sz[32], ex[32]; unsigned char ok[32];
    snprintf(other, sizeof other, "%s/other.key", root); rnd(ok, 32); FILE *f = fopen(other, "wb"); fwrite(ok, 1, 32, f); fclose(f); chmod(other, 0600);
    snprintf(sz, sizeof sz, "%zu", data_len + 1); snprintf(ex, sizeof ex, "%lld", atoll(opt_expires) - 1);
    char a64[65], b64[65], c64[65]; memset(a64, 0, 65); memset(b64, 0, 65); memset(c64, 0, 65); for (int i = 0; i < 32; i++) { memcpy(a64 + 2 * i, "ab", 2); memcpy(b64 + 2 * i, "bc", 2); memcpy(c64 + 2 * i, "cd", 2); }
    Kv variants[][1] = {{{"scope", a64}}, {{"recipient", b64}}, {{"sha256", c64}}, {{"size", sz}}, {{"expires", ex}}, {{"key-file", other}}};
    for (size_t i = 0; i < 6; i++) { Fetch *x = fetch(s.port, variants[i], 1); CHECK(x->rc == 77 && !strcmp(sget(x->receipt, "state"), "not_admitted") && !exists(x->out), "variant %s denied (rc %d %s)", variants[i][0].k, x->rc, sget(x->receipt, "state")); }
    CHECK(fetch(s.port, NULL, 0)->rc == 0, "true grant still succeeds"); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0"); }
T(nonce_replay_is_denied_and_fresh_request_still_succeeds) { Seed s = start_seed(NULL, 0); unsigned char n1[32], n2[32]; Buf old = {0}; Val v; unsigned char *raw; size_t rn;
    int a = peer(s.port, n1); request(&old, n1); close(a); int b = peer(s.port, n2);
    CHECK(memcmp(n1, n2, 32) != 0, "fresh nonce"); send_frame(b, &old, 0); CHECK(read_frame(b, &v, &raw, &rn) && !strcmp(frame_kind(&v), "denied"), "replayed request denied"); close(b);
    CHECK(fetch(s.port, NULL, 0)->rc == 0, "fresh fetch succeeds"); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0"); }
T(fragmented_control_frames_and_independent_proofs) { Seed s = start_seed(NULL, 0); unsigned char n[32], t[16], pa[32], pc[32], ps[32], *raw; size_t rn; Val v; Buf rq = {0}, st = {0};
    int fd = peer(s.port, n); hex2bin(opt_transfer, t, 16); request(&rq, n); send_frame(fd, &rq, 1);
    proof("accepted", n, opt_scope, opt_recipient, opt_sha, pa);
    CHECK(read_frame(fd, &v, &raw, &rn) && v.count == 5 && !strcmp(frame_kind(&v), "accepted") && v.items[2].n == 16 && !memcmp(v.items[2].s, t, 16) && v.items[3].i == data_len && v.items[4].n == 32 && !memcmp(v.items[4].s, pa, 32), "accepted with independent proof");
    CHECK(!contains(raw, rn, key, 32), "accepted has no key");
    unsigned char *got = malloc(data_len); CHECK(recv_all(fd, got, data_len) && !memcmp(got, data, data_len), "bytes after acceptance"); free(got);
    proof("stored", n, opt_scope, opt_recipient, opt_sha, ps); p_arr(&st, 4); p_uint(&st, 1); p_str(&st, "stored"); p_bin(&st, t, 16); p_bin(&st, ps, 32); send_frame(fd, &st, 1);
    proof("complete", n, opt_scope, opt_recipient, opt_sha, pc);
    CHECK(read_frame(fd, &v, &raw, &rn) && v.count == 4 && !strcmp(frame_kind(&v), "complete") && v.items[3].n == 32 && !memcmp(v.items[3].s, pc, 32), "complete proof");
    close(fd); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0"); }
T(unproved_storage_ack_cannot_confirm_a_transfer) { Seed s = start_seed(NULL, 0); unsigned char n[32], t[16], pr[32], *raw; size_t rn; Val v, rec; Buf rq = {0}, st = {0}; int rc;
    int fd = peer(s.port, n); hex2bin(opt_transfer, t, 16); request(&rq, n); send_frame(fd, &rq, 0); read_frame(fd, &v, &raw, &rn);
    unsigned char *got = malloc(data_len); recv_all(fd, got, data_len); free(got);
    proof("request", n, opt_scope, opt_recipient, opt_sha, pr); /* a request proof reflected as a storage proof */
    p_arr(&st, 4); p_uint(&st, 1); p_str(&st, "stored"); p_bin(&st, t, 16); p_bin(&st, pr, 32); send_frame(fd, &st, 0); close(fd);
    finish_seed(&s, &rec, &rc); CHECK(rc == 75 && !strcmp(sget(&rec, "state"), "unconfirmed"), "reflected proof leaves unconfirmed");
    char idx[640]; snprintf(idx, sizeof idx, "%s/sender.db", root); CHECK(!strcmp(db_one(idx, "SELECT state FROM swarm_scoped_transfers"), "unconfirmed"), "sender row unconfirmed"); }
T(oversized_truncated_and_trailing_frames_are_rejected) { Seed s = start_seed(NULL, 0); unsigned char n[32], *raw; size_t rn; Val v;
    int fd = peer(s.port, n); unsigned char big[4] = {0, 0, 4, 1}; send(fd, big, 4, 0); shutdown(fd, SHUT_WR); CHECK(read_frame(fd, &v, &raw, &rn) && !strcmp(frame_kind(&v), "denied"), "oversized denied"); close(fd);
    fd = peer(s.port, n); unsigned char trunc[6] = {0, 0, 0, 100, 0x98, 0x01}; send(fd, trunc, 6, 0); shutdown(fd, SHUT_WR); CHECK(read_frame(fd, &v, &raw, &rn) && !strcmp(frame_kind(&v), "denied"), "truncated denied"); close(fd);
    fd = peer(s.port, n); Buf rq = {0}; request(&rq, n); put8(&rq, 1); send_frame(fd, &rq, 0); CHECK(read_frame(fd, &v, &raw, &rn) && !strcmp(frame_kind(&v), "denied"), "trailing byte denied"); close(fd);
    CHECK(fetch(s.port, NULL, 0)->rc == 0, "listener still serves"); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0"); }
T(untrusted_server_cannot_supply_an_acceptance_proof) { pid_t c; int port = fake_server(beh_bad_proof, &c); Fetch *f = fetch(port, NULL, 0);
    CHECK(join(c), "fake server ran cleanly"); CHECK(f->rc == 77 && !strcmp(sget(f->receipt, "state"), "not_admitted") && !exists(f->out), "bad acceptance proof refused"); }
T(authenticated_tampered_bytes_fail_digest_before_publication) { pid_t c; int port = fake_server(beh_tamper, &c); Fetch *f = fetch(port, NULL, 0);
    CHECK(join(c), "fake server ran cleanly"); CHECK(f->rc == 65 && !strcmp(sget(f->receipt, "state"), "artifact_mismatch") && !exists(f->out) && !tmp_files(), "tampered bytes never published"); }
T(truncated_bytes_fail_without_destination_or_temporary_file) { pid_t c; int port = fake_server(beh_truncate, &c); Fetch *f = fetch(port, NULL, 0);
    CHECK(join(c), "fake server ran cleanly"); CHECK(f->rc == 75 && !exists(f->out) && !tmp_files(), "truncated transfer leaves nothing"); }
T(lost_confirmation_retains_stored_output_without_claiming_complete) { pid_t c; int port = fake_server(beh_lost_confirm, &c); Fetch *f = fetch(port, NULL, 0);
    CHECK(join(c), "fake server saw a correct storage proof"); CHECK(f->rc == 75 && !strcmp(sget(f->receipt, "state"), "stored_unconfirmed"), "stored but unconfirmed");
    FILE *o = fopen(f->out, "rb"); unsigned char *got = malloc(data_len + 1); size_t gn = o ? fread(got, 1, data_len + 1, o) : 0; if (o) fclose(o);
    CHECK(gn == data_len && !memcmp(got, data, data_len), "stored bytes retained"); free(got);
    CHECK(!strcmp(db_one(f->index, "SELECT state FROM swarm_scoped_transfers"), "stored_unconfirmed"), "receiver row stored_unconfirmed"); }
T(source_is_verified_before_listening_and_snapshot_does_not_change) { char *argv[64], bad[640], zero[65]; Run r; memset(zero, 0, 65); for (int i = 0; i < 64; i++) zero[i] = '0';
    snprintf(bad, sizeof bad, "%s/bad.db", root); Kv ch[] = {{"sha256", zero}}; Kv ex[] = {{"artifact", source_path}, {"port", "0"}, {"index", bad}};
    build_argv("scoped-seed", ch, 1, ex, 3, argv); run_argv(argv, &r); Val *rec = native(&r);
    CHECK(r.rc == 65 && !strcmp(sget(rec, "state"), "artifact_mismatch") && !strstr(r.err, "SWARM_READY"), "bad digest refused before listening");
    Seed s = start_seed(NULL, 0); FILE *f = fopen(source_path, "wb"); fputs("changed after owner-admitted snapshot", f); fclose(f);
    Fetch *x = fetch(s.port, NULL, 0); FILE *o = fopen(x->out, "rb"); unsigned char *got = malloc(data_len + 1); size_t gn = o ? fread(got, 1, data_len + 1, o) : 0; if (o) fclose(o);
    CHECK(x->rc == 0 && gn == data_len && !memcmp(got, data, data_len), "snapshot served, not the changed file"); free(got); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0"); }
T(empty_artifact_is_an_exact_valid_output) { FILE *f = fopen(source_path, "wb"); fclose(f); snprintf(opt_size, sizeof opt_size, "0"); sha_hex("", 0, opt_sha);
    Seed s = start_seed(NULL, 0); Fetch *x = fetch(s.port, NULL, 0); struct stat st; CHECK(x->rc == 0 && stat(x->out, &st) == 0 && st.st_size == 0, "empty output exact"); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0"); }
T(existing_destination_and_symlink_are_never_overwritten) { char prot[640], link[640]; snprintf(prot, sizeof prot, "%s/protected", root); snprintf(link, sizeof link, "%s/link", root);
    FILE *f = fopen(prot, "wb"); fputs("keep this", f); fclose(f); symlink(prot, link); const char *outs[] = {prot, link};
    for (int i = 0; i < 2; i++) { Kv ch[] = {{"out", outs[i]}}; Fetch *x = fetch(1, ch, 1); char buf[32] = ""; f = fopen(prot, "rb"); size_t n = fread(buf, 1, 31, f); fclose(f); buf[n] = 0;
        CHECK(x->rc == 73 && !strcmp(buf, "keep this"), "destination %d kept (rc %d)", i, x->rc); }
    struct stat st; CHECK(lstat(link, &st) == 0 && S_ISLNK(st.st_mode), "link still a link"); }
T(unsafe_key_and_symlink_source_are_rejected) { char sl[640], kl[640], kh[640], idx[640], *argv[64]; Run r;
    snprintf(sl, sizeof sl, "%s/source-link", root); symlink(source_path, sl); snprintf(idx, sizeof idx, "%s/unsafe.db", root);
    Kv ex[] = {{"artifact", sl}, {"port", "0"}, {"index", idx}}; build_argv("scoped-seed", NULL, 0, ex, 3, argv); run_argv(argv, &r); native(&r); CHECK(r.rc == 65, "symlink source refused");
    chmod(key_path, 0644); CHECK(fetch(1, NULL, 0)->rc == 77, "group-readable key refused"); chmod(key_path, 0600);
    snprintf(kl, sizeof kl, "%s/key-link", root); symlink(key_path, kl); Kv ch[] = {{"key-file", kl}}; CHECK(fetch(1, ch, 1)->rc == 77, "symlinked key refused");
    snprintf(kh, sizeof kh, "%s/key-hardlink", root); link(key_path, kh); CHECK(fetch(1, NULL, 0)->rc == 77, "hard-linked key refused"); }
T(expired_grant_and_listener_expiry_do_not_transfer) { char ex[32]; snprintf(ex, sizeof ex, "%lld", (long long)time(NULL) - 1); Kv ch[] = {{"expires", ex}};
    CHECK(fetch(1, ch, 1)->rc == 78, "expired grant refused"); snprintf(opt_expires, sizeof opt_expires, "%lld", (long long)time(NULL) + 2);
    Seed s = start_seed(NULL, 0); Val rec; int rc; finish_seed(&s, &rec, &rc); CHECK(rc == 78 && !strcmp(sget(&rec, "state"), "expired_or_revoked"), "listener expires"); }
T(removing_key_revokes_waiting_listener) { Seed s = start_seed(NULL, 0); unlink(key_path); Val rec; int rc; finish_seed(&s, &rec, &rc); CHECK(rc == 78 && !strcmp(sget(&rec, "state"), "expired_or_revoked"), "key removal revokes"); }
T(transfer_id_cannot_be_reused_even_with_changed_scope) { Seed s = start_seed(NULL, 0); Fetch *f = fetch(s.port, NULL, 0); char db[640]; snprintf(db, sizeof db, "%s", f->index);
    CHECK(f->rc == 0, "first transfer"); CHECK(finish_seed(&s, NULL, NULL) == 0, "seed exits 0");
    char ef[65], idx[640], *argv[64]; Run r; memset(ef, 0, 65); for (int i = 0; i < 32; i++) memcpy(ef + 2 * i, "ef", 2); snprintf(idx, sizeof idx, "%s/sender.db", root);
    Kv ch[] = {{"scope", ef}}; Kv ex[] = {{"artifact", source_path}, {"port", "0"}, {"index", idx}}; build_argv("scoped-seed", ch, 1, ex, 3, argv); run_argv(argv, &r); Val *rec = native(&r);
    CHECK(r.rc == 77 && !strcmp(sget(rec, "state"), "grant_already_used") && !strstr(r.err, "SWARM_READY"), "reused transfer id refused");
    char port[16]; snprintf(port, sizeof port, "%d", s.port); Kv reuse[] = {{"index", db}}; CHECK(fetch(s.port, reuse, 1)->rc == 77, "receiver refuses reuse"); }
T(argument_bounds_and_remote_host_are_rejected) { char exp[32], big[16]; snprintf(exp, sizeof exp, "%lld", (long long)time(NULL) + 1000); snprintf(big, sizeof big, "%d", 67108865);
    char t64[65]; memset(t64, 0, 65); for (int i = 0; i < 32; i++) memcpy(t64 + 2 * i, "11", 2);
    Kv variants[][1] = {{{"port", "-1"}}, {{"port", "65536"}}, {{"size", "-1"}}, {{"size", big}}, {{"expires", exp}}, {{"host", "example.invalid"}}, {{"scope", "invalid"}}, {{"transfer-id", t64}}};
    for (size_t i = 0; i < 8; i++) { Fetch *x = fetch(1, variants[i], 1); CHECK(x->rc == 64, "bound %s=%s refused (rc %d)", variants[i][0].k, variants[i][0].v, x->rc); }
    char *argv[64], out[640], idx[640]; Run r; int a; snprintf(out, sizeof out, "%s/duplicate", root); snprintf(idx, sizeof idx, "%s/duplicate.db", root);
    Kv ex[] = {{"port", "1"}, {"out", out}, {"index", idx}}; a = build_argv("scoped-fetch", NULL, 0, ex, 3, argv); argv[a++] = "--scope"; argv[a++] = opt_scope; argv[a] = NULL;
    run_argv(argv, &r); native(&r); CHECK(r.rc == 64, "duplicate option refused"); }
T(legacy_gate_key_is_refused_without_disclosure) { const char *tok = "test-secret-must-not-cross-wire"; char *argv[] = {(char *)binary, "seed", root, "--gate-key", (char *)tok, NULL}; Run r; run_argv(argv, &r);
    CHECK(r.rc == 77 && !contains(r.out, r.on, tok, strlen(tok)) && !contains((unsigned char *)r.err, r.en, tok, strlen(tok)), "legacy gate key refused silently"); }

int main(int argc, char **argv) {
    if (argc != 2) { fprintf(stderr, "usage: scoped_transfer_test BONFYRE_SWARM_BINARY\n"); return 2; }
    binary = argv[1]; signal(SIGPIPE, SIG_IGN); bf_crypto_init();
    struct { const char *name; void (*fn)(void); } tests[] = {
#define E(n) {#n, n}
        E(capabilities_are_bounded_and_truthful), E(two_native_peers_verify_publish_and_confirm), E(wrong_scope_recipient_digest_size_expiry_and_key_are_denied),
        E(nonce_replay_is_denied_and_fresh_request_still_succeeds), E(fragmented_control_frames_and_independent_proofs), E(unproved_storage_ack_cannot_confirm_a_transfer),
        E(oversized_truncated_and_trailing_frames_are_rejected), E(untrusted_server_cannot_supply_an_acceptance_proof), E(authenticated_tampered_bytes_fail_digest_before_publication),
        E(truncated_bytes_fail_without_destination_or_temporary_file), E(lost_confirmation_retains_stored_output_without_claiming_complete), E(source_is_verified_before_listening_and_snapshot_does_not_change),
        E(empty_artifact_is_an_exact_valid_output), E(existing_destination_and_symlink_are_never_overwritten), E(unsafe_key_and_symlink_source_are_rejected),
        E(expired_grant_and_listener_expiry_do_not_transfer), E(removing_key_revokes_waiting_listener), E(transfer_id_cannot_be_reused_even_with_changed_scope),
        E(argument_bounds_and_remote_host_are_rejected), E(legacy_gate_key_is_refused_without_disclosure)
#undef E
    };
    size_t total = sizeof tests / sizeof *tests;
    for (size_t i = 0; i < total; i++) { int before = failures; setup(); tests[i].fn(); teardown(); printf("%s %s\n", failures == before ? "ok  " : "FAIL", tests[i].name); }
    printf("scoped transfer: %s (%zu cases, %d checks)\n", failures ? "FAIL" : "PASS", total, checks);
    return failures ? 1 : 0;
}
