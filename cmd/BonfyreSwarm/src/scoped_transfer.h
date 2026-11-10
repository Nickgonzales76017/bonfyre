/*
 * One-shot, exact-content artifact transfer in the existing Swarm owner.
 *
 * The caller must authorize the account, recipient and output before creating
 * this grant. Scope hashes and transport-key possession are not account grants.
 * v1 deliberately binds only IPv4 loopback. It provides no remote discovery,
 * encrypted network carrier, model execution or public listener.
 *
 * Control frames: uint32 big-endian byte length + MessagePack array (<=1024).
 * challenge [1,"challenge", transfer:bin16, nonce:bin32]
 * request   [1,"request", transfer, scope:bin32, recipient:bin32,
 *            sha256:bin32, size:uint, proof:bin32]
 * accepted  [1,"accepted", transfer, size:uint, proof:bin32]
 *            followed by exactly size artifact bytes
 * stored    [1,"stored", transfer, proof:bin32]
 * complete  [1,"complete", transfer, proof:bin32]
 * denied    [1,"denied", transfer, "not_admitted"]
 *
 * Proof = existing bf_crypto_hmac (keyed BLAKE2b-256), over the 24-byte
 * zero-padded ASCII domain "bf.swarm.v1.<kind>" + nonce + transfer + scope +
 * recipient + sha256 + uint64 big-endian size + uint64 big-endian expiry.
 * The server snapshots and verifies one admitted file before listening. A
 * fresh challenge is issued on every connection. A transfer ID is single-use
 * per direction in the existing Swarm index. Receiver confirmation follows
 * digest verification, durable no-overwrite publication and a local receipt
 * in the native receiver. receiver_confirmed records the authenticated peer's
 * acknowledgement; it is not an independent storage or account attestation.
 */
#ifndef BONFYRE_SWARM_SCOPED_TRANSFER_H
#define BONFYRE_SWARM_SCOPED_TRANSFER_H

#include <poll.h>
#include "bf_crypto.h"
#include "bf_msgpack.h"

#define ST_MAX_BYTES (64u * 1024u * 1024u)
#define ST_MAX_FRAME 1024u
#define ST_MAX_LIFETIME 300u
#define ST_MAX_ATTEMPTS 8
#define ST_USAGE 64
#define ST_DATA 65
#define ST_STORAGE 73
#define ST_RETRY 75
#define ST_DENIED 77
#define ST_EXPIRED 78

typedef struct {
    const char *artifact, *out, *index, *key_file;
    uint8_t scope[32], recipient[32], digest[32], transfer[16], key[32];
    char scope_hex[65], recipient_hex[65], digest_hex[65], transfer_hex[33];
    uint64_t size, expires;
    unsigned port;
} st_options;

static int64_t st_clock(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return -1;
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int64_t st_deadline(const st_options *o, int budget_ms) {
    int64_t left = ((int64_t)o->expires - (int64_t)time(NULL)) * 1000;
    if (left < 0) left = 0;
    if (left > budget_ms) left = budget_ms;
    return st_clock() + left;
}

static int st_wait(int fd, short event, int64_t deadline) {
    while (g_running) {
        int64_t left = deadline - st_clock();
        if (left <= 0) { errno = ETIMEDOUT; return -1; }
        struct pollfd p = { .fd = fd, .events = event };
        int rc = poll(&p, 1, left > 1000 ? 1000 : (int)left);
        if (rc < 0 && errno == EINTR) continue;
        if (rc < 0) return -1;
        if (rc && (p.revents & event)) return 0;
        if (rc && (p.revents & (POLLERR | POLLHUP | POLLNVAL))) return -1;
    }
    errno = EINTR;
    return -1;
}

static int st_io(int fd, void *buffer, size_t size, int sending, int64_t deadline) {
    uint8_t *p = buffer;
    while (size) {
        if (st_wait(fd, sending ? POLLOUT : POLLIN, deadline)) return -1;
        ssize_t n = sending ? send(fd, p, size, 0) : recv(fd, p, size, 0);
        if (n < 0 && (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        if (n <= 0) return -1;
        p += n;
        size -= (size_t)n;
    }
    return 0;
}

static void st_u64(uint8_t *out, uint64_t n) {
    for (int i = 7; i >= 0; --i) { out[i] = (uint8_t)n; n >>= 8; }
}

static int st_frame_send(int fd, bf_mp_writer_t *w, int64_t deadline) {
    uint32_t length = (uint32_t)bf_mp_writer_size(w);
    uint8_t h[4];
    if (!length || length > ST_MAX_FRAME) return -1;
    swarm_put_u32(h, length);
    return st_io(fd, h, 4, 1, deadline) ||
           st_io(fd, (void *)bf_mp_writer_data(w), length, 1, deadline) ? -1 : 0;
}

static int st_frame_read(int fd, uint8_t *frame, bf_mp_reader_t *r, int64_t deadline) {
    uint8_t h[4];
    if (st_io(fd, h, 4, 0, deadline)) return -1;
    uint32_t length = swarm_get_u32(h);
    if (!length || length > ST_MAX_FRAME || st_io(fd, frame, length, 0, deadline)) return -1;
    bf_mp_reader_init(r, frame, length);
    return 0;
}

static int st_uint(bf_mp_reader_t *r, uint64_t expected) {
    bf_mp_value_t v;
    return bf_mp_read_next(r, &v) == BF_MP_UINT && v.v.u == expected ? 0 : -1;
}

static int st_str(bf_mp_reader_t *r, const char *expected) {
    bf_mp_value_t v;
    return bf_mp_read_next(r, &v) == BF_MP_STR && v.v.raw.len == strlen(expected) &&
           !memcmp(v.v.raw.ptr, expected, v.v.raw.len) ? 0 : -1;
}

static int st_bin(bf_mp_reader_t *r, void *out, size_t size) {
    bf_mp_value_t v;
    if (bf_mp_read_next(r, &v) != BF_MP_BIN || v.v.raw.len != size) return -1;
    memcpy(out, v.v.raw.ptr, size);
    return 0;
}

static int st_bin_equal(bf_mp_reader_t *r, const void *expected, size_t size) {
    uint8_t actual[32];
    if (size > sizeof(actual) || st_bin(r, actual, size)) return -1;
    return bf_crypto_ct_eq(actual, expected, size) == 0 ? 0 : -1;
}

static int st_array(bf_mp_reader_t *r, uint32_t count, const char *kind) {
    bf_mp_value_t v;
    return bf_mp_read_next(r, &v) == BF_MP_ARRAY && v.v.count == count &&
           !st_uint(r, 1) && !st_str(r, kind) ? 0 : -1;
}

static int st_begin(bf_mp_writer_t *w, uint32_t count, const char *kind, const st_options *o) {
    bf_mp_writer_reset(w);
    int bad = bf_mp_pack_array(w, count);
    bad |= bf_mp_pack_uint(w, 1);
    bad |= bf_mp_pack_str_cstr(w, kind);
    bad |= bf_mp_pack_bin(w, o->transfer, 16);
    return bad;
}

static void st_mac(const st_options *o, const uint8_t nonce[32], const char *kind, uint8_t mac[32]) {
    uint8_t material[184] = {0};
    snprintf((char *)material, 24, "bf.swarm.v1.%s", kind);
    memcpy(material + 24, nonce, 32);
    memcpy(material + 56, o->transfer, 16);
    memcpy(material + 72, o->scope, 32);
    memcpy(material + 104, o->recipient, 32);
    memcpy(material + 136, o->digest, 32);
    st_u64(material + 168, o->size);
    st_u64(material + 176, o->expires);
    bf_crypto_hmac(mac, material, sizeof(material), o->key);
    bf_crypto_wipe(material, sizeof(material));
}

static int st_random(uint8_t *out, size_t size) {
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    size_t done = 0;
    while (done < size) {
        ssize_t n = read(fd, out + done, size - done);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) { close(fd); return -1; }
        done += (size_t)n;
    }
    close(fd);
    return 0;
}

static int st_key_read(const char *path, uint8_t key[32]) {
    struct stat s;
    int fd = open(path, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) return -1;
    int ok = !fstat(fd, &s) && S_ISREG(s.st_mode) && s.st_uid == geteuid() &&
             ((s.st_mode & 07777) == 0600 || (s.st_mode & 07777) == 0400) &&
             s.st_nlink == 1 && s.st_size == 32;
    size_t n = 0;
    while (ok && n < 32) {
        ssize_t got = read(fd, key + n, 32 - n);
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) { ok = 0; break; }
        n += (size_t)got;
    }
    close(fd);
    return ok ? 0 : -1;
}

static int st_grant_current(const st_options *o) {
    uint8_t key[32] = {0};
    int ok = g_running && time(NULL) < (time_t)o->expires &&
             !st_key_read(o->key_file, key) && bf_crypto_ct_eq(key, o->key, 32) == 0;
    bf_crypto_wipe(key, sizeof(key));
    return ok;
}

static int st_number(const char *s, uint64_t maximum, uint64_t *out) {
    if (!s || !*s) return -1;
    uint64_t n = 0;
    for (; *s; ++s) {
        if (*s < '0' || *s > '9' || n > (maximum - (uint64_t)(*s - '0')) / 10) return -1;
        n = n * 10 + (uint64_t)(*s - '0');
        if (n > maximum) return -1;
    }
    *out = n;
    return 0;
}

static int st_hex(const char *s, uint8_t *bytes, size_t n, char *copy) {
    if (strlen(s) != n * 2) return -1;
    for (size_t i = 0; i < n * 2; ++i)
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return -1;
    if (bf_crypto_from_hex(bytes, n, s, n * 2) != 0) return -1;
    memcpy(copy, s, n * 2 + 1);
    return 0;
}

static int st_options_read(int argc, char **argv, int seed, st_options *o) {
    uint32_t seen = 0;
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc) return ST_USAGE;
        const char *name = argv[i], *value = argv[i + 1];
        uint32_t bit = 0;
        uint64_t n = 0;
        if (!strcmp(name, "--scope")) {
            bit = 1; if (st_hex(value, o->scope, 32, o->scope_hex)) return ST_USAGE;
        } else if (!strcmp(name, "--recipient")) {
            bit = 2; if (st_hex(value, o->recipient, 32, o->recipient_hex)) return ST_USAGE;
        } else if (!strcmp(name, "--sha256")) {
            bit = 4; if (st_hex(value, o->digest, 32, o->digest_hex)) return ST_USAGE;
        } else if (!strcmp(name, "--transfer-id")) {
            bit = 8; if (st_hex(value, o->transfer, 16, o->transfer_hex)) return ST_USAGE;
        } else if (!strcmp(name, "--size")) {
            bit = 16; if (st_number(value, ST_MAX_BYTES, &o->size)) return ST_USAGE;
        } else if (!strcmp(name, "--expires")) {
            bit = 32; if (st_number(value, INT64_MAX, &o->expires)) return ST_USAGE;
        } else if (!strcmp(name, "--key-file")) {
            bit = 64; o->key_file = value;
        } else if (!strcmp(name, "--port")) {
            bit = 128; if (st_number(value, 65535, &n) || (!seed && n == 0)) return ST_USAGE;
            o->port = (unsigned)n;
        } else if (!strcmp(name, "--index")) {
            bit = 256; o->index = value;
        } else if (!strcmp(name, seed ? "--artifact" : "--out")) {
            bit = 512; if (seed) o->artifact = value; else o->out = value;
        } else return ST_USAGE;
        if ((seen & bit) || !*value) return ST_USAGE;
        seen |= bit;
    }
    if (seen != 1023) return ST_USAGE;
    uint64_t now = (uint64_t)time(NULL);
    if (o->expires <= now) return ST_EXPIRED;
    if (o->expires - now > ST_MAX_LIFETIME) return ST_USAGE;
    return st_key_read(o->key_file, o->key) ? ST_DENIED : 0;
}

static int st_receipt(const st_options *o, const char *direction, const char *state, int rc) {
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, 1024);
    int bad = bf_mp_pack_array(&w, 3);
    bad |= bf_mp_pack_uint(&w, 1);
    bad |= bf_mp_pack_bool(&w, rc == 0);
    bad |= bf_mp_pack_map(&w, 12);
    bad |= bf_mp_pack_kv_str(&w, "schema", "bonfyre.swarm.transfer.v1");
    bad |= bf_mp_pack_kv_str(&w, "direction", direction);
    bad |= bf_mp_pack_kv_str(&w, "state", state);
    bad |= bf_mp_pack_kv_str(&w, "transport", "loopback-tcp");
    bad |= bf_mp_pack_kv_str(&w, "transfer_id", o->transfer_hex);
    bad |= bf_mp_pack_kv_str(&w, "scope", o->scope_hex);
    bad |= bf_mp_pack_kv_str(&w, "recipient", o->recipient_hex);
    bad |= bf_mp_pack_kv_str(&w, "sha256", o->digest_hex);
    bad |= bf_mp_pack_kv_int(&w, "size", (int64_t)o->size);
    bad |= bf_mp_pack_kv_int(&w, "expires", (int64_t)o->expires);
    bad |= bf_mp_pack_kv_int(&w, "port", o->port);
    bad |= bf_mp_pack_kv_int(&w, "exit_status", rc);
    if (bad || fwrite(w.buf, 1, w.len, stdout) != w.len || fflush(stdout)) {
        if (!rc) rc = ST_STORAGE;
    }
    bf_mp_writer_free(&w);
    return rc;
}

static int st_capabilities(void) {
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, 1024);
    int bad = bf_mp_pack_array(&w, 3);
    bad |= bf_mp_pack_uint(&w, 1);
    bad |= bf_mp_pack_bool(&w, 1);
    bad |= bf_mp_pack_map(&w, 12);
    bad |= bf_mp_pack_kv_str(&w, "schema", "bonfyre.swarm.transfer.v1");
    bad |= bf_mp_pack_kv_str(&w, "transport", "loopback-tcp");
    bad |= bf_mp_pack_kv_str(&w, "authority", "initiating-account-output-owner");
    bad |= bf_mp_pack_kv_str(&w, "proof", "blake2b-256-challenge");
    bad |= bf_mp_pack_kv_bool(&w, "remote_transport", 0);
    bad |= bf_mp_pack_kv_bool(&w, "distributed_inference", 0);
    bad |= bf_mp_pack_kv_bool(&w, "automatic_discovery", 0);
    bad |= bf_mp_pack_kv_bool(&w, "exact_content", 1);
    bad |= bf_mp_pack_kv_bool(&w, "receiver_confirmation", 1);
    bad |= bf_mp_pack_kv_bool(&w, "single_use", 1);
    bad |= bf_mp_pack_kv_int(&w, "max_bytes", ST_MAX_BYTES);
    bad |= bf_mp_pack_kv_int(&w, "max_lifetime_seconds", ST_MAX_LIFETIME);
    int rc = bad || fwrite(w.buf, 1, w.len, stdout) != w.len || fflush(stdout) ? ST_STORAGE : 0;
    bf_mp_writer_free(&w);
    return rc;
}

static int st_index_open(const st_options *o, sqlite3 **db) {
    struct stat s;
    if (!lstat(o->index, &s) && (!S_ISREG(s.st_mode) || s.st_uid != geteuid())) return -1;
    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
#ifdef SQLITE_OPEN_NOFOLLOW
    flags |= SQLITE_OPEN_NOFOLLOW;
#endif
    if (sqlite3_open_v2(o->index, db, flags, NULL) != SQLITE_OK) return -1;
    sqlite3_busy_timeout(*db, 1000);
    return sqlite3_exec(*db,
        "CREATE TABLE IF NOT EXISTS swarm_scoped_transfers ("
        "transfer_id TEXT NOT NULL, direction TEXT NOT NULL, scope_hash TEXT NOT NULL, "
        "recipient_hash TEXT NOT NULL, sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL, "
        "expires INTEGER NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL, "
        "updated_at INTEGER NOT NULL, PRIMARY KEY(transfer_id,direction));", NULL, NULL, NULL) == SQLITE_OK ? 0 : -1;
}

static int st_index_begin(sqlite3 *db, const st_options *o, const char *direction) {
    sqlite3_stmt *q = NULL;
    int rc = sqlite3_prepare_v2(db,
        "INSERT INTO swarm_scoped_transfers VALUES(?,?,?,?,?,?,?,'admitted',strftime('%s','now'),strftime('%s','now'));",
        -1, &q, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(q, 1, o->transfer_hex, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 2, direction, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 3, o->scope_hex, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 4, o->recipient_hex, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 5, o->digest_hex, -1, SQLITE_STATIC);
        sqlite3_bind_int64(q, 6, (sqlite3_int64)o->size);
        sqlite3_bind_int64(q, 7, (sqlite3_int64)o->expires);
        rc = sqlite3_step(q);
    }
    sqlite3_finalize(q);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int st_index_state(sqlite3 *db, const st_options *o, const char *direction, const char *state) {
    sqlite3_stmt *q = NULL;
    int rc = sqlite3_prepare_v2(db,
        "UPDATE swarm_scoped_transfers SET state=?,updated_at=strftime('%s','now') WHERE transfer_id=? AND direction=?;",
        -1, &q, NULL);
    if (rc == SQLITE_OK) {
        sqlite3_bind_text(q, 1, state, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 2, o->transfer_hex, -1, SQLITE_STATIC);
        sqlite3_bind_text(q, 3, direction, -1, SQLITE_STATIC);
        rc = sqlite3_step(q);
    }
    sqlite3_finalize(q);
    return rc == SQLITE_DONE && sqlite3_changes(db) == 1 ? 0 : -1;
}

static int st_snapshot(const st_options *o, uint8_t **bytes) {
    struct stat s;
    int fd = open(o->artifact, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) return -1;
    int ok = !fstat(fd, &s) && S_ISREG(s.st_mode) && s.st_size >= 0 && (uint64_t)s.st_size == o->size;
    uint8_t *data = ok ? malloc((size_t)o->size + 1) : NULL;
    if (!data) { close(fd); return -1; }
    size_t n = 0;
    while (n < o->size) {
        ssize_t got = read(fd, data + n, (size_t)o->size - n);
        if (got < 0 && errno == EINTR) continue;
        if (got <= 0) { ok = 0; break; }
        n += (size_t)got;
    }
    uint8_t extra;
    ssize_t tail;
    do { tail = read(fd, &extra, 1); } while (tail < 0 && errno == EINTR);
    if (tail != 0) ok = 0;
    close(fd);
    uint8_t digest[32];
    BfSha256 sha;
    bf_sha256_init(&sha);
    bf_sha256_update(&sha, data, n);
    bf_sha256_final(&sha, digest);
    if (!ok || n != o->size || bf_crypto_ct_eq(digest, o->digest, 32) != 0) {
        bf_crypto_wipe(data, n); free(data); return -1;
    }
    *bytes = data;
    return 0;
}

static int st_socket(void) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0 && (fcntl(fd, F_SETFL, O_NONBLOCK) || fcntl(fd, F_SETFD, FD_CLOEXEC))) {
        close(fd); return -1;
    }
    return fd;
}

static int st_proof_send(int fd, bf_mp_writer_t *w, const st_options *o,
                         const uint8_t nonce[32], const char *kind, int size, int64_t deadline) {
    uint8_t mac[32];
    st_mac(o, nonce, kind, mac);
    int bad = st_begin(w, size ? 5 : 4, kind, o);
    if (size) bad |= bf_mp_pack_uint(w, o->size);
    bad |= bf_mp_pack_bin(w, mac, 32);
    return bad ? -1 : st_frame_send(fd, w, deadline);
}

static int st_proof_read(int fd, const st_options *o, const uint8_t nonce[32],
                         const char *kind, int size, int64_t deadline) {
    uint8_t frame[ST_MAX_FRAME], proof[32], expected[32];
    bf_mp_reader_t r;
    if (st_frame_read(fd, frame, &r, deadline) || st_array(&r, size ? 5 : 4, kind) ||
        st_bin_equal(&r, o->transfer, 16) || (size && st_uint(&r, o->size)) ||
        st_bin(&r, proof, 32) || bf_mp_reader_remaining(&r)) return -1;
    st_mac(o, nonce, kind, expected);
    return bf_crypto_hmac_verify(proof, expected) == 0 ? 0 : -1;
}

/* -1 rejects only this connection. Other nonzero values end the grant. */
static int st_serve_peer(int fd, const st_options *o, const uint8_t *bytes, sqlite3 *db) {
    uint8_t nonce[32], frame[ST_MAX_FRAME], proof[32], expected[32];
    bf_mp_reader_t r;
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, ST_MAX_FRAME);
    int64_t deadline = st_deadline(o, 3000);
    int rc = -1;
    if (!st_grant_current(o)) { rc = ST_EXPIRED; goto done; }
    if (st_random(nonce, 32) || st_begin(&w, 4, "challenge", o) ||
        bf_mp_pack_bin(&w, nonce, 32) || st_frame_send(fd, &w, deadline)) goto done;
    if (st_frame_read(fd, frame, &r, deadline) || st_array(&r, 8, "request") ||
        st_bin_equal(&r, o->transfer, 16) || st_bin_equal(&r, o->scope, 32) ||
        st_bin_equal(&r, o->recipient, 32) || st_bin_equal(&r, o->digest, 32) ||
        st_uint(&r, o->size) || st_bin(&r, proof, 32) || bf_mp_reader_remaining(&r)) goto deny;
    st_mac(o, nonce, "request", expected);
    if (bf_crypto_hmac_verify(proof, expected) != 0) goto deny;
    if (!st_grant_current(o)) { rc = ST_EXPIRED; goto done; }
    /* Once accepted, this grant cannot be used for a second transfer. */
    rc = ST_RETRY;
    if (st_index_state(db, o, "send", "sending")) { rc = ST_STORAGE; goto done; }
    deadline = st_deadline(o, 30000);
    if (st_proof_send(fd, &w, o, nonce, "accepted", 1, deadline)) goto done;
    for (size_t n = 0; n < o->size;) {
        if (!st_grant_current(o)) { rc = ST_EXPIRED; goto done; }
        size_t count = (size_t)o->size - n;
        if (count > 65536) count = 65536;
        if (st_io(fd, (void *)(bytes + n), count, 1, deadline)) goto done;
        n += count;
    }
    if (st_proof_read(fd, o, nonce, "stored", 0, deadline)) goto done;
    if (!st_grant_current(o)) { rc = ST_EXPIRED; goto done; }
    if (st_index_state(db, o, "send", "receiver_confirmed")) { rc = ST_STORAGE; goto done; }
    /* A lost final frame does not undo the authenticated durable-storage ack. */
    (void)st_proof_send(fd, &w, o, nonce, "complete", 0, deadline);
    rc = 0;
    goto done;
deny:
    if (!st_begin(&w, 4, "denied", o) && !bf_mp_pack_str_cstr(&w, "not_admitted"))
        (void)st_frame_send(fd, &w, deadline);
done:
    bf_mp_writer_free(&w);
    return rc;
}

static int st_seed(st_options *o) {
    int listener = -1, rc = ST_STORAGE, begun = 0;
    uint8_t *bytes = NULL;
    sqlite3 *db = NULL;
    const char *state = "failed";
    if (st_snapshot(o, &bytes)) { rc = ST_DATA; state = "artifact_mismatch"; goto done; }
    if (st_index_open(o, &db)) goto done;
    if (st_index_begin(db, o, "send")) { rc = ST_DENIED; state = "grant_already_used"; goto done; }
    begun = 1;
    listener = st_socket();
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons((uint16_t)o->port),
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
    if (listener < 0 || bind(listener, (struct sockaddr *)&addr, sizeof(addr)) || listen(listener, 8)) goto done;
    socklen_t addr_len = sizeof(addr);
    if (getsockname(listener, (struct sockaddr *)&addr, &addr_len)) goto done;
    o->port = ntohs(addr.sin_port);
    fprintf(stderr, "SWARM_READY 127.0.0.1 %u\n", o->port);
    fflush(stderr);
    rc = ST_EXPIRED;
    state = "expired_or_revoked";
    int attempts = 0;
    while (st_grant_current(o)) {
        if (st_wait(listener, POLLIN, st_deadline(o, 1000))) continue;
        struct sockaddr_in remote;
        socklen_t len = sizeof(remote);
        int fd = accept(listener, (struct sockaddr *)&remote, &len);
        if (fd < 0) continue;
        if (remote.sin_family != AF_INET || ntohl(remote.sin_addr.s_addr) != INADDR_LOOPBACK ||
            fcntl(fd, F_SETFL, O_NONBLOCK) || fcntl(fd, F_SETFD, FD_CLOEXEC)) { close(fd); continue; }
        int result = st_serve_peer(fd, o, bytes, db);
        close(fd);
        if (result >= 0) {
            rc = result;
            state = !rc ? "receiver_confirmed" : rc == ST_EXPIRED ? "expired_or_revoked" : "unconfirmed";
            break;
        }
        if (++attempts >= ST_MAX_ATTEMPTS) { rc = ST_DENIED; state = "denied"; break; }
    }
done:
    if (listener >= 0) close(listener);
    if (begun && st_index_state(db, o, "send", state)) { rc = ST_STORAGE; state = "receipt_unavailable"; }
    if (db) sqlite3_close(db);
    if (bytes) { bf_crypto_wipe(bytes, (size_t)o->size); free(bytes); }
    return st_receipt(o, "send", state, rc);
}

static int st_output_open(const st_options *o, int *dir_fd, char *leaf, char *temporary) {
    char parent[PATH_MAX];
    size_t n = strlen(o->out);
    if (!n || n >= sizeof(parent)) return -1;
    memcpy(parent, o->out, n + 1);
    char *slash = strrchr(parent, '/');
    const char *name = slash ? slash + 1 : parent;
    if (!*name || !strcmp(name, ".") || !strcmp(name, "..") || strlen(name) > NAME_MAX) return -1;
    strcpy(leaf, name);
    if (slash) { if (slash == parent) slash[1] = 0; else *slash = 0; }
    else strcpy(parent, ".");
    *dir_fd = open(parent, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
    if (*dir_fd < 0) return -1;
    struct stat s;
    if (fstatat(*dir_fd, leaf, &s, AT_SYMLINK_NOFOLLOW) == 0 || errno != ENOENT) return -1;
    uint8_t nonce[8];
    char suffix[17];
    if (st_random(nonce, sizeof(nonce))) return -1;
    hex_encode(nonce, sizeof(nonce), suffix);
    snprintf(temporary, NAME_MAX + 1, ".swarm-%s-%s.tmp", o->transfer_hex, suffix);
    return openat(*dir_fd, temporary, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
}

static int st_file_write(int fd, const uint8_t *bytes, size_t size) {
    while (size) {
        ssize_t n = write(fd, bytes, size);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        bytes += n; size -= (size_t)n;
    }
    return 0;
}

static int st_fetch(st_options *o) {
    int fd = -1, output = -1, dir_fd = -1, rc = ST_STORAGE, begun = 0, published = 0;
    char leaf[NAME_MAX + 1] = "", temporary[NAME_MAX + 1] = "";
    sqlite3 *db = NULL;
    const char *state = "failed";
    bf_mp_writer_t w;
    bf_mp_writer_init(&w, ST_MAX_FRAME);
    uint8_t frame[ST_MAX_FRAME], nonce[32], proof[32], buffer[65536];
    bf_mp_reader_t r;
    output = st_output_open(o, &dir_fd, leaf, temporary);
    if (output < 0 || st_index_open(o, &db)) goto done;
    if (st_index_begin(db, o, "receive")) { rc = ST_DENIED; state = "grant_already_used"; goto done; }
    begun = 1;
    rc = ST_RETRY;
    fd = st_socket();
    struct sockaddr_in addr = { .sin_family = AF_INET, .sin_port = htons((uint16_t)o->port),
                               .sin_addr.s_addr = htonl(INADDR_LOOPBACK) };
    int64_t deadline = st_deadline(o, 30000);
    if (fd < 0) goto done;
    int result = connect(fd, (struct sockaddr *)&addr, sizeof(addr));
    if (result && errno != EINPROGRESS) goto done;
    if (result) {
        int error = 0; socklen_t length = sizeof(error);
        if (st_wait(fd, POLLOUT, deadline) || getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) || error) goto done;
    }
    if (st_frame_read(fd, frame, &r, deadline) || st_array(&r, 4, "challenge") ||
        st_bin_equal(&r, o->transfer, 16) || st_bin(&r, nonce, 32) || bf_mp_reader_remaining(&r)) goto done;
    if (!st_grant_current(o)) { rc = ST_EXPIRED; state = "expired_or_revoked"; goto done; }
    st_mac(o, nonce, "request", proof);
    if (st_begin(&w, 8, "request", o) || bf_mp_pack_bin(&w, o->scope, 32) ||
        bf_mp_pack_bin(&w, o->recipient, 32) || bf_mp_pack_bin(&w, o->digest, 32) ||
        bf_mp_pack_uint(&w, o->size) || bf_mp_pack_bin(&w, proof, 32) || st_frame_send(fd, &w, deadline)) goto done;
    if (st_proof_read(fd, o, nonce, "accepted", 1, deadline)) { rc = ST_DENIED; state = "not_admitted"; goto done; }
    BfSha256 sha;
    bf_sha256_init(&sha);
    for (uint64_t remaining = o->size; remaining;) {
        if (!st_grant_current(o)) { rc = ST_EXPIRED; state = "expired_or_revoked"; goto done; }
        size_t count = remaining > sizeof(buffer) ? sizeof(buffer) : (size_t)remaining;
        if (st_io(fd, buffer, count, 0, deadline)) goto done;
        bf_sha256_update(&sha, buffer, count);
        if (st_file_write(output, buffer, count)) { rc = ST_STORAGE; goto done; }
        remaining -= count;
    }
    bf_sha256_final(&sha, proof);
    if (bf_crypto_ct_eq(proof, o->digest, 32) != 0) { rc = ST_DATA; state = "artifact_mismatch"; goto done; }
    if (!st_grant_current(o)) { rc = ST_EXPIRED; state = "expired_or_revoked"; goto done; }
    if (fsync(output)) { rc = ST_STORAGE; goto done; }
    if (close(output)) { output = -1; rc = ST_STORAGE; goto done; }
    output = -1;
    if (linkat(dir_fd, temporary, dir_fd, leaf, 0)) { rc = ST_STORAGE; goto done; }
    published = 1;
    state = "stored_unconfirmed";
    if (unlinkat(dir_fd, temporary, 0) || fsync(dir_fd) || st_index_state(db, o, "receive", state)) {
        rc = ST_STORAGE; goto done;
    }
    temporary[0] = 0;
    rc = ST_RETRY;
    if (st_proof_send(fd, &w, o, nonce, "stored", 0, deadline) ||
        st_proof_read(fd, o, nonce, "complete", 0, deadline)) goto done;
    state = "stored_confirmed";
    rc = 0;
done:
    if (fd >= 0) close(fd);
    if (output >= 0) close(output);
    if (dir_fd >= 0) {
        if (*temporary) unlinkat(dir_fd, temporary, 0);
        close(dir_fd);
    }
    if (begun && st_index_state(db, o, "receive", state)) {
        rc = ST_STORAGE; state = published ? "stored_receipt_unavailable" : "receipt_unavailable";
    }
    if (db) sqlite3_close(db);
    bf_crypto_wipe(buffer, sizeof(buffer));
    bf_mp_writer_free(&w);
    return st_receipt(o, "receive", state, rc);
}

static int st_command(int argc, char **argv, int seed) {
    st_options o = {0};
    int rc = st_options_read(argc, argv, seed, &o);
    if (rc) rc = st_receipt(&o, seed ? "send" : "receive",
                          rc == ST_USAGE ? "invalid_arguments" : rc == ST_EXPIRED ? "expired_or_revoked" : "key_unavailable", rc);
    else {
        umask(077);
        signal(SIGPIPE, SIG_IGN);
        signal(SIGINT, sig_handler);
        signal(SIGTERM, sig_handler);
        rc = seed ? st_seed(&o) : st_fetch(&o);
    }
    bf_crypto_wipe(o.key, sizeof(o.key));
    return rc;
}

#endif
