/*
 * BonfyreSwarm — P2P artifact distribution via BitTorrent v2 protocol
 *
 * Architecture:
 *   BonfyreHash SHA-256 → torrent piece hashes (zero conversion)
 *   BonfyreIndex SQLite → local DHT / peer tracker
 *   BonfyreGate         → swarm credential (license token = peer auth)
 *   BonfyreMeter        → metered P2P (track bytes seeded per peer)
 *
 * Wire protocol: BitTorrent v2 (BEP 52) with SHA-256 piece tree.
 * Piece size: 256KB (matches BonfyreCompress block alignment).
 *
 * Subcommands:
 *   seed   <dir>       — seed all artifacts in directory
 *   leech  <info-hash> — download artifact by info hash
 *   peers              — list known peers from index
 *   status             — show swarm statistics
 *
 * Usage:
 *   bonfyre-swarm seed ./output --port 6881 --gate-key <KEY>
 *   bonfyre-swarm leech <hash> --out ./download
 *   bonfyre-swarm peers --index ~/.local/share/bonfyre/index.db
 */

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sqlite3.h>

#ifdef BF_HAS_FOUNTAIN
#include <bf_fountain.h>
#endif

/* ── Constants ───────────────────────────────────────────────── */

#define PIECE_SIZE       (256 * 1024)   /* 256KB — matches BonfyreCompress blocks */
#define MAX_PEERS        128
#define MAX_ARTIFACTS    4096
#define HANDSHAKE_LEN    68
#define BLOCK_SIZE       (16 * 1024)    /* 16KB request blocks */
#define PEER_ID_LEN      20
#define FOUNTAIN_BLOCK   4096           /* Fountain symbol block size */
#define BF_EXT_FOUNTAIN  130            /* Fountain symbol extension ID */
#define INFO_HASH_LEN    32             /* SHA-256 for BT v2 */
#define BT_PROTOCOL      "BitTorrent protocol"
#define BT_PSTRLEN       19

/* BitTorrent message IDs */
#define BT_CHOKE          0
#define BT_UNCHOKE        1
#define BT_INTERESTED     2
#define BT_NOT_INTERESTED 3
#define BT_HAVE           4
#define BT_BITFIELD       5
#define BT_REQUEST        6
#define BT_PIECE          7
#define BT_CANCEL         8
#define BT_HASH_REQUEST   21  /* BEP 52: v2 hash request */
#define BT_HASHES         22  /* BEP 52: v2 hash response */

/* Bonfyre extension message IDs (in extended handshake) */
#define BF_EXT_GATE_AUTH  128  /* Gate license token exchange */
#define BF_EXT_METER_ACK  129  /* Meter acknowledgment */

static volatile int g_running = 1;

/* ── SHA-256 (FIPS 180-4, same implementation as bf_sha256) ── */

static const uint32_t K256[64] = {
    0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
    0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
    0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
    0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
    0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
    0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
    0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
    0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
};

#define RR(x,n) (((x)>>(n))|((x)<<(32-(n))))
#define CH(a,b,c) (((a)&(b))^((~(a))&(c)))
#define MAJ(a,b,c) (((a)&(b))^((a)&(c))^((b)&(c)))
#define S0(x) (RR(x,2)^RR(x,13)^RR(x,22))
#define S1(x) (RR(x,6)^RR(x,11)^RR(x,25))
#define s0(x) (RR(x,7)^RR(x,18)^((x)>>3))
#define s1(x) (RR(x,17)^RR(x,19)^((x)>>10))

typedef struct { uint32_t h[8]; uint64_t len; uint8_t buf[64]; size_t blen; } sha256_ctx;

static void sha256_init(sha256_ctx *c) {
    c->h[0]=0x6a09e667; c->h[1]=0xbb67ae85; c->h[2]=0x3c6ef372; c->h[3]=0xa54ff53a;
    c->h[4]=0x510e527f; c->h[5]=0x9b05688c; c->h[6]=0x1f83d9ab; c->h[7]=0x5be0cd19;
    c->len=0; c->blen=0;
}

static void sha256_block(sha256_ctx *c) {
    uint32_t w[64], a,b,d,e,f,g,h,t1,t2;
    for(int i=0;i<16;i++) w[i]=(uint32_t)c->buf[i*4]<<24|(uint32_t)c->buf[i*4+1]<<16|
                                (uint32_t)c->buf[i*4+2]<<8|(uint32_t)c->buf[i*4+3];
    for(int i=16;i<64;i++) w[i]=s1(w[i-2])+w[i-7]+s0(w[i-15])+w[i-16];
    a=c->h[0];b=c->h[1];d=c->h[2];e=c->h[3];f=c->h[4];g=c->h[5];h=c->h[6];t2=c->h[7];
    for(int i=0;i<64;i++){
        t1=t2+S1(f)+CH(f,g,h)+K256[i]+w[i]; uint32_t t0=S0(a)+MAJ(a,b,d);
        t2=h; h=g; g=f; f=e+t1; e=d; d=b; b=a; a=t1+t0;
    }
    c->h[0]+=a;c->h[1]+=b;c->h[2]+=d;c->h[3]+=e;c->h[4]+=f;c->h[5]+=g;c->h[6]+=h;c->h[7]+=t2;
}

static void sha256_update(sha256_ctx *c, const uint8_t *data, size_t len) {
    c->len += len;
    while (len > 0) {
        size_t space = 64 - c->blen;
        size_t take = len < space ? len : space;
        memcpy(c->buf + c->blen, data, take);
        c->blen += take; data += take; len -= take;
        if (c->blen == 64) { sha256_block(c); c->blen = 0; }
    }
}

static void sha256_final(sha256_ctx *c, uint8_t out[32]) {
    uint64_t bits = c->len * 8;
    uint8_t pad = 0x80;
    sha256_update(c, &pad, 1);
    pad = 0;
    while (c->blen != 56) sha256_update(c, &pad, 1);
    for (int i = 7; i >= 0; i--) { uint8_t b = (uint8_t)(bits >> (i*8)); sha256_update(c, &b, 1); }
    for (int i = 0; i < 8; i++) {
        out[i*4]=(uint8_t)(c->h[i]>>24); out[i*4+1]=(uint8_t)(c->h[i]>>16);
        out[i*4+2]=(uint8_t)(c->h[i]>>8); out[i*4+3]=(uint8_t)(c->h[i]);
    }
}

static void hex_encode(const uint8_t *data, size_t len, char *out) {
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        out[i*2] = hex[data[i] >> 4];
        out[i*2+1] = hex[data[i] & 0x0F];
    }
    out[len*2] = '\0';
}

/* ── Bencode encoder (for torrent metainfo) ──────────────────── */

typedef struct { char *buf; size_t len, cap; } bencode_buf_t;

static void bc_ensure(bencode_buf_t *b, size_t need) {
    if (b->len + need > b->cap) {
        b->cap = (b->len + need) * 2;
        b->buf = realloc(b->buf, b->cap);
    }
}

static void bc_raw(bencode_buf_t *b, const char *s, size_t n) {
    bc_ensure(b, n);
    memcpy(b->buf + b->len, s, n);
    b->len += n;
}

static void bc_int(bencode_buf_t *b, int64_t v) {
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), "i%llde", (long long)v);
    bc_raw(b, tmp, (size_t)n);
}

static void bc_str(bencode_buf_t *b, const char *s, size_t len) {
    char prefix[16];
    int n = snprintf(prefix, sizeof(prefix), "%zu:", len);
    bc_raw(b, prefix, (size_t)n);
    bc_raw(b, s, len);
}

static void bc_dict_open(bencode_buf_t *b) { bc_raw(b, "d", 1); }
static void bc_dict_close(bencode_buf_t *b) { bc_raw(b, "e", 1); }
static void bc_list_open(bencode_buf_t *b) { bc_raw(b, "l", 1); }
static void bc_list_close(bencode_buf_t *b) { bc_raw(b, "e", 1); }

/* ── Artifact descriptor ─────────────────────────────────────── */

typedef struct {
    char     path[PATH_MAX];
    char     sha256_hex[65];
    uint8_t  sha256[32];
    int64_t  size;
    char     family_key[17];
    char     artifact_type[128];
    int      piece_count;
    uint8_t *piece_hashes;   /* piece_count * 32 bytes (SHA-256 each) */
} swarm_artifact_t;

/* ── Peer ─────────────────────────────────────────────────────── */

typedef struct {
    struct sockaddr_storage addr;
    socklen_t               addrlen;
    int                     fd;
    uint8_t                 peer_id[PEER_ID_LEN];
    uint8_t                 info_hash[INFO_HASH_LEN];
    int                     am_choking;
    int                     am_interested;
    int                     peer_choking;
    int                     peer_interested;
    int                     authenticated;  /* Gate token verified */
    uint64_t                bytes_uploaded;
    uint64_t                bytes_downloaded;
    time_t                  connected_at;
} swarm_peer_t;

/* ── Swarm state ──────────────────────────────────────────────── */

typedef struct {
    swarm_artifact_t  artifacts[MAX_ARTIFACTS];
    int               artifact_count;
    swarm_peer_t      peers[MAX_PEERS];
    int               peer_count;
    sqlite3          *index_db;       /* BonfyreIndex SQLite */
    char              gate_key[256];  /* Gate license token */
    uint8_t           self_peer_id[PEER_ID_LEN];
    int               listen_fd;
    uint16_t          port;
    uint64_t          total_uploaded;
    uint64_t          total_downloaded;
} swarm_state_t;

/* ── Piece hashing (reuses BonfyreHash SHA-256 exactly) ──────── */

static int compute_piece_hashes(swarm_artifact_t *art) {
    FILE *fp = fopen(art->path, "rb");
    if (!fp) return -1;

    art->piece_count = (int)((art->size + PIECE_SIZE - 1) / PIECE_SIZE);
    art->piece_hashes = malloc((size_t)art->piece_count * 32);
    if (!art->piece_hashes) { fclose(fp); return -1; }

    uint8_t *block = malloc(PIECE_SIZE);
    if (!block) { free(art->piece_hashes); fclose(fp); return -1; }

    /* Also compute file-level SHA-256 */
    sha256_ctx file_ctx;
    sha256_init(&file_ctx);

    for (int i = 0; i < art->piece_count; i++) {
        size_t n = fread(block, 1, PIECE_SIZE, fp);
        sha256_ctx ctx;
        sha256_init(&ctx);
        sha256_update(&ctx, block, n);
        sha256_final(&ctx, art->piece_hashes + i * 32);

        sha256_update(&file_ctx, block, n);
    }

    sha256_final(&file_ctx, art->sha256);
    hex_encode(art->sha256, 32, art->sha256_hex);

    free(block);
    fclose(fp);
    return 0;
}

/* ── Torrent metainfo (BEP 52 v2 format) ────────────────────── */

static bencode_buf_t build_torrent_v2(const swarm_artifact_t *art) {
    bencode_buf_t b = {0};

    bc_dict_open(&b);

    /* info dict */
    bc_str(&b, "info", 4);
    bc_dict_open(&b);
    {
        bc_str(&b, "file tree", 9);
        bc_dict_open(&b);
        {
            /* Single file: basename as key */
            const char *basename = strrchr(art->path, '/');
            basename = basename ? basename + 1 : art->path;
            bc_str(&b, basename, strlen(basename));
            bc_dict_open(&b);
            {
                bc_str(&b, "", 0); /* empty string key = file attributes */
                bc_dict_open(&b);
                {
                    bc_str(&b, "length", 6);
                    bc_int(&b, art->size);

                    bc_str(&b, "pieces root", 11);
                    /* Root hash of the piece hash tree */
                    if (art->piece_count == 1) {
                        bc_str(&b, (const char *)art->piece_hashes, 32);
                    } else {
                        /* Merkle root of piece hashes */
                        /* For single-level, just hash all piece hashes together */
                        sha256_ctx ctx;
                        sha256_init(&ctx);
                        sha256_update(&ctx, art->piece_hashes,
                                      (size_t)art->piece_count * 32);
                        uint8_t root[32];
                        sha256_final(&ctx, root);
                        bc_str(&b, (const char *)root, 32);
                    }
                }
                bc_dict_close(&b);
            }
            bc_dict_close(&b);
        }
        bc_dict_close(&b);

        bc_str(&b, "meta version", 12);
        bc_int(&b, 2);

        bc_str(&b, "name", 4);
        {
            const char *bn = strrchr(art->path, '/');
            bn = bn ? bn + 1 : art->path;
            bc_str(&b, bn, strlen(bn));
        }

        bc_str(&b, "piece length", 12);
        bc_int(&b, PIECE_SIZE);
    }
    bc_dict_close(&b);

    /* Bonfyre extension: family_key + artifact_type */
    bc_str(&b, "x-bonfyre-family", 16);
    bc_str(&b, art->family_key, strlen(art->family_key));

    bc_str(&b, "x-bonfyre-type", 14);
    bc_str(&b, art->artifact_type, strlen(art->artifact_type));

    bc_dict_close(&b);
    return b;
}

/* ── SQLite peer tracker (replaces distributed DHT) ──────────── */

static int init_peer_db(swarm_state_t *state) {
    const char *db_path = NULL;
    char path[PATH_MAX];

    if (state->index_db) return 0; /* already open */

    /* Default path alongside BonfyreIndex */
    snprintf(path, sizeof(path), "%s/.local/share/bonfyre/swarm.db",
             getenv("HOME") ? getenv("HOME") : "/tmp");
    db_path = path;

    /* Ensure directory exists */
    char dir[PATH_MAX];
    snprintf(dir, sizeof(dir), "%s/.local/share/bonfyre",
             getenv("HOME") ? getenv("HOME") : "/tmp");
    mkdir(dir, 0755);

    int rc = sqlite3_open(db_path, &state->index_db);
    if (rc != SQLITE_OK) return -1;

    sqlite3_exec(state->index_db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(state->index_db, "PRAGMA synchronous=NORMAL;", NULL, NULL, NULL);

    /* Peer table: who has what */
    sqlite3_exec(state->index_db,
        "CREATE TABLE IF NOT EXISTS peers ("
        "  peer_id TEXT NOT NULL,"
        "  addr TEXT NOT NULL,"
        "  port INTEGER NOT NULL,"
        "  info_hash TEXT NOT NULL,"
        "  bytes_uploaded INTEGER DEFAULT 0,"
        "  bytes_downloaded INTEGER DEFAULT 0,"
        "  last_seen TEXT DEFAULT (datetime('now')),"
        "  gate_token TEXT,"
        "  PRIMARY KEY (peer_id, info_hash)"
        ");",
        NULL, NULL, NULL);

    /* Artifact table: what's available in the swarm */
    sqlite3_exec(state->index_db,
        "CREATE TABLE IF NOT EXISTS swarm_artifacts ("
        "  info_hash TEXT PRIMARY KEY,"
        "  family_key TEXT,"
        "  artifact_type TEXT,"
        "  size INTEGER,"
        "  piece_count INTEGER,"
        "  piece_hashes BLOB,"
        "  seeders INTEGER DEFAULT 0,"
        "  leechers INTEGER DEFAULT 0,"
        "  created_at TEXT DEFAULT (datetime('now'))"
        ");",
        NULL, NULL, NULL);

    /* Meter log: per-peer transfer accounting for BonfyreMeter */
    sqlite3_exec(state->index_db,
        "CREATE TABLE IF NOT EXISTS swarm_meter ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  peer_id TEXT NOT NULL,"
        "  info_hash TEXT NOT NULL,"
        "  direction TEXT NOT NULL,"  /* 'upload' or 'download' */
        "  bytes INTEGER NOT NULL,"
        "  recorded_at TEXT DEFAULT (datetime('now'))"
        ");",
        NULL, NULL, NULL);

    return 0;
}

static int register_artifact(swarm_state_t *state, const swarm_artifact_t *art) {
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(state->index_db,
        "INSERT OR REPLACE INTO swarm_artifacts "
        "(info_hash, family_key, artifact_type, size, piece_count, piece_hashes, seeders) "
        "VALUES (?, ?, ?, ?, ?, ?, 1);",
        -1, &stmt, NULL);

    sqlite3_bind_text(stmt, 1, art->sha256_hex, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, art->family_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, art->artifact_type, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 4, art->size);
    sqlite3_bind_int(stmt, 5, art->piece_count);
    sqlite3_bind_blob(stmt, 6, art->piece_hashes,
                       art->piece_count * 32, SQLITE_STATIC);

    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return 0;
}

static void record_meter(swarm_state_t *state, const char *peer_id,
                          const char *info_hash, const char *direction,
                          uint64_t bytes) {
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(state->index_db,
        "INSERT INTO swarm_meter (peer_id, info_hash, direction, bytes) "
        "VALUES (?, ?, ?, ?);",
        -1, &stmt, NULL);

    sqlite3_bind_text(stmt, 1, peer_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, info_hash, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, direction, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 4, (int64_t)bytes);

    sqlite3_step(stmt);
    sqlite3_finalize(stmt);
}

/* ── BitTorrent handshake ────────────────────────────────────── */

static int bt_handshake_send(int fd, const uint8_t info_hash[32],
                              const uint8_t peer_id[20]) {
    uint8_t msg[68];
    msg[0] = BT_PSTRLEN;
    memcpy(msg + 1, BT_PROTOCOL, BT_PSTRLEN);
    memset(msg + 20, 0, 8);  /* reserved bytes */
    msg[20] |= 0x10;         /* BEP 10: extension protocol */
    memcpy(msg + 28, info_hash, 20);  /* BT v1 uses first 20 of SHA-256 */
    memcpy(msg + 48, peer_id, 20);
    return (int)send(fd, msg, 68, 0);
}

static int bt_handshake_recv(int fd, uint8_t out_info_hash[20],
                              uint8_t out_peer_id[20]) {
    uint8_t msg[68];
    ssize_t n = recv(fd, msg, 68, MSG_WAITALL);
    if (n != 68) return -1;
    if (msg[0] != BT_PSTRLEN) return -1;
    if (memcmp(msg + 1, BT_PROTOCOL, BT_PSTRLEN) != 0) return -1;
    memcpy(out_info_hash, msg + 28, 20);
    memcpy(out_peer_id, msg + 48, 20);
    return 0;
}

/* ── Gate auth extension (Bonfyre-specific) ──────────────────── */

static int send_gate_auth(int fd, const char *gate_key) {
    size_t klen = strlen(gate_key);
    if (klen > 255) klen = 255;
    uint8_t msg[4 + 2 + 256];
    uint32_t len = (uint32_t)(2 + klen);
    msg[0] = (uint8_t)(len >> 24);
    msg[1] = (uint8_t)(len >> 16);
    msg[2] = (uint8_t)(len >> 8);
    msg[3] = (uint8_t)(len);
    msg[4] = BF_EXT_GATE_AUTH;
    msg[5] = (uint8_t)klen;
    memcpy(msg + 6, gate_key, klen);
    return (int)send(fd, msg, 4 + 2 + klen, 0);
}

/* ── Listener ────────────────────────────────────────────────── */

static int start_listener(swarm_state_t *state) {
    state->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (state->listen_fd < 0) return -1;

    int one = 1;
    setsockopt(state->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(state->port),
        .sin_addr.s_addr = INADDR_ANY
    };

    if (bind(state->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(state->listen_fd);
        return -1;
    }

    if (listen(state->listen_fd, 32) < 0) {
        close(state->listen_fd);
        return -1;
    }

    /* Non-blocking */
    int flags = fcntl(state->listen_fd, F_GETFL, 0);
    fcntl(state->listen_fd, F_SETFL, flags | O_NONBLOCK);

    return 0;
}

/* ── Accept incoming peer ────────────────────────────────────── */

static int accept_peer(swarm_state_t *state) {
    if (state->peer_count >= MAX_PEERS) return -1;

    struct sockaddr_storage from;
    socklen_t fromlen = sizeof(from);
    int fd = accept(state->listen_fd, (struct sockaddr *)&from, &fromlen);
    if (fd < 0) return -1;

    swarm_peer_t *p = &state->peers[state->peer_count];
    memset(p, 0, sizeof(*p));
    p->fd = fd;
    memcpy(&p->addr, &from, fromlen);
    p->addrlen = fromlen;
    p->am_choking = 1;
    p->peer_choking = 1;
    p->connected_at = time(NULL);

    /* Receive handshake */
    uint8_t their_hash[20], their_id[20];
    if (bt_handshake_recv(fd, their_hash, their_id) < 0) {
        close(fd);
        return -1;
    }
    memcpy(p->peer_id, their_id, 20);

    /* Send our handshake back (using first registered artifact's hash) */
    if (state->artifact_count > 0) {
        bt_handshake_send(fd, state->artifacts[0].sha256, state->self_peer_id);
    }

    /* Gate auth: require license token */
    if (state->gate_key[0]) {
        send_gate_auth(fd, state->gate_key);
    }

    state->peer_count++;
    return 0;
}

/* ── Serve piece data to peer ────────────────────────────────── */

static int serve_piece(swarm_state_t *state, swarm_peer_t *peer,
                        const char *info_hash_hex, uint32_t piece_idx,
                        uint32_t offset, uint32_t length) {
    /* Find artifact by info hash */
    swarm_artifact_t *art = NULL;
    for (int i = 0; i < state->artifact_count; i++) {
        if (strcmp(state->artifacts[i].sha256_hex, info_hash_hex) == 0) {
            art = &state->artifacts[i];
            break;
        }
    }
    if (!art) return -1;
    if ((int)piece_idx >= art->piece_count) return -1;

    /* Read piece data from file */
    FILE *fp = fopen(art->path, "rb");
    if (!fp) return -1;

    int64_t file_offset = (int64_t)piece_idx * PIECE_SIZE + offset;
    fseek(fp, file_offset, SEEK_SET);

    uint8_t *buf = malloc(length + 13);  /* 4 len + 1 id + 4 idx + 4 offset + data */
    if (!buf) { fclose(fp); return -1; }

    size_t n = fread(buf + 13, 1, length, fp);
    fclose(fp);

    /* Build BT_PIECE message */
    uint32_t msglen = (uint32_t)(9 + n);
    buf[0] = (uint8_t)(msglen >> 24);
    buf[1] = (uint8_t)(msglen >> 16);
    buf[2] = (uint8_t)(msglen >> 8);
    buf[3] = (uint8_t)(msglen);
    buf[4] = BT_PIECE;
    buf[5] = (uint8_t)(piece_idx >> 24);
    buf[6] = (uint8_t)(piece_idx >> 16);
    buf[7] = (uint8_t)(piece_idx >> 8);
    buf[8] = (uint8_t)(piece_idx);
    buf[9]  = (uint8_t)(offset >> 24);
    buf[10] = (uint8_t)(offset >> 16);
    buf[11] = (uint8_t)(offset >> 8);
    buf[12] = (uint8_t)(offset);

    ssize_t sent = send(peer->fd, buf, 13 + n, 0);
    free(buf);

    if (sent > 0) {
        peer->bytes_uploaded += (uint64_t)n;
        state->total_uploaded += (uint64_t)n;

        /* Record in meter */
        char peer_hex[41];
        hex_encode(peer->peer_id, 20, peer_hex);
        record_meter(state, peer_hex, info_hash_hex, "upload", (uint64_t)n);
    }

    return sent > 0 ? 0 : -1;
}

/* ── Fountain-coded piece transfer ───────────────────────────── */

#ifdef BF_HAS_FOUNTAIN
/*
 * Serve a piece via fountain coding: encodes the piece into N symbols
 * (N = K * overhead), sends each as a BF_EXT_FOUNTAIN message.
 * Multiple peers calling this concurrently produce unique symbols
 * (each encoder increments its own seed) — receiver reconstructs
 * from any K-of-N symbols across ALL peers.
 */
static int serve_fountain_piece(swarm_state_t *state, swarm_peer_t *peer,
                                 const char *info_hash_hex, uint32_t piece_idx) {
    /* Find artifact */
    swarm_artifact_t *art = NULL;
    for (int i = 0; i < state->artifact_count; i++) {
        if (strcmp(state->artifacts[i].sha256_hex, info_hash_hex) == 0) {
            art = &state->artifacts[i];
            break;
        }
    }
    if (!art || (int)piece_idx >= art->piece_count) return -1;

    /* Read the full piece */
    FILE *fp = fopen(art->path, "rb");
    if (!fp) return -1;

    size_t piece_len = PIECE_SIZE;
    int64_t file_offset = (int64_t)piece_idx * PIECE_SIZE;
    int64_t remain = art->size - file_offset;
    if (remain < (int64_t)piece_len) piece_len = (size_t)remain;

    uint8_t *piece_data = malloc(piece_len);
    if (!piece_data) { fclose(fp); return -1; }
    fseek(fp, file_offset, SEEK_SET);
    size_t nr = fread(piece_data, 1, piece_len, fp);
    fclose(fp);
    if (nr != piece_len) { free(piece_data); return -1; }

    /* Create fountain encoder */
    bf_fountain_enc_t *enc = bf_fountain_enc_new(piece_data, piece_len, FOUNTAIN_BLOCK);
    free(piece_data);
    if (!enc) return -1;

    uint32_t K = bf_fountain_enc_k(enc);
    uint32_t n_symbols = (uint32_t)((double)K * BF_FOUNTAIN_OVERHEAD) + 2;

    /* Generate and send fountain symbols */
    uint8_t *wire_buf = malloc(16 + FOUNTAIN_BLOCK + 9);
    if (!wire_buf) { bf_fountain_enc_free(enc); return -1; }

    uint64_t total_sent = 0;
    for (uint32_t s = 0; s < n_symbols; s++) {
        bf_fountain_symbol_t sym = {0};
        if (bf_fountain_enc_next(enc, &sym) != 0) break;

        size_t packed = bf_fountain_symbol_pack(&sym, K,
                            bf_fountain_enc_block_size(enc),
                            wire_buf + 9, 16 + FOUNTAIN_BLOCK);
        bf_fountain_symbol_free(&sym);
        if (packed == 0) continue;

        /* BT extended message: 4B len + 1B ext_id + 4B piece_idx + payload */
        uint32_t msglen = (uint32_t)(5 + packed);
        wire_buf[0] = (uint8_t)(msglen >> 24);
        wire_buf[1] = (uint8_t)(msglen >> 16);
        wire_buf[2] = (uint8_t)(msglen >> 8);
        wire_buf[3] = (uint8_t)(msglen);
        wire_buf[4] = BF_EXT_FOUNTAIN;
        wire_buf[5] = (uint8_t)(piece_idx >> 24);
        wire_buf[6] = (uint8_t)(piece_idx >> 16);
        wire_buf[7] = (uint8_t)(piece_idx >> 8);
        wire_buf[8] = (uint8_t)(piece_idx);

        ssize_t sent = send(peer->fd, wire_buf, 9 + packed, 0);
        if (sent > 0) total_sent += (uint64_t)sent;
        else break;
    }

    free(wire_buf);
    bf_fountain_enc_free(enc);

    if (total_sent > 0) {
        peer->bytes_uploaded += total_sent;
        state->total_uploaded += total_sent;
        char peer_hex[41];
        hex_encode(peer->peer_id, 20, peer_hex);
        record_meter(state, peer_hex, info_hash_hex, "fountain-upload", total_sent);
    }

    return total_sent > 0 ? 0 : -1;
}
#endif /* BF_HAS_FOUNTAIN */

/* ── Signal handler ──────────────────────────────────────────── */

static void sig_handler(int sig) { (void)sig; g_running = 0; }

/* ── command: seed ───────────────────────────────────────────── */

static int command_seed(const char *dir, uint16_t port, const char *gate_key,
                         const char *index_path) {
    (void)index_path;
    swarm_state_t state = {0};
    state.port = port;
    if (gate_key) strncpy(state.gate_key, gate_key, sizeof(state.gate_key) - 1);

    /* Generate peer ID: -BF2600-<random> */
    memcpy(state.self_peer_id, "-BF2600-", 8);
    for (int i = 8; i < PEER_ID_LEN; i++)
        state.self_peer_id[i] = (uint8_t)('0' + (rand() % 10));

    /* Initialize peer database */
    if (init_peer_db(&state) < 0) {
        fprintf(stderr, "Failed to initialize peer database\n");
        return 1;
    }

    /* Discover and hash artifacts */
    DIR *dp = opendir(dir);
    if (!dp) {
        fprintf(stderr, "Cannot open directory: %s\n", dir);
        return 1;
    }

    fprintf(stderr, "Scanning artifacts in %s...\n", dir);
    struct dirent *ent;
    while ((ent = readdir(dp)) != NULL && state.artifact_count < MAX_ARTIFACTS) {
        if (ent->d_name[0] == '.') continue;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", dir, ent->d_name);
        struct stat st;
        if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;

        swarm_artifact_t *art = &state.artifacts[state.artifact_count];
        memset(art, 0, sizeof(*art));
        strncpy(art->path, path, sizeof(art->path) - 1);
        art->size = st.st_size;

        /* Compute piece hashes (these ARE the torrent piece hashes) */
        if (compute_piece_hashes(art) < 0) continue;

        /* Register in SQLite tracker */
        register_artifact(&state, art);

        fprintf(stderr, "  [%d] %s (%lld bytes, %d pieces, hash=%s)\n",
                state.artifact_count, ent->d_name,
                (long long)art->size, art->piece_count, art->sha256_hex);
        state.artifact_count++;
    }
    closedir(dp);

    if (state.artifact_count == 0) {
        fprintf(stderr, "No artifacts found in %s\n", dir);
        return 1;
    }

    /* Start listener */
    if (start_listener(&state) < 0) {
        fprintf(stderr, "Failed to bind port %u: %s\n", port, strerror(errno));
        return 1;
    }

    signal(SIGINT, sig_handler);
    signal(SIGTERM, sig_handler);

    fprintf(stderr, "Seeding %d artifacts on port %u (Ctrl-C to stop)\n",
            state.artifact_count, port);

    /* Main event loop */
    while (g_running) {
        accept_peer(&state);

        /* TODO: full message loop per peer (request/piece/unchoke) */
        /* For now, the accept + handshake + gate auth is the scaffold */

        usleep(10000); /* 10ms */
    }

    /* Print stats */
    printf("{\"kind\":\"swarm-seed-stats\","
           "\"artifacts\":%d,\"peers\":%d,"
           "\"uploaded\":%llu,\"downloaded\":%llu}\n",
           state.artifact_count, state.peer_count,
           (unsigned long long)state.total_uploaded,
           (unsigned long long)state.total_downloaded);

    /* Cleanup */
    close(state.listen_fd);
    for (int i = 0; i < state.peer_count; i++) close(state.peers[i].fd);
    for (int i = 0; i < state.artifact_count; i++) free(state.artifacts[i].piece_hashes);
    sqlite3_close(state.index_db);
    return 0;
}

/* ── command: peers ──────────────────────────────────────────── */

static int command_peers(const char *db_path) {
    sqlite3 *db;
    char path[PATH_MAX];
    if (!db_path) {
        snprintf(path, sizeof(path), "%s/.local/share/bonfyre/swarm.db",
                 getenv("HOME") ? getenv("HOME") : "/tmp");
        db_path = path;
    }

    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot open %s\n", db_path);
        return 1;
    }

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db,
        "SELECT peer_id, addr, port, info_hash, bytes_uploaded, bytes_downloaded, last_seen "
        "FROM peers ORDER BY last_seen DESC LIMIT 100;",
        -1, &stmt, NULL);

    printf("[");
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (!first) printf(",");
        printf("{\"peer_id\":\"%s\",\"addr\":\"%s\",\"port\":%d,"
               "\"info_hash\":\"%s\",\"up\":%lld,\"down\":%lld,\"seen\":\"%s\"}",
               sqlite3_column_text(stmt, 0),
               sqlite3_column_text(stmt, 1),
               sqlite3_column_int(stmt, 2),
               sqlite3_column_text(stmt, 3),
               sqlite3_column_int64(stmt, 4),
               sqlite3_column_int64(stmt, 5),
               sqlite3_column_text(stmt, 6));
        first = 0;
    }
    printf("]\n");

    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return 0;
}

/* ── command: status ─────────────────────────────────────────── */

static int command_status(const char *db_path) {
    sqlite3 *db;
    char path[PATH_MAX];
    if (!db_path) {
        snprintf(path, sizeof(path), "%s/.local/share/bonfyre/swarm.db",
                 getenv("HOME") ? getenv("HOME") : "/tmp");
        db_path = path;
    }

    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        fprintf(stderr, "Cannot open %s\n", db_path);
        return 1;
    }

    int64_t art_count = 0, peer_count = 0, total_up = 0, total_down = 0;

    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db, "SELECT COUNT(*) FROM swarm_artifacts;", -1, &stmt, NULL);
    if (sqlite3_step(stmt) == SQLITE_ROW) art_count = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db, "SELECT COUNT(DISTINCT peer_id) FROM peers;", -1, &stmt, NULL);
    if (sqlite3_step(stmt) == SQLITE_ROW) peer_count = sqlite3_column_int64(stmt, 0);
    sqlite3_finalize(stmt);

    sqlite3_prepare_v2(db,
        "SELECT COALESCE(SUM(CASE WHEN direction='upload' THEN bytes ELSE 0 END), 0),"
        "       COALESCE(SUM(CASE WHEN direction='download' THEN bytes ELSE 0 END), 0) "
        "FROM swarm_meter;", -1, &stmt, NULL);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        total_up = sqlite3_column_int64(stmt, 0);
        total_down = sqlite3_column_int64(stmt, 1);
    }
    sqlite3_finalize(stmt);

    printf("{\"kind\":\"swarm-status\","
           "\"artifacts\":%lld,\"peers\":%lld,"
           "\"total_uploaded\":%lld,\"total_downloaded\":%lld}\n",
           (long long)art_count, (long long)peer_count,
           (long long)total_up, (long long)total_down);

    sqlite3_close(db);
    return 0;
}

/* ── main ────────────────────────────────────────────────────── */

int main(int argc, char **argv) {
    srand((unsigned)time(NULL));

    if (argc < 2) goto usage;

    if (strcmp(argv[1], "seed") == 0 && argc >= 3) {
        const char *dir = argv[2];
        uint16_t port = 6881;
        const char *gate_key = NULL;
        const char *index_path = NULL;
        for (int i = 3; i < argc - 1; i++) {
            if (strcmp(argv[i], "--port") == 0) port = (uint16_t)atoi(argv[++i]);
            else if (strcmp(argv[i], "--gate-key") == 0) gate_key = argv[++i];
            else if (strcmp(argv[i], "--index") == 0) index_path = argv[++i];
        }
        return command_seed(dir, port, gate_key, index_path);
    }

    if (strcmp(argv[1], "peers") == 0) {
        const char *db = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--index") == 0) db = argv[++i];
        }
        return command_peers(db);
    }

    if (strcmp(argv[1], "status") == 0) {
        const char *db = NULL;
        for (int i = 2; i < argc - 1; i++) {
            if (strcmp(argv[i], "--index") == 0) db = argv[++i];
        }
        return command_status(db);
    }

usage:
    fprintf(stderr,
        "BonfyreSwarm — P2P artifact distribution\n\n"
        "Usage:\n"
        "  bonfyre-swarm seed <dir> [--port N] [--gate-key KEY] [--index DB]\n"
        "  bonfyre-swarm peers [--index DB]\n"
        "  bonfyre-swarm status [--index DB]\n");
    return 1;
}
