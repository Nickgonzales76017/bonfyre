/*
 * bonfyre-moq — live-object protocol utilities for Bonfyre
 *
 * The legacy local frame relay below is intentionally TCP compatibility only.
 * It is not MOQT: the IETF draft defines no TCP mapping.  MOQT wire helpers
 * in this command are kept separate so a framed local relay can never be
 * mistaken for a QUIC/WebTransport object session.
 *
 * The compatibility relay provides a practical local mode for video frames:
 *
 *   bonfyre-moq video-relay --ingest 9450 --subscribe 9451
 *
 * Producers connect to --ingest and send framed payloads:
 *   [u32_be frame_len][frame_bytes]
 *
 * Subscribers connect to --subscribe and receive the same frame format.
 *
 * To avoid backpressure stalls, relay keeps a 30-frame ring buffer and always
 * broadcasts the latest frame to all active subscribers.
 */

#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_quic.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

#define VERSION "1.0.0"
#define MAX_CLIENTS 128
#define MAX_FRAME_BYTES (2 * 1024 * 1024) /* 2MB/frame (YUV420 typical for 720p) */
#define FRAME_RING 30
#define MOQT_SUBSCRIBE_TYPE 0x03u /* draft-ietf-moq-transport-21, section 9.6 */
#define MOQT_MAX_NAMESPACE_FIELDS 32
#define MOQT_MAX_TRACK_NAME 4096

static int moqt_put_vi64(uint8_t *out, size_t cap, size_t *offset, uint64_t value);

typedef struct {
    uint32_t len;
    uint64_t ts_ms;
    uint8_t *data;
} BfFrame;

typedef struct {
    BfFrame slots[FRAME_RING];
    int     head;
    int     count;
} BfFrameRing;

static volatile sig_atomic_t g_stop = 0;
static void on_sig(int sig) { (void)sig; g_stop = 1; }

static uint64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

/* draft-21 uses MOQT's 1..9 byte vi64 (not QUIC's 1/2/4/8 byte integer).
 * Control-message lengths remain u16. */
static int moqt_vi64(uint8_t *out, size_t cap, uint64_t value, size_t *used) {
    size_t n;
    if (value < (1ULL << 7)) n = 1;
    else if (value < (1ULL << 14)) n = 2;
    else if (value < (1ULL << 21)) n = 3;
    else if (value < (1ULL << 28)) n = 4;
    else if (value < (1ULL << 35)) n = 5;
    else if (value < (1ULL << 42)) n = 6;
    else if (value < (1ULL << 49)) n = 7;
    else if (value < (1ULL << 56)) n = 8;
    else n = 9;
    if (!out || cap < n) return 0;
    if (n == 9) {
        out[0] = 0xff;
        for (size_t i = 1; i < 9; i++) out[i] = (uint8_t)(value >> (8 * (8 - i)));
    } else {
        for (size_t i = 0; i < n; i++) out[n - 1 - i] = (uint8_t)(value >> (8 * i));
        if (n > 1) out[0] |= (uint8_t)(0xffu << (9 - n));
    }
    *used = n;
    return 1;
}

/* Native QUIC SETUP contains a sorted delta-coded option list. The client
 * supplies PATH (0x01), AUTHORITY (0x05), and its own implementation marker
 * (0x07); none of these are inferred from a track name. */
static int moqt_setup_wire(const char *authority, const char *path,
                           uint8_t *out, size_t cap, size_t *out_len) {
    const char *impl = "bonfyre-moq/1.0.0";
    const char *values[3] = {path, authority, impl};
    const uint64_t deltas[3] = {1, 4, 2};
    size_t off = 0, start;
    if (!authority || !*authority || !path || path[0] != '/' || !out || !out_len) return 0;
    if (!moqt_put_vi64(out, cap, &off, 0x2f00) || cap - off < 2) return 0;
    start = off; out[off++] = 0; out[off++] = 0;
    for (size_t i = 0; i < 3; i++) {
        size_t n = strlen(values[i]);
        if (!n || n > 65535 || !moqt_put_vi64(out, cap, &off, deltas[i]) ||
            !moqt_put_vi64(out, cap, &off, n) || cap - off < n) return 0;
        memcpy(out + off, values[i], n); off += n;
    }
    if (off - start - 2 > 65535) return 0;
    out[start] = (uint8_t)((off - start - 2) >> 8);
    out[start + 1] = (uint8_t)(off - start - 2);
    *out_len = off;
    return 1;
}

static int moqt_server_setup_wire(uint8_t *out, size_t cap, size_t *out_len) {
    const char *impl = "bonfyre-moq/1.0.0";
    size_t off = 0, start, n = strlen(impl);
    if (!out || !out_len || !n || n > 65535 ||
        !moqt_put_vi64(out, cap, &off, 0x2f00) || cap - off < 2) return 0;
    start = off; out[off++] = 0; out[off++] = 0;
    /* First option's Delta Type is absolute from zero.  Server SETUP must
     * never carry the native-QUIC client AUTHORITY/PATH options. */
    if (!moqt_put_vi64(out, cap, &off, 7) || !moqt_put_vi64(out, cap, &off, n) || cap - off < n) return 0;
    memcpy(out + off, impl, n); off += n;
    out[start] = (uint8_t)((off - start - 2) >> 8);
    out[start + 1] = (uint8_t)(off - start - 2);
    *out_len = off;
    return 1;
}

static int moqt_put_vi64(uint8_t *out, size_t cap, size_t *offset, uint64_t value) {
    size_t used = 0;
    if (!moqt_vi64(out + *offset, cap - *offset, value, &used)) return 0;
    *offset += used;
    return 1;
}

static int moqt_get_vi64(const uint8_t *data, size_t len, size_t *offset, uint64_t *value) {
    uint8_t first; size_t n = 1; uint64_t result = 0;
    if (!data || !offset || !value || *offset >= len) return 0;
    first = data[*offset];
    while (n < 9 && (first & (uint8_t)(0x80u >> (n - 1)))) n++;
    if (len - *offset < n) return 0;
    if (n == 9) {
        for (size_t i = 1; i < 9; i++) result = (result << 8) | data[*offset + i];
    } else {
        result = first & (uint8_t)(0x7fu >> (n - 1));
        for (size_t i = 1; i < n; i++) result = (result << 8) | data[*offset + i];
    }
    *offset += n; *value = result; return 1;
}

static int moqt_request_error_wire(uint8_t *out, size_t cap, size_t *out_len,
                                   uint64_t error_code, const char *reason) {
    size_t off = 0, start, n;
    if (!out || !out_len || !reason || !(n = strlen(reason)) || n > 1024 ||
        !moqt_put_vi64(out, cap, &off, 0x5) || cap - off < 2) return 0;
    start = off; out[off++] = 0; out[off++] = 0;
    /* Retry Interval 0 means this request is not retryable. */
    if (!moqt_put_vi64(out, cap, &off, error_code) || !moqt_put_vi64(out, cap, &off, 0) ||
        !moqt_put_vi64(out, cap, &off, n) || cap - off < n || off - start - 2 > 65535) return 0;
    memcpy(out + off, reason, n); off += n;
    out[start] = (uint8_t)((off - start - 2) >> 8);
    out[start + 1] = (uint8_t)(off - start - 2);
    *out_len = off; return 1;
}

/* The control stream must begin with SETUP and contain a complete, ordered
 * option list before a peer can use a request stream.  This is deliberately a
 * narrow wire validator, not an authority parser: AUTHORITY/PATH identify the
 * transport endpoint only and a higher TransportLease still decides whether a
 * request may participate. */
static int moqt_setup_valid(const uint8_t *data, size_t len) {
    size_t off = 0, end; uint64_t type = 0, delta = 0, value_len = 0, option = 0;
    if (!moqt_get_vi64(data, len, &off, &type) || type != 0x2f00 || len - off < 2) return 0;
    end = off + 2 + ((size_t)data[off] << 8) + data[off + 1]; off += 2;
    if (end != len) return 0;
    while (off < end) {
        if (!moqt_get_vi64(data, end, &off, &delta) || delta == 0 ||
            UINT64_MAX - option < delta) return 0;
        option += delta;
        if (!moqt_get_vi64(data, end, &off, &value_len) || value_len == 0 ||
            value_len > end - off) return 0;
        off += (size_t)value_len;
    }
    return off == end;
}

/* Encodes the draft-21 SUBSCRIBE request body. `namespace_csv` represents
 * binary namespace fields only for this CLI fixture; real callers must use
 * the typed TransportLease/Track contract, never inferred path text. */
/* When a lease is provided it is carried using draft-21's AUTHORIZATION
 * TOKEN parameter: USE_VALUE, out-of-band Token Type 0, lease id bytes.
 * Type 0 means the token format is private to this endpoint pair; it is not
 * an IANA claim nor a substitute for Bonfyre's delivery owner. */
static int moqt_subscribe_wire(const char *namespace_csv, const char *track,
                               const char *lease, uint64_t request_id,
                               uint8_t *out, size_t cap, size_t *out_len) {
    char fields[MOQT_MAX_TRACK_NAME + 1], *field, *save = NULL;
    size_t off = 0, body_start, fields_count = 0, track_len;
    if (!namespace_csv || !*namespace_csv || !track || !*track || !out || !out_len) return 0;
    if (strlen(namespace_csv) > MOQT_MAX_TRACK_NAME || (track_len = strlen(track)) > MOQT_MAX_TRACK_NAME) return 0;
    snprintf(fields, sizeof(fields), "%s", namespace_csv);
    if (!moqt_put_vi64(out, cap, &off, MOQT_SUBSCRIBE_TYPE)) return 0;
    if (cap - off < 2) return 0;
    body_start = off; out[off++] = 0; out[off++] = 0;
    if (!moqt_put_vi64(out, cap, &off, request_id)) return 0;
    /* Reserve and backfill the namespace field count once parsed.  At most
     * 32 fields means its vi64 stays one byte. */
    if (off >= cap) return 0;
    size_t count_at = off++;
    for (field = strtok_r(fields, ",", &save); field; field = strtok_r(NULL, ",", &save)) {
        size_t n = strlen(field);
        if (!n || ++fields_count > MOQT_MAX_NAMESPACE_FIELDS || !moqt_put_vi64(out, cap, &off, n) || cap - off < n) return 0;
        memcpy(out + off, field, n); off += n;
    }
    out[count_at] = (uint8_t)fields_count;
    if (!fields_count || !moqt_put_vi64(out, cap, &off, track_len) || cap - off < track_len) return 0;
    memcpy(out + off, track, track_len); off += track_len;
    if (lease && *lease) {
        size_t lease_len = strlen(lease);
        if (lease_len > 512 || !moqt_put_vi64(out, cap, &off, 1) ||
            !moqt_put_vi64(out, cap, &off, 3) ||
            !moqt_put_vi64(out, cap, &off, lease_len + 2) ||
            !moqt_put_vi64(out, cap, &off, 3) || !moqt_put_vi64(out, cap, &off, 0) ||
            cap - off < lease_len) return 0;
        memcpy(out + off, lease, lease_len); off += lease_len;
    } else if (!moqt_put_vi64(out, cap, &off, 0)) return 0;
    { size_t body_len = off - body_start - 2; out[body_start] = (uint8_t)(body_len >> 8); out[body_start + 1] = (uint8_t)body_len; }
    *out_len = off;
    return 1;
}

static void hex_write(const uint8_t *data, size_t len) {
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) { putchar(hex[data[i] >> 4]); putchar(hex[data[i] & 15]); }
}

static int cmd_moqt_subscribe_wire(const char *namespace_csv, const char *track, const char *lease, const char *request_text) {
    uint8_t wire[MOQT_MAX_TRACK_NAME * 2 + 128]; size_t len = 0; char *end = NULL;
    unsigned long long request_id = request_text ? strtoull(request_text, &end, 10) : 1;
    if (!namespace_csv || !track || (request_text && (!end || *end)) || !moqt_subscribe_wire(namespace_csv, track, lease, request_id, wire, sizeof(wire), &len)) {
        fputs("moqt-subscribe-wire: invalid namespace, track, or request id\n", stderr);
        return 1;
    }
    printf("{\"protocol\":\"draft-ietf-moq-transport-21\",\"message\":\"SUBSCRIBE\",\"request_id\":%llu,\"namespace_fields\":", request_id);
    { int fields = 1; for (const char *p = namespace_csv; *p; p++) if (*p == ',') fields++; printf("%d", fields); }
    fputs(",\"wire_hex\":\"", stdout); hex_write(wire, len); printf("\",\"authorization_token\":%s,\"wire_bytes\":%zu,\"transport_state\":\"wire_contract_only_no_quic_session\",\"truth\":\"not_authorized_not_subscribed\"}\n", lease&&*lease?"true":"false",len);
    return 0;
}

static int cmd_moqt_setup_wire(const char *authority, const char *path) {
    uint8_t wire[8192]; size_t len = 0;
    if (!moqt_setup_wire(authority, path, wire, sizeof(wire), &len)) {
        fputs("moqt-setup-wire: authority and absolute path required\n", stderr);
        return 1;
    }
    fputs("{\"protocol\":\"draft-ietf-moq-transport-21\",\"message\":\"SETUP\",\"alpn\":\"moqt-21\",\"wire_hex\":\"", stdout);
    hex_write(wire, len);
    printf("\",\"wire_bytes\":%zu,\"transport_state\":\"wire_contract_only_no_quic_session\",\"truth\":\"not_authorized_not_connected\"}\n", len);
    return 0;
}

/* This narrow parser accepts exactly the locally-produced SUBSCRIBE shape.
 * It extracts the out-of-band type-0 lease token and binds it to the requested
 * track before the delivery owner is consulted. */
static int moqt_subscribe_lease(const uint8_t *data, size_t len, char *track,
                                size_t track_cap, char *lease, size_t lease_cap) {
    size_t off=0,end=0,n=0; uint64_t type=0,body=0,fields=0,value=0,params=0,delta=0,token_len=0,alias=0,token_type=0;
    if(!data||!track||!lease||!moqt_get_vi64(data,len,&off,&type)||type!=MOQT_SUBSCRIBE_TYPE||len-off<2)return 0;
    body=((uint64_t)data[off]<<8)|data[off+1];off+=2;if(body!=len-off||!moqt_get_vi64(data,len,&off,&value)||!moqt_get_vi64(data,len,&off,&fields)||fields>MOQT_MAX_NAMESPACE_FIELDS)return 0;
    for(uint64_t i=0;i<fields;i++){if(!moqt_get_vi64(data,len,&off,&value)||value==0||value>len-off){return 0;}off+=(size_t)value;}
    if(!moqt_get_vi64(data,len,&off,&value)||value==0||value>=track_cap||value>len-off)return 0;memcpy(track,data+off,(size_t)value);track[value]=0;off+=(size_t)value;
    if(!moqt_get_vi64(data,len,&off,&params)||params!=1||!moqt_get_vi64(data,len,&off,&delta)||delta!=3||!moqt_get_vi64(data,len,&off,&token_len)||token_len<3||token_len>len-off)return 0;
    end=off+(size_t)token_len;if(!moqt_get_vi64(data,end,&off,&alias)||alias!=3||!moqt_get_vi64(data,end,&off,&token_type)||token_type!=0)return 0;
    n=end-off;if(!n||n>=lease_cap)return 0;memcpy(lease,data+off,n);lease[n]=0;return end==len;
}
static int moqt_subscribe_ok_wire(uint8_t*out,size_t cap,size_t*out_len){size_t off=0,start;if(!out||!out_len||!moqt_put_vi64(out,cap,&off,0x4)||cap-off<2)return 0;start=off;out[off++]=0;out[off++]=0;if(!moqt_put_vi64(out,cap,&off,1)||!moqt_put_vi64(out,cap,&off,0)||off-start-2>65535)return 0;out[start]=(uint8_t)((off-start-2)>>8);out[start+1]=(uint8_t)(off-start-2);*out_len=off;return 1;}
static int child_json(const char *bin,const char *db,const char *lease,char*out,size_t cap){int fd[2],status=0;pid_t p;size_t got=0;ssize_t n;char*const av[]={(char*)bin,"lease-status","--db",(char*)db,"--lease",(char*)lease,0};if(!bin||!*bin||!db||!*db||!lease||!out||cap<2||pipe(fd))return 0;p=fork();if(p<0){close(fd[0]);close(fd[1]);return 0;}if(!p){close(fd[0]);if(dup2(fd[1],1)<0)_exit(127);close(fd[1]);execvp(bin,av);_exit(127);}close(fd[1]);while(got+1<cap&&(n=read(fd[0],out+got,cap-1-got))>0)got+=(size_t)n;close(fd[0]);out[got]=0;return waitpid(p,&status,0)>=0&&WIFEXITED(status)&&WEXITSTATUS(status)==0&&got>0;}
static int json_field_eq(const char *json,const char *key,const char *want){char needle[80];const char*p;if(!json||!key||!want||snprintf(needle,sizeof(needle),"\"%s\"",key)>=(int)sizeof(needle))return 0;p=strstr(json,needle);if(!p||(p=strchr(p+strlen(needle),':'))==0)return 0;while(*++p==' '||*p=='\n'||*p=='\r'||*p=='\t'){}return *p=='\"'&&!strncmp(p+1,want,strlen(want))&&p[1+strlen(want)]=='\"';}
static int delivery_lease_active(const char *bin,const char *db,const char *lease,const char *track){char json[2048];return child_json(bin,db,lease,json,sizeof(json))&&json_field_eq(json,"state","active")&&json_field_eq(json,"protocol","moqt")&&json_field_eq(json,"representation_id",track);}

typedef struct {
    bf_quic_conn_t *conn;
    const char *delivery_db;
    const char *delivery_bin;
    int server_setup_sent;
    int client_setup_received;
    int server_setup_received;
    int is_server;
    int request_error_received;
    int subscribe_ok_received;
} MoqtNativePeer;

static void moqt_raw_stream(int64_t stream_id, uint64_t offset, const uint8_t *data,
                            size_t len, int fin, void *user) {
    MoqtNativePeer *peer = (MoqtNativePeer *)user;
    /* QUIC stream ID low bits distinguish endpoint and direction: client uni
     * is 2, server uni is 3, and client bidi requests are 0.  MOQT requires
     * the corresponding control SETUP before a request stream. */
    unsigned stream_kind = (unsigned)(stream_id & 3);
    size_t parse_offset = 0; uint64_t message_type = 0, message_len = 0;
    if (peer && peer->is_server && stream_kind == 2 && offset == 0 &&
        moqt_setup_valid(data, len)) {
        peer->client_setup_received = 1;
    }
    /* The server cannot write application stream data at Initial. A valid
     * client control SETUP proves 1-RTT processing and opens its paired
     * server control stream; receiving a request alone is not sufficient. */
    if (peer && peer->is_server && peer->conn && peer->client_setup_received &&
        !peer->server_setup_sent) {
        uint8_t setup[128]; size_t setup_len = 0;
        bf_quic_stream_t *control;
        if (moqt_server_setup_wire(setup, sizeof(setup), &setup_len) &&
            (control = bf_quic_raw_stream_open(peer->conn, 0, BF_QUIC_OPERATION_SUBSCRIPTION_RESUME)) &&
            bf_quic_raw_stream_write(control, setup, setup_len, 0) == BF_QUIC_OK)
            peer->server_setup_sent = 1;
        else
            fprintf(stderr, "moqt native server: failed to submit server SETUP after peer stream\n");
    }
    /* A transport handshake is intentionally not transport authorization.
     * Until a TransportLease/Authority projection is installed, reject an
     * actual SUBSCRIBE on its same bidirectional request stream. */
    if (peer && peer->is_server && stream_kind == 0 && offset == 0 && moqt_get_vi64(data, len, &parse_offset, &message_type) &&
        len - parse_offset >= 2) {
        message_len = ((uint64_t)data[parse_offset] << 8) | data[parse_offset + 1];
        if (message_type == MOQT_SUBSCRIBE_TYPE && len - parse_offset - 2 >= message_len) {
            /* draft-21 allows a request stream to arrive before both control
             * streams. It is buffered or reset; there is no SETUP_REQUIRED
             * REQUEST_ERROR, and code 0x2 is TIMEOUT. Until the raw QUIC
             * substrate exposes request-stream reset, do not invent a wire
             * response for this ordering case. */
            if (!peer->client_setup_received) {
                fprintf(stderr, "moqt native server deferred pre-SETUP subscribe stream=%lld; request-stream reset unavailable\n", (long long)stream_id);
                return;
            }
            uint8_t reject[1200], ok_wire[128]; size_t reject_len = 0, ok_len = 0;
            bf_quic_stream_t *request = bf_quic_raw_stream_attach(peer->conn, stream_id);
            char track[MOQT_MAX_TRACK_NAME + 1], lease[513];
            if (request && moqt_subscribe_lease(data, len, track, sizeof(track), lease, sizeof(lease)) &&
                delivery_lease_active(peer->delivery_bin ? peer->delivery_bin : "bonfyre-media-delivery", peer->delivery_db, lease, track) &&
                moqt_subscribe_ok_wire(ok_wire, sizeof(ok_wire), &ok_len) &&
                bf_quic_raw_stream_write(request, ok_wire, ok_len, 0) == BF_QUIC_OK) {
                fprintf(stderr, "moqt native server accepted leased subscribe stream=%lld track=%s\n", (long long)stream_id, track);
            } else {
                const char *reason = "UNAUTHORIZED";
                if (request && moqt_request_error_wire(reject, sizeof(reject), &reject_len, 0x1, reason) &&
                    bf_quic_raw_stream_write(request, reject, reject_len, 0) == BF_QUIC_OK)
                    fprintf(stderr, "moqt native server rejected subscribe stream=%lld reason=%s\n", (long long)stream_id, reason);
            }
        }
    }
    if (peer && !peer->is_server && stream_kind == 3 && offset == 0 &&
        moqt_setup_valid(data, len)) {
        peer->server_setup_received = 1;
        fprintf(stderr, "moqt native client received paired server SETUP\n");
    }
    if (peer && !peer->is_server && offset == 0 && moqt_get_vi64(data, len, &parse_offset, &message_type) &&
        len - parse_offset >= 2 && message_type == 0x5) {
        peer->request_error_received = 1;
        fprintf(stderr, "moqt native client received REQUEST_ERROR\n");
    }
    parse_offset = 0;
    if (peer && !peer->is_server && offset == 0 && moqt_get_vi64(data, len, &parse_offset, &message_type) &&
        len - parse_offset >= 2 && message_type == 0x4) {
        peer->subscribe_ok_received = 1;
        fprintf(stderr, "moqt native client received SUBSCRIBE_OK\n");
    }
    fprintf(stderr, "moqt native session received stream=%lld offset=%llu bytes=%zu fin=%d\n",
            (long long)stream_id, (unsigned long long)offset, len, fin);
}
static void moqt_raw_datagram(const uint8_t *data, size_t len, int early, void *user) {
    (void)data; (void)user;
    fprintf(stderr, "moqt native session received datagram bytes=%zu early=%d\n", len, early);
}

typedef struct { int accepted; const char *delivery_db; const char *delivery_bin; MoqtNativePeer peers[64]; } MoqtNativeServer;
static void moqt_native_accept(bf_quic_conn_t *conn, void *user) {
    MoqtNativeServer *server = (MoqtNativeServer *)user;
    MoqtNativePeer *peer;
    if (!conn || !server || server->accepted >= (int)(sizeof(server->peers) / sizeof(server->peers[0]))) return;
    peer = &server->peers[server->accepted];
    peer->conn = conn;
    peer->delivery_db = server->delivery_db;
    peer->delivery_bin = server->delivery_bin;
    peer->is_server = 1;
    server->accepted++;
    bf_quic_conn_set_raw_callbacks(conn, moqt_raw_stream, moqt_raw_datagram, peer);
}

/* A native QUIC MOQT session listener. It establishes the server half of the
 * required paired control streams and leaves all track authorization to a
 * higher TransportLease/Authority projection. It is intentionally not the
 * legacy TCP compatibility relay. */
static int cmd_moqt_native_server(const char *bind, const char *port_text,
                                  const char *cert, const char *key,
                                  const char *delivery_db, const char *delivery_bin) {
    char *end = NULL; unsigned long port = strtoul(port_text ? port_text : "", &end, 10);
    bf_quic_ctx_t *ctx; bf_quic_server_t *server; MoqtNativeServer state = {0};
    if (!bind || !cert || !key || !end || *end || !port || port > 65535) {
        fputs("moqt-native-server: --bind --port --cert --key required\n", stderr); return 1;
    }
    state.delivery_db = delivery_db; state.delivery_bin = delivery_bin;
    ctx = bf_quic_ctx_new_application(cert, key, NULL, "moqt-21", 1, 1);
    if (!ctx) { fputs("moqt-native-server: raw MOQT QUIC profile unavailable\n", stderr); return 1; }
    server = bf_quic_server_start(ctx, bind, (uint16_t)port, moqt_native_accept, &state);
    if (!server) { bf_quic_ctx_free(ctx); fputs("moqt-native-server: bind failed\n", stderr); return 1; }
    signal(SIGINT, on_sig); signal(SIGTERM, on_sig);
    printf("{\"protocol\":\"draft-ietf-moq-transport-21\",\"transport\":\"native_quic\",\"alpn\":\"moqt-21\",\"datagrams\":\"required_and_negotiated_by_profile\",\"delivery_lease_verifier\":%s,\"state\":\"listening_paired_control_stream_server\"}\n", delivery_db?"true":"false");
    fflush(stdout);
    while (!g_stop) (void)bf_quic_server_poll(server, 100);
    bf_quic_server_stop(server); bf_quic_ctx_free(ctx);
    return 0;
}

/* This is a direct native-QUIC initiation path, not a TCP compatibility
 * relay. It submits client SETUP, waits for the paired server control SETUP,
 * then sends SUBSCRIBE as the first message of a bidirectional request
 * stream. A SUBSCRIBE_OK from a peer remains the separate
 * authorization/establishment proof. */
static int cmd_moqt_native_subscribe(const char *host, const char *port_text,
                                     const char *authority, const char *path,
                                     const char *namespace_csv, const char *track,
                                     const char *lease) {
    char *end = NULL; unsigned long port = strtoul(port_text ? port_text : "", &end, 10);
    uint8_t setup[8192], subscribe[8192]; size_t setup_len = 0, subscribe_len = 0;
    bf_quic_ctx_t *ctx; bf_quic_conn_t *conn; bf_quic_stream_t *control, *request;
    MoqtNativePeer peer = {0};
    if (!host || !authority || !path || !namespace_csv || !track || !end || *end || port == 0 || port > 65535 ||
        !moqt_setup_wire(authority, path, setup, sizeof(setup), &setup_len) ||
        !moqt_subscribe_wire(namespace_csv, track, lease, 0, subscribe, sizeof(subscribe), &subscribe_len)) {
        fputs("moqt-native-subscribe: invalid native QUIC session arguments\n", stderr);
        return 1;
    }
    ctx = bf_quic_ctx_new_application(NULL, NULL, NULL, "moqt-21", 1, 1);
    if (!ctx) { fputs("moqt-native-subscribe: raw QUIC profile unavailable\n", stderr); return 1; }
    conn = bf_quic_connect(ctx, host, (uint16_t)port);
    if (!conn) { bf_quic_ctx_free(ctx); fputs("moqt-native-subscribe: QUIC connect failed\n", stderr); return 1; }
    peer.conn = conn;
    bf_quic_conn_set_raw_callbacks(conn, moqt_raw_stream, moqt_raw_datagram, &peer);
    control = bf_quic_raw_stream_open(conn, 0, BF_QUIC_OPERATION_SUBSCRIPTION_RESUME);
    if (!control || bf_quic_raw_stream_write(control, setup, setup_len, 0) != BF_QUIC_OK) {
        bf_quic_conn_close(conn); bf_quic_ctx_free(ctx);
        fputs("moqt-native-subscribe: client control SETUP submission failed\n", stderr);
        return 1;
    }
    for (int i = 0; i < 40 && !peer.server_setup_received; i++) bf_quic_recv_poll(conn, 25);
    if (!peer.server_setup_received) {
        bf_quic_conn_close(conn); bf_quic_ctx_free(ctx);
        fputs("moqt-native-subscribe: paired server SETUP was not received\n", stderr);
        return 1;
    }
    request = bf_quic_raw_stream_open(conn, 1, BF_QUIC_OPERATION_SUBSCRIPTION_RESUME);
    if (!request || bf_quic_raw_stream_write(request, subscribe, subscribe_len, 0) != BF_QUIC_OK) {
        bf_quic_conn_close(conn); bf_quic_ctx_free(ctx);
        fputs("moqt-native-subscribe: request stream submission failed\n", stderr);
        return 1;
    }
    for (int i = 0; i < 40 && !peer.request_error_received && !peer.subscribe_ok_received; i++) bf_quic_recv_poll(conn, 25);
    if (peer.request_error_received)
        fprintf(stderr, "moqt native client: subscription refused pending TransportLease authority\n");
    bf_quic_conn_close(conn); bf_quic_ctx_free(ctx);
    printf("{\"protocol\":\"draft-ietf-moq-transport-21\",\"transport\":\"native_quic\",\"alpn\":\"moqt-21\",\"datagrams\":\"required_by_profile\",\"control_stream\":\"paired_setup_completed\",\"request_stream\":\"subscribe_submitted\",\"state\":\"%s\",\"truth\":\"%s\"}\n", peer.subscribe_ok_received?"SUBSCRIBED_CONTROL_ONLY_NO_OBJECTS":"request_submitted_not_authorized_not_subscribed", peer.subscribe_ok_received?"lease_verified_subscription_not_media_delivery_or_watch_proof":"not_authorized_not_subscribed");
    return peer.subscribe_ok_received ? 0 : 1;
}

static void ring_init(BfFrameRing *r) {
    memset(r, 0, sizeof(*r));
}

static void ring_push(BfFrameRing *r, const uint8_t *data, uint32_t len) {
    if (!r || !data || len == 0) return;
    if (len > MAX_FRAME_BYTES) return;

    int idx;
    if (r->count < FRAME_RING) {
        idx = (r->head + r->count) % FRAME_RING;
        r->count++;
    } else {
        idx = r->head;
        r->head = (r->head + 1) % FRAME_RING;
        free(r->slots[idx].data);
        r->slots[idx].data = NULL;
    }

    r->slots[idx].data = (uint8_t *)malloc(len);
    if (!r->slots[idx].data) return;
    memcpy(r->slots[idx].data, data, len);
    r->slots[idx].len = len;
    r->slots[idx].ts_ms = now_ms();
}

static const BfFrame *ring_latest(const BfFrameRing *r) {
    if (!r || r->count == 0) return NULL;
    int idx = (r->head + r->count - 1) % FRAME_RING;
    return &r->slots[idx];
}

static int make_server(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }
    if (listen(fd, 32) < 0) {
        close(fd); return -1;
    }
    return fd;
}

static int recv_all(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t got = 0;
    while (got < n) {
        ssize_t r = recv(fd, p + got, n - got, 0);
        if (r <= 0) return -1;
        got += (size_t)r;
    }
    return 0;
}

static int send_all(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t sent = 0;
    while (sent < n) {
        ssize_t w = send(fd, p + sent, n - sent, 0);
        if (w <= 0) return -1;
        sent += (size_t)w;
    }
    return 0;
}

static void close_client(int *arr, int i) {
    if (arr[i] >= 0) close(arr[i]);
    arr[i] = -1;
}

static int add_client(int *arr, int fd) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (arr[i] < 0) { arr[i] = fd; return 0; }
    }
    return -1;
}

static void cmd_video_relay(int ingest_port, int sub_port) {
    int ingest_srv = make_server(ingest_port);
    int sub_srv    = make_server(sub_port);
    if (ingest_srv < 0 || sub_srv < 0) {
        fprintf(stderr, "video-relay: failed to bind ingest=%d subscribe=%d\n",
                ingest_port, sub_port);
        exit(1);
    }

    int producers[MAX_CLIENTS];
    int subscribers[MAX_CLIENTS];
    for (int i = 0; i < MAX_CLIENTS; i++) { producers[i] = -1; subscribers[i] = -1; }

    BfFrameRing ring;
    ring_init(&ring);

    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    printf("bonfyre-moq video-relay\n");
    printf("  ingest port:    %d\n", ingest_port);
    printf("  subscribe port: %d\n", sub_port);
    printf("  ring buffer:    %d frames\n", FRAME_RING);
    printf("\nProtocol: producer sends [u32_be len][bytes].\n");
    printf("Press Ctrl-C to stop.\n\n");

    uint8_t *frame_buf = (uint8_t *)malloc(MAX_FRAME_BYTES);
    if (!frame_buf) {
        fprintf(stderr, "video-relay: OOM\n");
        exit(1);
    }

    while (!g_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ingest_srv, &rfds);
        FD_SET(sub_srv, &rfds);
        int maxfd = ingest_srv > sub_srv ? ingest_srv : sub_srv;

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (producers[i] >= 0) {
                FD_SET(producers[i], &rfds);
                if (producers[i] > maxfd) maxfd = producers[i];
            }
        }

        struct timeval tv = {0, 100000}; /* 100ms tick */
        int n = select(maxfd + 1, &rfds, NULL, NULL, &tv);
        if (n < 0) {
            if (errno == EINTR) continue;
            perror("video-relay: select");
            break;
        }

        if (FD_ISSET(ingest_srv, &rfds)) {
            int cfd = accept(ingest_srv, NULL, NULL);
            if (cfd >= 0) {
                if (add_client(producers, cfd) == 0)
                    printf("[relay] producer connected fd=%d\n", cfd);
                else close(cfd);
            }
        }
        if (FD_ISSET(sub_srv, &rfds)) {
            int cfd = accept(sub_srv, NULL, NULL);
            if (cfd >= 0) {
                if (add_client(subscribers, cfd) == 0) {
                    printf("[relay] subscriber connected fd=%d\n", cfd);
                    const BfFrame *last = ring_latest(&ring);
                    if (last && last->data && last->len > 0) {
                        uint32_t nlen = htonl(last->len);
                        if (send_all(cfd, &nlen, 4) < 0 ||
                            send_all(cfd, last->data, last->len) < 0) {
                            close(cfd);
                        }
                    }
                } else close(cfd);
            }
        }

        for (int i = 0; i < MAX_CLIENTS; i++) {
            int pfd = producers[i];
            if (pfd < 0) continue;
            if (!FD_ISSET(pfd, &rfds)) continue;

            uint32_t nlen = 0;
            if (recv_all(pfd, &nlen, 4) < 0) { close_client(producers, i); continue; }
            uint32_t len = ntohl(nlen);
            if (len == 0 || len > MAX_FRAME_BYTES) { close_client(producers, i); continue; }
            if (recv_all(pfd, frame_buf, len) < 0) { close_client(producers, i); continue; }

            ring_push(&ring, frame_buf, len);

            uint32_t out_len = htonl(len);
            for (int s = 0; s < MAX_CLIENTS; s++) {
                int sfd = subscribers[s];
                if (sfd < 0) continue;
                if (send_all(sfd, &out_len, 4) < 0 || send_all(sfd, frame_buf, len) < 0)
                    close_client(subscribers, s);
            }
        }
    }

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (producers[i] >= 0) close(producers[i]);
        if (subscribers[i] >= 0) close(subscribers[i]);
    }
    close(ingest_srv);
    close(sub_srv);

    for (int i = 0; i < FRAME_RING; i++) free(ring.slots[i].data);
    free(frame_buf);
}

static void cmd_help(void) {
    printf(
"bonfyre-moq %s — live-object protocol utilities\n\n"
"USAGE\n"
"  bonfyre-moq <command> [args]\n\n"
"COMMANDS\n"
"  video-relay [--ingest PORT] [--subscribe PORT]\n"
"      start a TCP compatibility frame relay with 30-frame ring buffer\n"
"      default ingest=9450, subscribe=9451\n"
"      NOT MOQT: TCP has no MOQT mapping\n"
"  moqt-subscribe-wire --namespace A,B --track NAME [--request-id N]\n"
"      emit one draft-ietf-moq-transport-21 SUBSCRIBE wire message\n"
"      wire contract only; does not establish QUIC, authorize, or subscribe\n"
"  moqt-setup-wire --authority HOST --path /PATH\n"
"      emit a draft-21 native-QUIC control-stream SETUP message\n"
"  moqt-native-subscribe --host HOST --port PORT --authority HOST --path /PATH --namespace A,B --track NAME\n"
"      use ALPN moqt-21, QUIC DATAGRAM negotiation, a raw control stream,\n"
"      and a raw bidirectional request stream; submission is not authorization\n"
"  moqt-native-server --bind IP --port PORT --cert CERT.pem --key KEY.pem\n"
"      native QUIC moqt-21 listener that opens the server control stream\n"
"  layer <artifact_id> [--root DIR]\n"
"      show metadata-first streaming plan for a layer artifact\n"
"  help\n"
"      show this message\n\n"
"PRODUCER PROTOCOL\n"
"  connect to ingest port and send: [u32_be length][raw frame bytes]\n"
"  raw frame should be YUV420 or already browser-compatible payload\n\n"
"SUBSCRIBER PROTOCOL\n"
"  connect to subscribe port and receive the same frame packets\n"
"  newest frame is replayed immediately when subscriber joins\n",
    VERSION);
}

int main(int argc, char **argv) {
    enum {
        MQ_HELP, MQ_LAYER, MQ_MOQT_NATIVE_SERVER, MQ_MOQT_NATIVE_SUBSCRIBE,
        MQ_MOQT_SETUP_WIRE, MQ_MOQT_SUBSCRIBE_WIRE, MQ_VIDEO_RELAY,
    };
    static const BfCommandEntry moq_commands[] = {
        {"--help", MQ_HELP, 0, NULL},
        {"help", MQ_HELP, 0, NULL},
        {"layer", MQ_LAYER, 0, NULL},
        {"moqt-native-server", MQ_MOQT_NATIVE_SERVER, 0, NULL},
        {"moqt-native-subscribe", MQ_MOQT_NATIVE_SUBSCRIBE, 0, NULL},
        {"moqt-setup-wire", MQ_MOQT_SETUP_WIRE, 0, NULL},
        {"moqt-subscribe-wire", MQ_MOQT_SUBSCRIBE_WIRE, 0, NULL},
        {"video-relay", MQ_VIDEO_RELAY, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(moq_commands,
        sizeof(moq_commands) / sizeof(moq_commands[0]), argv[1]);
    if (argc < 2 || (command && command->id == MQ_HELP)) {
        cmd_help();
        return 0;
    }

    if (command && command->id == MQ_LAYER && argc >= 3) {
        const char *root = NULL;
        char *json = NULL, *out = NULL;
        for (int i = 1; i < argc - 1; i++) if (strcmp(argv[i], "--root") == 0) { root = argv[i+1]; break; }
        if (bf_layer_load_json(root, argv[2], &json) != 0) return 1;
        if (bf_layer_moq_json(json, &out) != 0) { free(json); return 1; }
        puts(out);
        free(out);
        free(json);
        return 0;
    }

    if (command && command->id == MQ_VIDEO_RELAY) {
        int ingest = 9450;
        int sub    = 9451;
        for (int i = 2; i < argc; i++) {
            if (strcmp(argv[i], "--ingest") == 0 && i + 1 < argc) ingest = atoi(argv[++i]);
            else if (strcmp(argv[i], "--subscribe") == 0 && i + 1 < argc) sub = atoi(argv[++i]);
            else {
                fprintf(stderr, "unknown arg: %s\n", argv[i]);
                return 1;
            }
        }
        cmd_video_relay(ingest, sub);
        return 0;
    }

    if (command && command->id == MQ_MOQT_SUBSCRIBE_WIRE) {
        const char *namespace_csv = NULL, *track = NULL, *request_id = NULL, *lease = NULL;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--namespace") && i + 1 < argc) namespace_csv = argv[++i];
            else if (!strcmp(argv[i], "--track") && i + 1 < argc) track = argv[++i];
            else if (!strcmp(argv[i], "--lease") && i + 1 < argc) lease = argv[++i];
            else if (!strcmp(argv[i], "--request-id") && i + 1 < argc) request_id = argv[++i];
            else { fprintf(stderr, "unknown arg: %s\n", argv[i]); return 1; }
        }
        return cmd_moqt_subscribe_wire(namespace_csv, track, lease, request_id);
    }

    if (command && command->id == MQ_MOQT_SETUP_WIRE) {
        const char *authority = NULL, *path = NULL;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--authority") && i + 1 < argc) authority = argv[++i];
            else if (!strcmp(argv[i], "--path") && i + 1 < argc) path = argv[++i];
            else { fprintf(stderr, "unknown arg: %s\n", argv[i]); return 1; }
        }
        return cmd_moqt_setup_wire(authority, path);
    }

    if (command && command->id == MQ_MOQT_NATIVE_SUBSCRIBE) {
        const char *host = NULL, *port = NULL, *authority = NULL, *path = NULL, *namespace_csv = NULL, *track = NULL, *lease = NULL;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--host") && i + 1 < argc) host = argv[++i];
            else if (!strcmp(argv[i], "--port") && i + 1 < argc) port = argv[++i];
            else if (!strcmp(argv[i], "--authority") && i + 1 < argc) authority = argv[++i];
            else if (!strcmp(argv[i], "--path") && i + 1 < argc) path = argv[++i];
            else if (!strcmp(argv[i], "--namespace") && i + 1 < argc) namespace_csv = argv[++i];
            else if (!strcmp(argv[i], "--track") && i + 1 < argc) track = argv[++i];
            else if (!strcmp(argv[i], "--lease") && i + 1 < argc) lease = argv[++i];
            else { fprintf(stderr, "unknown arg: %s\n", argv[i]); return 1; }
        }
        return cmd_moqt_native_subscribe(host, port, authority, path, namespace_csv, track, lease);
    }

    if (command && command->id == MQ_MOQT_NATIVE_SERVER) {
        const char *bind = NULL, *port = NULL, *cert = NULL, *key = NULL, *delivery_db = NULL, *delivery_bin = NULL;
        for (int i = 2; i < argc; i++) {
            if (!strcmp(argv[i], "--bind") && i + 1 < argc) bind = argv[++i];
            else if (!strcmp(argv[i], "--port") && i + 1 < argc) port = argv[++i];
            else if (!strcmp(argv[i], "--cert") && i + 1 < argc) cert = argv[++i];
            else if (!strcmp(argv[i], "--key") && i + 1 < argc) key = argv[++i];
            else if (!strcmp(argv[i], "--delivery-db") && i + 1 < argc) delivery_db = argv[++i];
            else if (!strcmp(argv[i], "--delivery-bin") && i + 1 < argc) delivery_bin = argv[++i];
            else { fprintf(stderr, "unknown arg: %s\n", argv[i]); return 1; }
        }
        return cmd_moqt_native_server(bind, port, cert, key, delivery_db, delivery_bin);
    }

    fprintf(stderr, "bonfyre-moq: unknown command '%s'\n", argv[1]);
    fprintf(stderr, "Run 'bonfyre-moq help' for usage.\n");
    return 1;
}
