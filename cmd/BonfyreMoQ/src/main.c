/*
 * bonfyre-moq — Media over QUIC style relay for Bonfyre
 *
 * This implementation provides a practical local relay mode for video frames:
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

#define VERSION "1.0.0"
#define MAX_CLIENTS 128
#define MAX_FRAME_BYTES (2 * 1024 * 1024) /* 2MB/frame (YUV420 typical for 720p) */
#define FRAME_RING 30

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
"bonfyre-moq %s — media relay\n\n"
"USAGE\n"
"  bonfyre-moq <command> [args]\n\n"
"COMMANDS\n"
"  video-relay [--ingest PORT] [--subscribe PORT]\n"
"      start video frame relay with 30-frame ring buffer\n"
"      default ingest=9450, subscribe=9451\n"
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
    if (argc < 2 || strcmp(argv[1], "help") == 0 || strcmp(argv[1], "--help") == 0) {
        cmd_help();
        return 0;
    }

    if (strcmp(argv[1], "layer") == 0 && argc >= 3) {
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

    if (strcmp(argv[1], "video-relay") == 0) {
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

    fprintf(stderr, "bonfyre-moq: unknown command '%s'\n", argv[1]);
    fprintf(stderr, "Run 'bonfyre-moq help' for usage.\n");
    return 1;
}
