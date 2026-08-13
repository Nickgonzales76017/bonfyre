#define _POSIX_C_SOURCE 200809L
#include "bonfyre_fabric.h"

#include <arpa/inet.h>
#include <signal.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <unistd.h>

static volatile sig_atomic_t running = 1;
static void stop(int signal_number) {
    (void)signal_number;
    running = 0;
}

static void send_response(int fd, int status, const char *content_type, const char *body) {
    const char *reason = status == 200 ? "OK" : "Not Found";
    dprintf(fd,
            "HTTP/1.1 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\n"
            "Connection: close\r\n\r\n%s",
            status, reason, content_type, strlen(body), body);
}

static void handle_client(int fd, sqlite3 *db) {
    char request[2048] = {0};
    ssize_t bytes = read(fd, request, sizeof(request) - 1);
    if (bytes <= 0) {
        return;
    }
    if (!strncmp(request, "GET /health ", 12)) {
        send_response(fd, 200, "application/json",
                      "{\"service\":\"bonfyred\",\"state\":\"ready\"}\n");
        return;
    }
    if (!strncmp(request, "GET /meta ", 10)) {
        sqlite3_stmt *statement = NULL;
        char generation[65] = "uncompiled";
        int public_commands = 0;
        int typed_contracts = 0;
        char body[512];

        if (sqlite3_prepare_v2(db,
                "SELECT value FROM fabric_meta WHERE key='catalog_generation'",
                -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW) {
            snprintf(generation, sizeof(generation), "%s",
                     sqlite3_column_text(statement, 0));
        }
        sqlite3_finalize(statement);
        statement = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT count(*),sum(EXISTS(SELECT 1 FROM operator_contract_bindings c "
                "WHERE c.operator_id=b.operator_id)) FROM catalog_bindings b "
                "WHERE b.operator_id LIKE 'command.%' AND b.binding_state='bound'",
                -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW) {
            public_commands = sqlite3_column_int(statement, 0);
            typed_contracts = sqlite3_column_int(statement, 1);
        }
        sqlite3_finalize(statement);
        snprintf(body, sizeof(body),
                 "{\"api_version\":\"v1\",\"catalog_generation\":\"%s\","
                 "\"public_commands\":%d,\"typed_command_contracts\":%d}\n",
                 generation, public_commands, typed_contracts);
        send_response(fd, 200, "application/json", body);
        return;
    }
    if (!strncmp(request, "GET /catalog ", 13)) {
        sqlite3_stmt *statement = NULL;
        int total = 0;
        int proven = 0;
        int public_commands = 0;
        char body[384];

        sqlite3_prepare_v2(db,
            "SELECT count(*),sum(maturity='workload_proven'),"
            "sum(id LIKE 'command.%') FROM catalog", -1, &statement, NULL);
        if (sqlite3_step(statement) == SQLITE_ROW) {
            total = sqlite3_column_int(statement, 0);
            proven = sqlite3_column_int(statement, 1);
            public_commands = sqlite3_column_int(statement, 2);
        }
        sqlite3_finalize(statement);
        snprintf(body, sizeof(body),
                 "{\"catalog\":{\"operators\":%d,\"public_commands\":%d,"
                 "\"workload_proven\":%d}}\n",
                 total, public_commands, proven);
        send_response(fd, 200, "application/json", body);
        return;
    }
    if (!strncmp(request, "GET /mission/", 13)) {
        char mission[160] = {0};
        if (sscanf(request, "GET /mission/%159[^ ?]", mission) == 1) {
            sqlite3_stmt *statement = NULL;
            sqlite3_prepare_v2(db,
                "SELECT m.status,m.workgraph_cursor,count(n.node_id),"
                "sum(n.status='complete') FROM missions m "
                "LEFT JOIN workgraph_nodes n ON n.mission_id=m.id "
                "WHERE m.id=? GROUP BY m.id", -1, &statement, NULL);
            sqlite3_bind_text(statement, 1, mission, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(statement) == SQLITE_ROW) {
                char body[512];
                snprintf(body, sizeof(body),
                         "{\"mission\":\"%s\",\"status\":\"%s\","
                         "\"cursor\":\"%s\",\"nodes\":%d,\"completed_nodes\":%d}\n",
                         mission, sqlite3_column_text(statement, 0),
                         sqlite3_column_text(statement, 1),
                         sqlite3_column_int(statement, 2),
                         sqlite3_column_int(statement, 3));
                sqlite3_finalize(statement);
                send_response(fd, 200, "application/json", body);
                return;
            }
            sqlite3_finalize(statement);
        }
    }
    if (!strncmp(request, "GET /operator ", 14)) {
        sqlite3_stmt *statement = NULL;
        char generation[65] = "uncompiled";
        int missions = 0;
        int ready = 0;
        int running_nodes = 0;
        char body[2048];

        if (sqlite3_prepare_v2(db,
                "SELECT value FROM fabric_meta WHERE key='catalog_generation'",
                -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW) {
            snprintf(generation, sizeof(generation), "%s",
                     sqlite3_column_text(statement, 0));
        }
        sqlite3_finalize(statement);
        statement = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT (SELECT count(*) FROM missions),"
                "(SELECT count(*) FROM workgraph_nodes WHERE status='ready'),"
                "(SELECT count(*) FROM workgraph_nodes WHERE status='running')",
                -1, &statement, NULL) == SQLITE_OK &&
            sqlite3_step(statement) == SQLITE_ROW) {
            missions = sqlite3_column_int(statement, 0);
            ready = sqlite3_column_int(statement, 1);
            running_nodes = sqlite3_column_int(statement, 2);
        }
        sqlite3_finalize(statement);
        snprintf(body, sizeof(body),
                 "<!doctype html><html><head><title>Bonfyre Operator</title></head>"
                 "<body><main data-catalog-generation=\"%s\">"
                 "<h1>Bonfyre Operator</h1><dl>"
                 "<dt>Missions</dt><dd id=\"mission-count\">%d</dd>"
                 "<dt>Ready nodes</dt><dd id=\"ready-count\">%d</dd>"
                 "<dt>Running nodes</dt><dd id=\"running-count\">%d</dd>"
                 "</dl></main></body></html>\n",
                 generation, missions, ready, running_nodes);
        send_response(fd, 200, "text/html; charset=utf-8", body);
        return;
    }
    send_response(fd, 404, "application/json", "{\"error\":\"not_found\"}\n");
}

static int serve(int port) {
    char state[4096], db_path[4096];
    if (bf_fabric_bootstrap(state, sizeof(state), db_path, sizeof(db_path), stderr) != 0) return 1;
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) { fprintf(stderr, "bonfyred: Fabric must be initialized before serving\n"); sqlite3_close(db); return 1; }
    int server = socket(AF_INET, SOCK_STREAM, 0); int yes = 1;
    if (server < 0 || setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) != 0) { sqlite3_close(db); return 1; }
    struct sockaddr_in address = { .sin_family = AF_INET, .sin_port = htons((unsigned short)port), .sin_addr.s_addr = htonl(0x7f000001u) };
    if (bind(server, (struct sockaddr *)&address, sizeof(address)) != 0 || listen(server, 16) != 0) { perror("bonfyred bind"); close(server); sqlite3_close(db); return 1; }
    signal(SIGINT, stop); signal(SIGTERM, stop);
    fprintf(stderr, "bonfyred listening on 127.0.0.1:%d\n", port);
    while (running) {
        fd_set read_set; FD_ZERO(&read_set); FD_SET(server, &read_set);
        struct timeval timeout = { .tv_sec = 1, .tv_usec = 0 };
        int ready = select(server + 1, &read_set, NULL, NULL, &timeout);
        if (ready <= 0) continue;
        int client = accept(server, NULL, NULL);
        if (client < 0) continue;
        handle_client(client, db); close(client);
    }
    close(server); sqlite3_close(db); return 0;
}

int main(int argc, char **argv) {
    char state[4096], db[4096];
    if (argc > 1 && strcmp(argv[1], "--health") == 0) {
        if (bf_fabric_bootstrap(state, sizeof(state), db, sizeof(db), stderr) != 0) return 1;
        printf("bonfyred=ready\nstate_dir=%s\ndatabase=%s\n", state, db);
        return 0;
    }
    if (argc > 1 && strcmp(argv[1], "serve") == 0) {
        int port = 9070;
        if (argc == 4 && strcmp(argv[2], "--port") == 0) port = atoi(argv[3]);
        if (port < 1024 || port > 65535) { fprintf(stderr, "bonfyred: invalid port\n"); return 2; }
        return serve(port);
    }
    fprintf(stderr, "usage: bonfyred --health | serve [--port PORT]\n");
    return 2;
}
