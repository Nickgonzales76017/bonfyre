#define _POSIX_C_SOURCE 200809L
#include "proof.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

char *bf_proof_read_file(const char *path) {
    FILE *f = fopen(path, "rb");
    long size;
    char *body;
    if (!f) return NULL;
    if (fseek(f, 0, SEEK_END) != 0 || (size = ftell(f)) < 0 ||
        fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }
    body = malloc((size_t)size + 1);
    if (!body) { fclose(f); return NULL; }
    if (size && fread(body, 1, (size_t)size, f) != (size_t)size) {
        free(body);
        fclose(f);
        return NULL;
    }
    body[size] = '\0';
    fclose(f);
    return body;
}

void bf_dirname_copy(const char *path, char *out, size_t cap) {
    char copy[PATH_MAX];
    char *slash;
    if (!out || !cap) return;
    snprintf(copy, sizeof(copy), "%s", path ? path : "");
    slash = strrchr(copy, '/');
    if (slash) *slash = '\0';
    else snprintf(copy, sizeof(copy), ".");
    snprintf(out, cap, "%s", copy);
}

void bf_init_repo_root(const char *argv0, char *out, size_t cap) {
    char self[PATH_MAX], level[PATH_MAX];
    out[0] = '\0';
    if (!argv0 || !realpath(argv0, self)) return;
    bf_dirname_copy(self, level, sizeof(level));
    bf_dirname_copy(level, level, sizeof(level));
    bf_dirname_copy(level, out, cap);
}

const char *bf_owner_bin(const char *env_name, const char *name,
                         char *buf, size_t cap) {
    const char *configured = getenv(env_name);
    const char *home = getenv("HOME");
    if (configured && *configured) return configured;
    if (home && *home) {
        snprintf(buf, cap, "%s/.bonfyre/bin/%s", home, name);
        if (access(buf, X_OK) == 0) return buf;
    }
    snprintf(buf, cap, "/usr/local/bin/%s", name);
    return buf;
}

int bf_run_capture(char *const argv[], const char *env_name,
                   const char *env_value, char *out, size_t out_cap,
                   char *err, size_t err_cap) {
    int op[2], ep[2], status = 0;
    pid_t pid;
    if (pipe(op) || pipe(ep)) return -1;
    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        if (env_name && env_value) setenv(env_name, env_value, 1);
        dup2(op[1], STDOUT_FILENO);
        dup2(ep[1], STDERR_FILENO);
        close(op[0]);
        close(ep[0]);
        execv(argv[0], argv);
        _exit(127);
    }
    close(op[1]);
    close(ep[1]);
    {
        int fd[2] = {op[0], ep[0]}, open_count = 2;
        char *buffer[2] = {out, err};
        size_t caps[2] = {out_cap, err_cap}, used[2] = {0, 0};
        for (int i = 0; i < 2; i++) {
            int flags = fcntl(fd[i], F_GETFL, 0);
            if (flags >= 0) fcntl(fd[i], F_SETFL, flags | O_NONBLOCK);
        }
        while (open_count) {
            fd_set rd;
            int maxfd = -1;
            FD_ZERO(&rd);
            for (int i = 0; i < 2; i++) if (fd[i] >= 0) {
                FD_SET(fd[i], &rd);
                if (fd[i] > maxfd) maxfd = fd[i];
            }
            if (maxfd < 0) break;
            if (select(maxfd + 1, &rd, NULL, NULL, NULL) < 0) {
                if (errno == EINTR) continue;
                break;
            }
            for (int i = 0; i < 2; i++) if (fd[i] >= 0 && FD_ISSET(fd[i], &rd)) {
                ssize_t n;
                if (used[i] + 1 >= caps[i]) {
                    close(fd[i]);
                    fd[i] = -1;
                    open_count--;
                    continue;
                }
                n = read(fd[i], buffer[i] + used[i], caps[i] - used[i] - 1);
                if (n > 0) used[i] += (size_t)n;
                else if (!n || (errno != EAGAIN && errno != EINTR)) {
                    close(fd[i]);
                    fd[i] = -1;
                    open_count--;
                }
            }
        }
        for (int i = 0; i < 2; i++) {
            if (fd[i] >= 0) close(fd[i]);
            buffer[i][used[i]] = '\0';
        }
    }
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

void bf_json_string(const char *text) {
    putchar('"');
    for (; text && *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') {
            putchar('\\');
            putchar(c);
        } else if (c == '\n') fputs("\\n", stdout);
        else if (c == '\r') fputs("\\r", stdout);
        else if (c == '\t') fputs("\\t", stdout);
        else if (c < 0x20) printf("\\u%04x", c);
        else putchar(c);
    }
    putchar('"');
}
