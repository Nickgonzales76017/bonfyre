#define _POSIX_C_SOURCE 200809L
#include "recall.h"

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

static char g_cmd_root[PATH_MAX];

void bf_recall_init(const char *argv0) {
    char self[PATH_MAX];
    if (!argv0 || !realpath(argv0, self)) return;
    char *slash = strrchr(self, '/');
    if (!slash) return;
    *slash = '\0';
    slash = strrchr(self, '/');
    if (!slash) return;
    *slash = '\0';
    snprintf(g_cmd_root, sizeof(g_cmd_root), "%s", self);
}

const char *bf_opt(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; i++)
        if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

const char *bf_recall_binpath(const char *env, const char *name,
                              char *buf, size_t cap) {
    const char *value = getenv(env);
    const char *dir;
    const char *home;
    if (value && *value) return value;
    dir = getenv("BONFYRE_BIN_DIR");
    if (dir && *dir) {
        snprintf(buf, cap, "%s/%s", dir, name);
        return buf;
    }
    if (g_cmd_root[0]) {
        const char *family =
            !strcmp(name, "bonfyre-embed") ? "BonfyreEmbed" :
            !strcmp(name, "bonfyre-vec") ? "BonfyreVec" :
            !strcmp(name, "bonfyre-paragraph") ? "BonfyreParagraph" : NULL;
        if (family) {
            snprintf(buf, cap, "%s/%s/%s", g_cmd_root, family, name);
            if (access(buf, X_OK) == 0) return buf;
        }
    }
    home = getenv("HOME");
    if (home && *home) {
        snprintf(buf, cap, "%s/.bonfyre/bin/%s", home, name);
        if (access(buf, X_OK) == 0) return buf;
    }
    snprintf(buf, cap, "/usr/local/bin/%s", name);
    return buf;
}

int bf_recall_run(char *const argv[], char *out, size_t out_cap,
                  char *err, size_t err_cap) {
    int out_pipe[2], err_pipe[2], status = 0;
    pid_t pid;
    if (pipe(out_pipe) || pipe(err_pipe)) return -1;
    pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        dup2(out_pipe[1], STDOUT_FILENO);
        dup2(err_pipe[1], STDERR_FILENO);
        close(out_pipe[0]);
        close(err_pipe[0]);
        execv(argv[0], argv);
        _exit(127);
    }
    close(out_pipe[1]);
    close(err_pipe[1]);
    {
        int fd[2] = {out_pipe[0], err_pipe[0]}, open_count = 2;
        char *buffer[2] = {out, err};
        size_t cap[2] = {out_cap, err_cap}, used[2] = {0, 0};
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
                if (used[i] + 1 >= cap[i]) {
                    close(fd[i]);
                    fd[i] = -1;
                    open_count--;
                    continue;
                }
                n = read(fd[i], buffer[i] + used[i], cap[i] - used[i] - 1);
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

void bf_json_file(FILE *out, const char *text) {
    fputc('"', out);
    for (; text && *text; text++) {
        unsigned char c = (unsigned char)*text;
        if (c == '"' || c == '\\') {
            fputc('\\', out);
            fputc(c, out);
        } else if (c == '\n') fputs("\\n", out);
        else if (c == '\r') fputs("\\r", out);
        else if (c == '\t') fputs("\\t", out);
        else if (c < 0x20) fprintf(out, "\\u%04x", c);
        else fputc(c, out);
    }
    fputc('"', out);
}

void bf_json_stdout(const char *text) {
    bf_json_file(stdout, text);
}

int bf_mkdir_p(const char *path) {
    char tmp[PATH_MAX];
    size_t len;
    if (!path || !*path || strlen(path) >= sizeof(tmp)) return 1;
    snprintf(tmp, sizeof(tmp), "%s", path);
    len = strlen(tmp);
    if (len && tmp[len - 1] == '/') tmp[len - 1] = '\0';
    for (char *p = tmp + 1; *p; p++) {
        if (*p != '/') continue;
        *p = '\0';
        if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
        *p = '/';
    }
    return mkdir(tmp, 0755) != 0 && errno != EEXIST;
}

int bf_nonempty_jsonl_count(const char *path) {
    FILE *f = fopen(path, "r");
    char *line = NULL;
    size_t cap = 0;
    ssize_t n;
    int count = 0;
    if (!f) return -1;
    while ((n = getline(&line, &cap, f)) >= 0) {
        int nonspace = 0;
        for (ssize_t i = 0; i < n; i++)
            if ((unsigned char)line[i] > 0x20) { nonspace = 1; break; }
        if (nonspace) count++;
    }
    free(line);
    fclose(f);
    return count;
}
