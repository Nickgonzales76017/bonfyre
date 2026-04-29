#include <dirent.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef __APPLE__
#include <mach-o/dyld.h>
#endif

static int read_file(const char *path, char **out) {
    FILE *fp;
    long sz;
    char *buf;
    if (!path || !out) return 1;
    *out = NULL;
    fp = fopen(path, "rb");
    if (!fp) return 1;
    fseek(fp, 0, SEEK_END);
    sz = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    if (sz < 0) { fclose(fp); return 1; }
    buf = (char *)malloc((size_t)sz + 1);
    if (!buf) { fclose(fp); return 1; }
    if (fread(buf, 1, (size_t)sz, fp) != (size_t)sz) {
        free(buf);
        fclose(fp);
        return 1;
    }
    buf[sz] = '\0';
    fclose(fp);
    *out = buf;
    return 0;
}

static void get_self_dir(char *buf, size_t sz) {
    char self[PATH_MAX];
    memset(self, 0, sizeof(self));
#ifdef __APPLE__
    uint32_t bsz = sizeof(self);
    if (_NSGetExecutablePath(self, &bsz) != 0) self[0] = '\0';
#elif defined(__linux__)
    ssize_t n = readlink("/proc/self/exe", self, sizeof(self) - 1);
    if (n > 0) self[n] = '\0'; else self[0] = '\0';
#else
    self[0] = '\0';
#endif
    if (self[0]) {
        char *last = strrchr(self, '/');
        if (last) {
            *last = '\0';
            snprintf(buf, sz, "%s", self);
            return;
        }
    }
    buf[0] = '\0';
}

static int resolve_clients_dir(char *out, size_t out_sz) {
    const char *candidates[] = {
        "clients",
        "../clients",
        "../../clients",
        "../../../clients",
        NULL
    };
    struct stat st;
    char self_dir[PATH_MAX];

    for (int i = 0; candidates[i]; i++) {
        if (stat(candidates[i], &st) == 0 && S_ISDIR(st.st_mode)) {
            snprintf(out, out_sz, "%s", candidates[i]);
            return 0;
        }
    }

    get_self_dir(self_dir, sizeof(self_dir));
    if (self_dir[0]) {
        for (int i = 0; candidates[i]; i++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s", self_dir, candidates[i]);
            if (stat(path, &st) == 0 && S_ISDIR(st.st_mode)) {
                snprintf(out, out_sz, "%s", path);
                return 0;
            }
        }
    }

    return 1;
}

static int surface_file_path(const char *surface_id, char *out, size_t out_sz) {
    DIR *root;
    struct dirent *ent;
    char clients_dir[PATH_MAX];
    if (!surface_id || !out || out_sz == 0) return 1;
    if (resolve_clients_dir(clients_dir, sizeof(clients_dir)) != 0) return 1;
    root = opendir(clients_dir);
    if (!root) return 1;
    while ((ent = readdir(root)) != NULL) {
        char path[PATH_MAX];
        char *text = NULL;
        if (ent->d_name[0] == '.') continue;
        snprintf(path, sizeof(path), "%s/%s/client_surface.yaml", clients_dir, ent->d_name);
        if (read_file(path, &text) == 0) {
            char needle[256];
            snprintf(needle, sizeof(needle), "adapter: %s", surface_id);
            if (strstr(text, needle) != NULL) {
                snprintf(out, out_sz, "%s", path);
                free(text);
                closedir(root);
                return 0;
            }
            free(text);
        }
    }
    closedir(root);
    return 1;
}

static int cmd_list(void) {
    char clients_dir[PATH_MAX];
    DIR *root;
    struct dirent *ent;
    int found = 0;
    if (resolve_clients_dir(clients_dir, sizeof(clients_dir)) != 0) {
        fprintf(stderr, "bonfyre-surface: no clients directory\n");
        return 1;
    }
    root = opendir(clients_dir);
    if (!root) {
        fprintf(stderr, "bonfyre-surface: no clients directory\n");
        return 1;
    }
    while ((ent = readdir(root)) != NULL) {
        char path[PATH_MAX];
        char *text = NULL;
        char *adapter, *type;
        if (ent->d_name[0] == '.') continue;
        snprintf(path, sizeof(path), "%s/%s/client_surface.yaml", clients_dir, ent->d_name);
        if (read_file(path, &text) != 0) continue;
        adapter = strstr(text, "adapter:");
        type = strstr(text, "type:");
        if (adapter) {
            char aid[128] = "";
            sscanf(adapter, "adapter: %127s", aid);
            printf("%s", aid);
            if (type) {
                char t[128] = "";
                sscanf(type, "type: %127s", t);
                printf("  %s", t);
            }
            printf("  %s\n", path);
            found = 1;
        }
        free(text);
    }
    closedir(root);
    return found ? 0 : 1;
}

static int cmd_show(const char *surface_id) {
    char path[PATH_MAX];
    char *text = NULL;
    if (surface_file_path(surface_id, path, sizeof(path)) != 0) {
        fprintf(stderr, "bonfyre-surface: unknown surface '%s'\n", surface_id);
        return 1;
    }
    if (read_file(path, &text) != 0) return 1;
    printf("%s", text);
    free(text);
    return 0;
}

static int cmd_validate(const char *path) {
    char *text = NULL;
    int ok;
    if (read_file(path, &text) != 0) {
        fprintf(stderr, "bonfyre-surface: cannot read %s\n", path);
        return 1;
    }
    ok =
        strstr(text, "adapter:") &&
        strstr(text, "type:") &&
        strstr(text, "reads:") &&
        strstr(text, "emits:") &&
        strstr(text, "requires:") &&
        strstr(text, "does_not_own:");
    printf("{\"path\":\"%s\",\"valid\":%s}\n", path, ok ? "true" : "false");
    free(text);
    return ok ? 0 : 1;
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "list") == 0) return cmd_list();
    if (argc >= 3 && strcmp(argv[1], "show") == 0) return cmd_show(argv[2]);
    if (argc >= 3 && strcmp(argv[1], "validate") == 0) return cmd_validate(argv[2]);
    fprintf(stderr,
            "BonfyreSurface — optional client surface registry\n\n"
            "Usage:\n"
            "  bonfyre-surface list\n"
            "  bonfyre-surface show <surface_id>\n"
            "  bonfyre-surface validate <client_surface.yaml>\n");
    return 1;
}
