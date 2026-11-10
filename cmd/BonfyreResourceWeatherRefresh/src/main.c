#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"

static const char *arg(int argc, char **argv, const char *name) {
    for (int i = 2; i + 1 < argc; ++i) if (!strcmp(argv[i], name)) return argv[i + 1];
    return NULL;
}

static char *slurp(const char *path) {
    FILE *f = fopen(path, "rb"); char *body = NULL; long n;
    if (!f || fseek(f, 0, SEEK_END) || (n = ftell(f)) < 0 || fseek(f, 0, SEEK_SET)) goto done;
    body = malloc((size_t)n + 1);
    if (!body || (n && fread(body, 1, (size_t)n, f) != (size_t)n)) { free(body); body = NULL; goto done; }
    body[n] = '\0';
done:
    if (f) fclose(f);
    return body;
}

static int run_capture(const char *bin, char *const av[], const char *out, const char *weather_tmp) {
    int fd, status = 0; pid_t child;
    if (!bin || !*bin || !out || (fd = open(out, O_WRONLY | O_CREAT | O_TRUNC, 0640)) < 0) return 0;
    child = fork();
    if (child < 0) { close(fd); return 0; }
    if (child == 0) {
        if (weather_tmp && setenv("BONFYRE_WEATHER", weather_tmp, 1)) _exit(127);
        if (dup2(fd, STDOUT_FILENO) < 0) _exit(127);
        close(fd);
        execv(bin, av);
        _exit(127);
    }
    if (waitpid(child, &status, 0) != child || fsync(fd)) { close(fd); return 0; }
    close(fd);
    return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}

static int weather_body_valid(const char *path) {
    char *body = slurp(path);
    int ok = body && strstr(body, "\"schema\":\"bonfyre-resource-weather.v1\"") && strstr(body, "\"sources\"");
    free(body);
    return ok;
}

static void usage(void) {
    fputs("bonfyre-resource-weather-refresh refresh --project BIN --transition BIN --weather SNAPSHOT --db DB --delta-bin BIN --reheat-bin BIN --weather-gate BIN --gate-db DB --address-db DB --lease-db DB --reheat-db DB --acquire-dir DIR --cancel-dir DIR --out-dir DIR --delta-dir DIR\n", stderr);
}

int main(int argc, char **argv) {
    const char *project = arg(argc, argv, "--project"), *transition = arg(argc, argv, "--transition");
    const char *weather = arg(argc, argv, "--weather"), *db = arg(argc, argv, "--db");
    const char *delta = arg(argc, argv, "--delta-bin"), *reheat = arg(argc, argv, "--reheat-bin");
    const char *gate = arg(argc, argv, "--weather-gate"), *gate_db = arg(argc, argv, "--gate-db");
    const char *address = arg(argc, argv, "--address-db"), *lease = arg(argc, argv, "--lease-db");
    const char *reheat_db = arg(argc, argv, "--reheat-db"), *acquire = arg(argc, argv, "--acquire-dir");
    const char *cancel = arg(argc, argv, "--cancel-dir"), *out = arg(argc, argv, "--out-dir");
    const char *delta_dir = arg(argc, argv, "--delta-dir");
    char weather_tmp[4096], project_out[4096], transition_out[4096];
    char *transition_body = NULL; int weather_fd = -1, ok = 0;

    enum { RR_REFRESH };
    static const BfCommandEntry refresh_commands[] = {
        {"refresh", RR_REFRESH, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(refresh_commands,
        sizeof(refresh_commands) / sizeof(refresh_commands[0]), argv[1]);

    if (argc < 2 || !command || command->id != RR_REFRESH || !project || !transition || !weather || !db || !delta || !reheat || !gate || !gate_db || !address || !lease || !reheat_db || !acquire || !cancel || !out || !delta_dir) { usage(); return 2; }
    if (snprintf(weather_tmp, sizeof(weather_tmp), "%s.project.XXXXXX", weather) >= (int)sizeof(weather_tmp) ||
        snprintf(project_out, sizeof(project_out), "%s.project.out", weather) >= (int)sizeof(project_out) ||
        snprintf(transition_out, sizeof(transition_out), "%s.transition.out", weather) >= (int)sizeof(transition_out)) return 2;
    weather_fd = mkstemp(weather_tmp);
    if (weather_fd < 0) goto done;
    close(weather_fd); weather_fd = -1;
    {
        char *const project_av[] = { (char *)project, "foreign", "reheat", "--dry-run", NULL };
        if (!run_capture(project, project_av, project_out, weather_tmp) || !weather_body_valid(weather_tmp) || rename(weather_tmp, weather)) goto done;
    }
    {
        char *const transition_av[] = {
            (char *)transition, "advance", "--db", (char *)db, "--snapshot", (char *)weather,
            "--delta-bin", (char *)delta, "--reheat-bin", (char *)reheat,
            "--weather-gate", (char *)gate, "--gate-db", (char *)gate_db,
            "--address-db", (char *)address, "--lease-db", (char *)lease,
            "--reheat-db", (char *)reheat_db, "--acquire-dir", (char *)acquire,
            "--cancel-dir", (char *)cancel, "--out-dir", (char *)out,
            "--delta-dir", (char *)delta_dir, NULL
        };
        if (!run_capture(transition, transition_av, transition_out, NULL)) goto done;
    }
    transition_body = slurp(transition_out);
    if (!transition_body || transition_body[0] != '{') goto done;
    printf("{\"schema\":\"bonfyre-resource-weather-refresh.v1\",\"weather\":\"%s\",\"project_dry_run\":true,\"transition\":%s,\"truth\":\"whole_census_weather_to_targeted_physical_intent_only\",\"cannot_infer\":[\"dry weather census -> foreign observation\",\"material handoff -> retained body\",\"transition result -> authority or evidence\"]}\n", weather, transition_body);
    ok = 1;
done:
    if (weather_fd >= 0) close(weather_fd);
    unlink(weather_tmp);
    free(transition_body);
    return ok ? 0 : 1;
}
