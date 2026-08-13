#define _POSIX_C_SOURCE 200809L
#include "bf_operator.h"

#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

static void process_error(char *buffer, size_t size, const char *format, ...) {
    va_list args;
    if (size == 0) return;
    va_start(args, format);
    vsnprintf(buffer, size, format, args);
    va_end(args);
}

static long elapsed_ms(struct timespec start, struct timespec finish) {
    return (finish.tv_sec - start.tv_sec) * 1000L +
           (finish.tv_nsec - start.tv_nsec) / 1000000L;
}

static int output_size(const char *path, size_t *size) {
    struct stat info;
    if (stat(path, &info) != 0) return -1;
    if (info.st_size < 0) return -1;
    *size = (size_t)info.st_size;
    return 0;
}

static void terminate_group(pid_t pid) {
    kill(-pid, SIGTERM);
    nanosleep(&(struct timespec){ .tv_nsec = 100000000L }, NULL);
    kill(-pid, SIGKILL);
}

int bf_process_operator_run(const BfProcessRequest *request,
                            BfProcessResult *result,
                            char *error,
                            size_t error_size) {
    int stdout_fd = -1;
    int stderr_fd = -1;
    int wait_status = 0;
    int child_complete = 0;
    pid_t pid;
    struct timespec start, now;

    if (result) memset(result, 0, sizeof(*result));
    if (!request || !request->executable || !request->argv || !request->argv[0] ||
        !request->working_directory || !request->stdout_path || !request->stderr_path ||
        request->timeout_seconds == 0 || request->output_limit_bytes == 0) {
        process_error(error, error_size, "invalid process operator request");
        return -1;
    }
    if (access(request->executable, X_OK) != 0) {
        process_error(error, error_size, "authorized executable unavailable: %s", request->executable);
        return -1;
    }

    stdout_fd = open(request->stdout_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    stderr_fd = open(request->stderr_path, O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (stdout_fd < 0 || stderr_fd < 0) {
        process_error(error, error_size, "cannot create process capture files: %s", strerror(errno));
        if (stdout_fd >= 0) close(stdout_fd);
        if (stderr_fd >= 0) close(stderr_fd);
        return -1;
    }

    clock_gettime(CLOCK_MONOTONIC, &start);
    pid = fork();
    if (pid == 0) {
        char *const safe_env[] = { "PATH=/usr/bin:/bin", "LANG=C", NULL };
        int stdin_fd = open("/dev/null", O_RDONLY);
        setpgid(0, 0);
        if (stdin_fd < 0 || chdir(request->working_directory) != 0 ||
            dup2(stdin_fd, STDIN_FILENO) < 0 ||
            dup2(stdout_fd, STDOUT_FILENO) < 0 ||
            dup2(stderr_fd, STDERR_FILENO) < 0) _exit(127);
        close(stdin_fd);
        close(stdout_fd);
        close(stderr_fd);
        execve(request->executable, (char *const *)request->argv,
               request->environment ? (char *const *)request->environment : safe_env);
        _exit(127);
    }
    close(stdout_fd);
    close(stderr_fd);
    if (pid < 0) {
        process_error(error, error_size, "cannot create operator process: %s", strerror(errno));
        return -1;
    }

    while (!child_complete) {
        size_t stdout_size = 0;
        size_t stderr_size = 0;
        pid_t waited = waitpid(pid, &wait_status, WNOHANG);
        if (waited == pid) {
            child_complete = 1;
            break;
        }
        if (waited < 0) {
            process_error(error, error_size, "waitpid failed: %s", strerror(errno));
            terminate_group(pid);
            waitpid(pid, &wait_status, 0);
            return -1;
        }
        if (output_size(request->stdout_path, &stdout_size) != 0 ||
            output_size(request->stderr_path, &stderr_size) != 0 ||
            stdout_size > request->output_limit_bytes ||
            stderr_size > request->output_limit_bytes) {
            terminate_group(pid);
            waitpid(pid, &wait_status, 0);
            process_error(error, error_size, "operator output exceeded bounded capture policy");
            return -1;
        }
        clock_gettime(CLOCK_MONOTONIC, &now);
        if ((unsigned long)elapsed_ms(start, now) > (unsigned long)request->timeout_seconds * 1000UL) {
            terminate_group(pid);
            waitpid(pid, &wait_status, 0);
            if (result) result->timed_out = 1;
            process_error(error, error_size, "operator timed out after %u seconds", request->timeout_seconds);
            return -1;
        }
        nanosleep(&(struct timespec){ .tv_nsec = 10000000L }, NULL);
    }
    clock_gettime(CLOCK_MONOTONIC, &now);
    if (result) {
        result->duration_ms = elapsed_ms(start, now);
        /* Per-child rusage is not portable across supported host SDKs. */
        result->cpu_user_ms = -1;
        result->cpu_system_ms = -1;
        output_size(request->stdout_path, &result->stdout_bytes);
        output_size(request->stderr_path, &result->stderr_bytes);
        if (WIFEXITED(wait_status)) result->exit_code = WEXITSTATUS(wait_status);
        if (WIFSIGNALED(wait_status)) result->terminating_signal = WTERMSIG(wait_status);
    }
    if ((result && (result->stdout_bytes > request->output_limit_bytes ||
                    result->stderr_bytes > request->output_limit_bytes))) {
        process_error(error, error_size, "operator output exceeded bounded capture policy");
        return -1;
    }
    if (!WIFEXITED(wait_status) || WEXITSTATUS(wait_status) != 0) {
        process_error(error, error_size, "operator terminated: exit=%d signal=%d",
                      WIFEXITED(wait_status) ? WEXITSTATUS(wait_status) : -1,
                      WIFSIGNALED(wait_status) ? WTERMSIG(wait_status) : 0);
        return -1;
    }
    return 0;
}
