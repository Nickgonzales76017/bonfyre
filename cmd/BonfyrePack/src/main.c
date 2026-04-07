#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <bonfyre.h>

extern char **environ;

#define MAX_PATH 2048

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static void path_join(char *buffer, size_t size, const char *left, const char *right) {
    snprintf(buffer, size, "%s/%s", left, right);
}

static int copy_file(const char *src, const char *dst) {
    FILE *in = fopen(src, "rb");
    if (!in) return 1;
    FILE *out = fopen(dst, "wb");
    if (!out) {
        fclose(in);
        return 1;
    }
    char buf[4096];
    size_t n = 0;
    while ((n = fread(buf, 1, sizeof(buf), in)) > 0) {
        fwrite(buf, 1, n, out);
    }
    fclose(in);
    fclose(out);
    return 0;
}

static int command_assemble(const char *proof_dir, const char *offer_dir, const char *output_dir) {
    if (ensure_dir(output_dir) != 0) {
        fprintf(stderr, "Failed to create output dir.\n");
        return 1;
    }

    char src[MAX_PATH];
    char dst[MAX_PATH];
    const char *proof_files[] = {"deliverable.md", "transcript.txt", "proof-bundle.json", NULL};
    const char *offer_files[] = {"offer.json", "offer.md", "outreach.md", NULL};

    for (int i = 0; proof_files[i]; i++) {
        path_join(src, sizeof(src), proof_dir, proof_files[i]);
        path_join(dst, sizeof(dst), output_dir, proof_files[i]);
        if (copy_file(src, dst) != 0) {
            fprintf(stderr, "Failed to copy %s\n", proof_files[i]);
            return 1;
        }
    }
    for (int i = 0; offer_files[i]; i++) {
        path_join(src, sizeof(src), offer_dir, offer_files[i]);
        path_join(dst, sizeof(dst), output_dir, offer_files[i]);
        if (copy_file(src, dst) != 0) {
            fprintf(stderr, "Failed to copy %s\n", offer_files[i]);
            return 1;
        }
    }

    char manifest_path[MAX_PATH];
    path_join(manifest_path, sizeof(manifest_path), output_dir, "package-manifest.txt");
    FILE *fp = fopen(manifest_path, "w");
    if (!fp) return 1;
    fprintf(fp,
            "proof_dir=%s\n"
            "offer_dir=%s\n"
            "files=deliverable.md,transcript.txt,proof-bundle.json,offer.json,offer.md,outreach.md\n",
            proof_dir, offer_dir);
    fclose(fp);

    printf("Package: %s\n", output_dir);
    return 0;
}

static int command_archive(const char *proof_dir, const char *offer_dir, const char *output_tar) {
    /* Assemble into a temp dir, then tar.gz it */
    char tmpdir[MAX_PATH];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/bonfyre-pack-%d", (int)getpid());
    if (ensure_dir(tmpdir) != 0) {
        fprintf(stderr, "Failed to create temp dir: %s\n", tmpdir);
        return 1;
    }

    int rc = command_assemble(proof_dir, offer_dir, tmpdir);
    if (rc != 0) return rc;

    /* Use tar to create archive */
    pid_t pid;
    posix_spawnattr_t attr;
    posix_spawnattr_init(&attr);

    char *argv[] = {
        "tar", "-czf", (char *)output_tar,
        "-C", "/tmp",
        NULL, /* basename */
        NULL
    };
    char basename[256];
    snprintf(basename, sizeof(basename), "bonfyre-pack-%d", (int)getpid());
    argv[5] = basename;

    rc = posix_spawnp(&pid, "tar", NULL, &attr, argv, environ);
    posix_spawnattr_destroy(&attr);
    if (rc != 0) {
        fprintf(stderr, "Failed to spawn tar: %s\n", strerror(rc));
        return 1;
    }
    int status = 0;
    waitpid(pid, &status, 0);

    /* Cleanup temp dir */
    char rm_cmd[MAX_PATH];
    snprintf(rm_cmd, sizeof(rm_cmd), "/tmp/bonfyre-pack-%d", (int)getpid());
    /* Remove temp files individually */
    const char *all_files[] = {"deliverable.md", "transcript.txt", "proof-bundle.json",
                                "offer.json", "offer.md", "outreach.md", "package-manifest.txt", NULL};
    for (int i = 0; all_files[i]; i++) {
        char f[MAX_PATH];
        path_join(f, sizeof(f), tmpdir, all_files[i]);
        remove(f);
    }
    rmdir(tmpdir);

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        printf("Archive: %s\n", output_tar);
        return 0;
    }
    fprintf(stderr, "tar failed with exit %d\n", WEXITSTATUS(status));
    return 1;
}

int main(int argc, char **argv) {
    if (argc == 5 && strcmp(argv[1], "assemble") == 0) {
        return command_assemble(argv[2], argv[3], argv[4]);
    }
    if (argc == 5 && strcmp(argv[1], "archive") == 0) {
        return command_archive(argv[2], argv[3], argv[4]);
    }
    fprintf(stderr,
            "Usage:\n"
            "  bonfyre-pack assemble <proof-dir> <offer-dir> <output-dir>\n"
            "  bonfyre-pack archive  <proof-dir> <offer-dir> <output.tar.gz>\n");
    return 1;
}
