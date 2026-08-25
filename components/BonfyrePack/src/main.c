#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#define MAX_PATH 2048

static int ensure_dir(const char *path) {
    char tmp[MAX_PATH];
    size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') {
            tmp[i] = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
            tmp[i] = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return 1;
    return 0;
}

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

int main(int argc, char **argv) {
    if (argc == 5 && strcmp(argv[1], "assemble") == 0) {
        return command_assemble(argv[2], argv[3], argv[4]);
    }
    fprintf(stderr, "Usage:\n  bonfyre-pack assemble <proof-dir> <offer-dir> <output-dir>\n");
    return 1;
}
