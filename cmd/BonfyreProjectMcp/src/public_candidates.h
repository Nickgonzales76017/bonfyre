#ifndef BONFYRE_PROJECT_PUBLIC_CANDIDATES_H
#define BONFYRE_PROJECT_PUBLIC_CANDIDATES_H

typedef struct {
    char name[160];
    time_t mtime;
    int accepted;
    int promoted;
} PublicCandidate;

static int public_candidate_cmp(const void *a, const void *b) {
    const PublicCandidate *x = a, *y = b;
    if (x->mtime < y->mtime) return 1;
    if (x->mtime > y->mtime) return -1;
    return strcmp(x->name, y->name);
}

static int cmd_public_candidates(const char *home) {
    char releases[PATH_MAX], path[PATH_MAX], marker[PATH_MAX];
    DIR *dir = NULL;
    struct dirent *entry = NULL;
    PublicCandidate rows[64];
    int count = 0;

    if (snprintf(releases, sizeof releases, "%s/.bonfyre/releases", home) >= (int)sizeof releases) return 1;
    dir = opendir(releases);
    if (!dir) { perror("public candidates"); return 1; }

    while ((entry = readdir(dir)) != NULL) {
        struct stat st;
        if (strncmp(entry->d_name, "activity-public-", 16) || !safe_release_name(entry->d_name)) continue;
        if (snprintf(path, sizeof path, "%s/%s", releases, entry->d_name) >= (int)sizeof path ||
            lstat(path, &st) != 0 || !S_ISDIR(st.st_mode) || S_ISLNK(st.st_mode)) continue;
        if (snprintf(marker, sizeof marker, "%s/stage.mpk", path) >= (int)sizeof marker || !regular_file(marker)) continue;
        if (count >= (int)(sizeof rows / sizeof rows[0])) continue;

        snprintf(rows[count].name, sizeof rows[count].name, "%s", entry->d_name);
        rows[count].mtime = st.st_mtime;
        if (snprintf(marker, sizeof marker, "%s/accepted.mpk", path) >= (int)sizeof marker) { closedir(dir); return 1; }
        rows[count].accepted = regular_file(marker);
        if (snprintf(marker, sizeof marker, "%s/promoted.mpk", path) >= (int)sizeof marker) { closedir(dir); return 1; }
        rows[count].promoted = regular_file(marker);
        count++;
    }
    closedir(dir);

    qsort(rows, (size_t)count, sizeof rows[0], public_candidate_cmp);
    printf("PUBLIC_CANDIDATES %d\n", count);
    for (int i = 0; i < count; i++)
        printf("%s accepted=%s promoted=%s mtime=%lld\n", rows[i].name,
            rows[i].accepted ? "yes" : "no", rows[i].promoted ? "yes" : "no", (long long)rows[i].mtime);
    return 0;
}

#endif
