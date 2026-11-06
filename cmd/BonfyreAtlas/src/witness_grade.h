/* Witness strength is derived from the existing Atlas and filesystem.
 * Shell discovery is retained as the historical "exercised" tier, never
 * promoted into a claim that a suite actually ran. */
static void witness_suites(const char *relative, int recursive, bf_jv *suites) {
    char *path = fmt("%s/%s", root, relative);
    DIR *d = opendir(path);
    if (!d) { free(path); return; }
    for (struct dirent *e; (e = readdir(d));) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char *rel = fmt("%s/%s", relative, e->d_name), *full = fmt("%s/%s", root, rel);
        struct stat st;
        if (!lstat(full, &st)) {
            if (S_ISDIR(st.st_mode) && recursive) witness_suites(rel, 1, suites);
            else if (S_ISREG(st.st_mode) && ends_with(e->d_name, ".sh")) bf_jv_set(suites, rel, bf_jv_bool(1));
        }
        free(full); free(rel);
    }
    closedir(d); free(path);
}

static bf_jv *witness_labels(const char *mutation) {
    bf_jv *labels = bf_jv_obj();
    if (!mutation) return labels;
    size_t len = 0; char *text = slurp(mutation, &len), error[128];
    bf_jv *receipt = text ? bf_jv_parse(text, len, error, sizeof error) : NULL;
    free(text);
    const bf_jv *guarded = bf_jv_get(receipt, "guarded");
    int valid = !guarded || guarded->type == BF_JV_NULL || guarded->type == BF_JV_ARR;
    for (size_t i = 0; valid && guarded && guarded->type == BF_JV_ARR && i < guarded->len; i++) {
        const bf_jv *entry = guarded->items[i], *noticed = bf_jv_get(entry, "noticed_by");
        valid = entry->type != BF_JV_NULL && (!noticed || noticed->type == BF_JV_NULL || noticed->type == BF_JV_ARR);
        for (size_t j = 0; valid && noticed && noticed->type == BF_JV_ARR && j < noticed->len; j++) {
            const char *label = bf_jv_s(noticed->items[j]);
            if (label) bf_jv_set(labels, label, bf_jv_bool(1));
        }
    }
    bf_jv_free(receipt);
    if (!valid) { bf_jv_free(labels); labels = bf_jv_obj(); }
    return labels;
}

static int cmd_witness_grade(const Atlas *a, int argc, char **argv) {
    const char *mutation = NULL;
    for (int i = 0; i < argc; i++) {
        if (!strcmp(argv[i], "--mutation") && i + 1 < argc) mutation = argv[++i];
        else { fputs("usage: bonfyre-atlas witness-grade [--mutation receipt.json]\n", stderr); return 2; }
    }
    bf_jv *suites = bf_jv_obj(), *labels = witness_labels(mutation), *graded = bf_jv_obj();
    char *commands = fmt("%s/cmd", root); DIR *d = opendir(commands);
    if (!d) { fprintf(stderr, "witness-grade: cannot read %s: %s\n", commands, strerror(errno)); free(commands); return 1; }
    for (struct dirent *e; (e = readdir(d));) {
        if (!strcmp(e->d_name, ".") || !strcmp(e->d_name, "..")) continue;
        char *path = fmt("%s/%s", commands, e->d_name); struct stat st;
        if (!lstat(path, &st) && S_ISDIR(st.st_mode)) {
            char *tests = fmt("cmd/%s/tests", e->d_name); witness_suites(tests, 0, suites); free(tests);
        }
        free(path);
    }
    closedir(d); free(commands);
    witness_suites("tests", 1, suites);
    for (size_t i = 0; i < suites->len; i++) {
        const char *base = strrchr(suites->keys[i], '/') + 1;
        char *stem = strndup(base, strlen(base) - 3);
        int match = bf_jv_has(labels, stem);
        if (!match && (ends_with(stem, "_test") || ends_with(stem, "_e2e"))) {
            stem[strlen(stem) - (ends_with(stem, "_test") ? 5 : 4)] = 0;
            match = bf_jv_has(labels, stem);
        }
        if (match) bf_jv_set(graded, suites->keys[i], bf_jv_bool(1));
        free(stem);
    }
    int counts[4] = {0}, total = 0;
    Strs ids = {0}, paths = {0};
    for (int i = 0; i < a->arch.n; i++) push(&ids, a->arch.b[i]->ident);
    qsort(ids.v, (size_t)ids.n, sizeof *ids.v, cmp_locale);
    Strs graded_ids = {0};
    for (int i = 0; i < ids.n; i++) {
        const Block *b = find(&a->arch, ids.v[i]);
        const char *maturity = field(b, "maturity", "");
        if (strcmp(maturity, "measured") && strcmp(maturity, "proven")) continue;
        const bf_jv *witnesses = list(b, "witness"); int tier = 0; char *first = NULL;
        for (size_t j = 0; j < witnesses->len; j++) {
            const char *text = witnesses->items[j]->s; size_t n = 0;
            while (text[n] && !js_ws((unsigned char)text[n])) n++;
            char *path = strndup(text, n), *full = fmt("%s/%s", root, path);
            if (strchr(path, '/') && !ends_with(path, ".archive") && exists(full)) {
                if (!first) first = strdup(path);
                int strength = bf_jv_has(graded, path) ? 3 : bf_jv_has(suites, path) ? 2 : 1;
                if (strength > tier) tier = strength;
            }
            free(full); free(path);
        }
        counts[tier]++; total++;
        if (tier == 3) { push(&graded_ids, ids.v[i]); push(&paths, first); } else free(first);
    }
    static const char *names[] = {"named", "runnable", "exercised", "graded"};
    static const char *notes[] = {"a witness is described in prose only", "a file exists; nothing runs it here",
        "a shell suite was discovered; execution is not evidenced", "a fact it reads was changed and it went red"};
    printf("%d architectures claim measured or proven\n\n", total);
    for (int i = 3; i >= 0; i--) printf("  %-10s %3d   %s\n", names[i], counts[i], notes[i]);
    putchar('\n');
    if (graded_ids.n) {
        puts("  graded:");
        for (int i = 0; i < graded_ids.n; i++) printf("    %-34s %s\n", graded_ids.v[i], paths.v[i]);
    }
    puts("\n  cannot infer: a graded witness -> a correct architecture; mutation shows the witness reacts to the state it reads");
    if (!mutation) puts("  (no --mutation receipt given, so nothing could be graded)");
    for (int i = 0; i < paths.n; i++) free(paths.v[i]);
    free(ids.v); free(paths.v); free(graded_ids.v);
    bf_jv_free(suites); bf_jv_free(labels); bf_jv_free(graded);
    return 0;
}
