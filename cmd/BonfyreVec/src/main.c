/*
 * BonfyreVec — local vector search via sqlite-vec.
 *
 * Single-file retrieval system. No server. No Weaviate.
 * SQLite + vector extension = embedded semantic search.
 *
 * Usage:
 *   bonfyre-vec init <db>                          → create vector table
 *   bonfyre-vec insert <db> <embeddings.json>      → bulk insert vectors
 *   bonfyre-vec search <db> <query-embedding.json> [--top N]  → nearest neighbors
 *   bonfyre-vec count <db>                         → row count
 */
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

/* ── helpers ─────────────────────────────────────────────────── */

static int ensure_dir(const char *path) {
    char tmp[PATH_MAX]; size_t len = strlen(path);
    if (len == 0 || len >= sizeof(tmp)) return 1;
    memcpy(tmp, path, len + 1);
    for (size_t i = 1; i < len; i++) {
        if (tmp[i] == '/') { tmp[i] = '\0'; mkdir(tmp, 0755); tmp[i] = '/'; }
    }
    mkdir(tmp, 0755);
    return 0;
}

static void iso_ts(char *buf, size_t sz) {
    time_t t = time(NULL); struct tm tm; gmtime_r(&t, &tm);
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return -1; }
    if (pid == 0) { execvp(argv[0], (char *const *)argv); _exit(127); }
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static int run_cmd_capture(const char *const argv[], char *out, size_t out_sz) {
    int pipefd[2];
    if (pipe(pipefd) < 0) return -1;
    pid_t pid = fork();
    if (pid < 0) { close(pipefd[0]); close(pipefd[1]); return -1; }
    if (pid == 0) {
        close(pipefd[0]);
        dup2(pipefd[1], STDOUT_FILENO);
        close(pipefd[1]);
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    close(pipefd[1]);
    size_t total = 0;
    ssize_t rd;
    while ((rd = read(pipefd[0], out + total, out_sz - total - 1)) > 0)
        total += (size_t)rd;
    close(pipefd[0]);
    out[total] = '\0';
    int st; waitpid(pid, &st, 0);
    return WIFEXITED(st) ? WEXITSTATUS(st) : -1;
}

static char *read_file_contents(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0) { fclose(f); return NULL; }
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    fclose(f);
    buf[rd] = '\0';
    if (out_len) *out_len = rd;
    return buf;
}

/* ── Python wrapper for sqlite-vec operations ─────────────────── */
/* sqlite-vec is a Python/loadable extension. We generate a small Python
 * script to do the heavy lifting, same pattern as BonfyreTranscribe. */

static int run_python_script(const char *script) {
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/bonfyre_vec_%d.py", getpid());
    FILE *f = fopen(tmp_path, "w");
    if (!f) { perror("fopen script"); return -1; }
    fputs(script, f);
    fclose(f);

    const char *python = getenv("BONFYRE_PYTHON3");
    if (!python) python = "python3";
    const char *argv[] = { python, tmp_path, NULL };
    int rc = run_cmd(argv);
    unlink(tmp_path);
    return rc;
}

static int run_python_capture(const char *script, char *out, size_t out_sz) {
    char tmp_path[PATH_MAX];
    snprintf(tmp_path, sizeof(tmp_path), "/tmp/bonfyre_vec_%d.py", getpid());
    FILE *f = fopen(tmp_path, "w");
    if (!f) { perror("fopen script"); return -1; }
    fputs(script, f);
    fclose(f);

    const char *python = getenv("BONFYRE_PYTHON3");
    if (!python) python = "python3";
    const char *argv[] = { python, tmp_path, NULL };
    int rc = run_cmd_capture(argv, out, out_sz);
    unlink(tmp_path);
    return rc;
}

/* ── commands ───────────────────────────────────────────────── */

static int cmd_init(const char *db_path) {
    fprintf(stderr, "[vec] Initializing vector DB: %s\n", db_path);

    char script[2048];
    snprintf(script, sizeof(script),
        "import sqlite3, sqlite_vec\n"
        "db = sqlite3.connect('%s')\n"
        "db.enable_load_extension(True)\n"
        "sqlite_vec.load(db)\n"
        "db.execute('CREATE TABLE IF NOT EXISTS artifacts('\n"
        "  'id TEXT PRIMARY KEY,'\n"
        "  'source TEXT,'\n"
        "  'type TEXT,'\n"
        "  'text TEXT,'\n"
        "  'metadata TEXT,'\n"
        "  'created_at TEXT'\n"
        ")')\n"
        "db.execute('CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifacts USING vec0('\n"
        "  'id TEXT PRIMARY KEY,'\n"
        "  'embedding float[384]'\n"
        ")')\n"
        "db.commit()\n"
        "db.close()\n"
        "print('OK: %s initialized')\n",
        db_path, db_path);

    return run_python_script(script);
}

static int cmd_insert(const char *db_path, const char *json_path) {
    fprintf(stderr, "[vec] Inserting vectors from %s into %s\n", json_path, db_path);

    /* Expects JSON with format:
     * { "embeddings": [
     *     { "id": "...", "source": "...", "type": "...", "text": "...",
     *       "embedding": [0.1, 0.2, ...] }
     *   ]
     * }
     */
    char script[4096];
    snprintf(script, sizeof(script),
        "import sqlite3, sqlite_vec, json, struct\n"
        "db = sqlite3.connect('%s')\n"
        "db.enable_load_extension(True)\n"
        "sqlite_vec.load(db)\n"
        "with open('%s') as f:\n"
        "    data = json.load(f)\n"
        "items = data.get('embeddings', data if isinstance(data, list) else [])\n"
        "count = 0\n"
        "for item in items:\n"
        "    emb = item.get('embedding', [])\n"
        "    if not emb: continue\n"
        "    eid = item.get('id', str(count))\n"
        "    # insert metadata\n"
        "    db.execute(\n"
        "        'INSERT OR REPLACE INTO artifacts VALUES (?,?,?,?,?,datetime(\"now\"))',\n"
        "        (eid, item.get('source',''), item.get('type',''),\n"
        "         item.get('text',''), json.dumps(item.get('metadata',{})))\n"
        "    )\n"
        "    # insert vector\n"
        "    blob = struct.pack('%%df' %% len(emb), *emb)\n"
        "    db.execute('INSERT OR REPLACE INTO vec_artifacts(id, embedding) VALUES (?,?)',\n"
        "              (eid, blob))\n"
        "    count += 1\n"
        "db.commit()\n"
        "db.close()\n"
        "print(f'{count} vectors inserted')\n",
        db_path, json_path);

    return run_python_script(script);
}

static int cmd_search(const char *db_path, const char *query_json, int top_k) {
    fprintf(stderr, "[vec] Searching %s (top %d)\n", db_path, top_k);

    char script[4096];
    snprintf(script, sizeof(script),
        "import sqlite3, sqlite_vec, json, struct\n"
        "db = sqlite3.connect('%s')\n"
        "db.enable_load_extension(True)\n"
        "sqlite_vec.load(db)\n"
        "with open('%s') as f:\n"
        "    data = json.load(f)\n"
        "emb = data.get('embedding', data if isinstance(data, list) else [])\n"
        "blob = struct.pack('%%df' %% len(emb), *emb)\n"
        "rows = db.execute(\n"
        "    'SELECT v.id, v.distance, a.source, a.type, a.text '\n"
        "    'FROM vec_artifacts v '\n"
        "    'LEFT JOIN artifacts a ON v.id = a.id '\n"
        "    'WHERE v.embedding MATCH ? '\n"
        "    'ORDER BY v.distance LIMIT ?',\n"
        "    (blob, %d)\n"
        ").fetchall()\n"
        "results = []\n"
        "for r in rows:\n"
        "    results.append({'id': r[0], 'distance': r[1], 'source': r[2],\n"
        "                    'type': r[3], 'text': r[4]})\n"
        "print(json.dumps({'results': results, 'query_file': '%s', 'top_k': %d}, indent=2))\n"
        "db.close()\n",
        db_path, query_json, top_k, query_json, top_k);

    return run_python_script(script);
}

static int cmd_count(const char *db_path) {
    char script[1024];
    snprintf(script, sizeof(script),
        "import sqlite3, sqlite_vec\n"
        "db = sqlite3.connect('%s')\n"
        "db.enable_load_extension(True)\n"
        "sqlite_vec.load(db)\n"
        "meta = db.execute('SELECT count(*) FROM artifacts').fetchone()[0]\n"
        "vec = db.execute('SELECT count(*) FROM vec_artifacts').fetchone()[0]\n"
        "print(f'artifacts: {meta}, vectors: {vec}')\n"
        "db.close()\n",
        db_path);

    return run_python_script(script);
}

/* ── main ───────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-vec — local vector search (sqlite-vec)\n\n"
        "Usage:\n"
        "  bonfyre-vec init <db>\n"
        "  bonfyre-vec insert <db> <embeddings.json>\n"
        "  bonfyre-vec search <db> <query.json> [--top N]\n"
        "  bonfyre-vec count <db>\n"
        "  bonfyre-vec status\n");
}

int main(int argc, char **argv) {
    if (argc >= 2 && strcmp(argv[1], "status") == 0) {
        printf("{\"binary\":\"bonfyre-vec\",\"status\":\"ok\",\"version\":\"1.0.0\"}\n");
        return 0;
    }

    if (argc < 3) { print_usage(); return 1; }

    const char *cmd = argv[1];

    if (strcmp(cmd, "init") == 0) {
        return cmd_init(argv[2]);
    } else if (strcmp(cmd, "insert") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        return cmd_insert(argv[2], argv[3]);
    } else if (strcmp(cmd, "search") == 0) {
        if (argc < 4) { print_usage(); return 1; }
        int top_k = 10;
        for (int i = 4; i < argc; i++) {
            if (strcmp(argv[i], "--top") == 0 && i + 1 < argc)
                top_k = atoi(argv[++i]);
        }
        return cmd_search(argv[2], argv[3], top_k);
    } else if (strcmp(cmd, "count") == 0) {
        return cmd_count(argv[2]);
    }

    print_usage();
    return 1;
}
