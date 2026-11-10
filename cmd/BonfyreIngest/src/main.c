/* POSIX/GNU interfaces used below are hidden by glibc under -std=c11 without
   a feature macro. macOS exposes them regardless, so this class of break is
   invisible on the dev machine and fatal on the Linux deploy host. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

/*
 * BonfyreIngest — universal intake binary.
 *
 * Owns the door. Every asset that enters the system goes through here.
 * Routes by type, normalizes, stamps with intake manifest, feeds pipeline.
 *
 * Usage:
 *   bonfyre-ingest <input_path> <output_dir> [--type audio|text|image|url]
 *
 * Produces:
 *   <output_dir>/intake-manifest.json   — typed intake record
 *   <output_dir>/normalized.*           — normalized input file
 *   <output_dir>/artifact.json          — intake-stage artifact manifest
 *   <output_dir>/fabric-output.manifest — governed Fabric discovery list
 */
#include <ctype.h>
#include "../../../lib/libbonfyre/include/bf_help.h"
#include "../../../lib/libbonfyre/include/bf_command_lookup.h"
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <bonfyre.h>
#include <bf_cloud_refs.h>

#define MAX_LINE 8192
#define VERSION "1.0.0"

/* ---------- utilities (shared pattern) ---------- */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }
static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

static unsigned long file_size(const char *path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (unsigned long)st.st_size;
}

static const char *extension(const char *path) {
    const char *dot = strrchr(path, '.');
    return dot ? dot + 1 : "";
}

static const char *basename_of(const char *path) {
    const char *s = strrchr(path, '/');
    return s ? s + 1 : path;
}

/* ---------- type detection ---------- */

typedef enum { TYPE_AUDIO, TYPE_TEXT, TYPE_IMAGE, TYPE_TABLE, TYPE_DOCUMENT, TYPE_URL, TYPE_UNKNOWN } IngestType;

static IngestType detect_type(const char *path, const char *hint) {
    if (hint) {
        if (strcmp(hint, "audio") == 0) return TYPE_AUDIO;
        if (strcmp(hint, "text")  == 0) return TYPE_TEXT;
        if (strcmp(hint, "image") == 0) return TYPE_IMAGE;
        if (strcmp(hint, "table") == 0) return TYPE_TABLE;
        if (strcmp(hint, "document") == 0) return TYPE_DOCUMENT;
        if (strcmp(hint, "url")   == 0) return TYPE_URL;
    }
    /* detect URLs */
    if (strncmp(path, "http://", 7) == 0 || strncmp(path, "https://", 8) == 0)
        return TYPE_URL;
    const char *ext = extension(path);
    if (strcasecmp(ext, "wav") == 0 || strcasecmp(ext, "mp3") == 0 ||
        strcasecmp(ext, "flac") == 0 || strcasecmp(ext, "m4a") == 0 ||
        strcasecmp(ext, "ogg") == 0 || strcasecmp(ext, "opus") == 0 ||
        strcasecmp(ext, "aiff") == 0 || strcasecmp(ext, "aif") == 0 ||
        strcasecmp(ext, "aac") == 0 || strcasecmp(ext, "caf") == 0)
        return TYPE_AUDIO;
    if (strcasecmp(ext, "csv") == 0) return TYPE_TABLE;
    if (strcasecmp(ext, "txt") == 0 || strcasecmp(ext, "md") == 0 ||
        strcasecmp(ext, "vtt") == 0 || strcasecmp(ext, "srt") == 0 ||
        strcasecmp(ext, "json") == 0)
        return TYPE_TEXT;
    if (strcasecmp(ext, "png") == 0 || strcasecmp(ext, "jpg") == 0 ||
        strcasecmp(ext, "jpeg") == 0 || strcasecmp(ext, "tiff") == 0 ||
        strcasecmp(ext, "bmp") == 0)
        return TYPE_IMAGE;
    if (strcasecmp(ext, "pdf") == 0) return TYPE_DOCUMENT;
    if (strcasecmp(ext, "html") == 0 || strcasecmp(ext, "htm") == 0) return TYPE_DOCUMENT;
    return TYPE_UNKNOWN;
}

static const char *type_str(IngestType t) {
    switch (t) {
        case TYPE_AUDIO: return "audio";
        case TYPE_TEXT:  return "text";
        case TYPE_IMAGE: return "image";
        case TYPE_TABLE: return "table";
        case TYPE_DOCUMENT: return "document";
        case TYPE_URL:   return "url";
        default:         return "unknown";
    }
}

static const char *media_type(IngestType t, const char *ext) {
    switch (t) {
        case TYPE_AUDIO:
            if (strcasecmp(ext, "wav") == 0)  return "audio/wav";
            if (strcasecmp(ext, "mp3") == 0)  return "audio/mpeg";
            if (strcasecmp(ext, "flac") == 0) return "audio/flac";
            if (strcasecmp(ext, "m4a") == 0)  return "audio/mp4";
            if (strcasecmp(ext, "aiff") == 0 || strcasecmp(ext, "aif") == 0) return "audio/aiff";
            if (strcasecmp(ext, "aac") == 0)  return "audio/aac";
            if (strcasecmp(ext, "ogg") == 0 || strcasecmp(ext, "opus") == 0) return "audio/ogg";
            return "audio/unknown";
        case TYPE_TEXT:
            if (strcasecmp(ext, "md") == 0)   return "text/markdown";
            if (strcasecmp(ext, "json") == 0) return "application/json";
            if (strcasecmp(ext, "vtt") == 0)  return "text/vtt";
            return "text/plain";
        case TYPE_IMAGE:
            if (strcasecmp(ext, "png") == 0)  return "image/png";
            if (strcasecmp(ext, "jpg") == 0 || strcasecmp(ext, "jpeg") == 0) return "image/jpeg";
            if (strcasecmp(ext, "pdf") == 0)  return "application/pdf";
            return "image/unknown";
        case TYPE_TABLE: return "text/csv";
        case TYPE_DOCUMENT: return "application/pdf";
        default: return "application/octet-stream";
    }
}

/* A filename is a locator, not evidence of an asset's encoding.  Preserve
 * source and normalized media types separately so a misnamed source cannot be
 * relabelled on the way into the evidence graph. */
static const char *source_media_type(IngestType t, const char *path, const char *ext) {
    unsigned char signature[12] = {0};
    FILE *input = fopen(path, "rb");
    size_t size = input ? fread(signature, 1, sizeof(signature), input) : 0;
    if (input) fclose(input);

    if (t == TYPE_IMAGE) {
        static const unsigned char png[] = {0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};
        if (size >= sizeof(png) && memcmp(signature, png, sizeof(png)) == 0) return "image/png";
        if (size >= 3 && signature[0] == 0xff && signature[1] == 0xd8 && signature[2] == 0xff) return "image/jpeg";
        if (size >= 6 && (!memcmp(signature, "GIF87a", 6) || !memcmp(signature, "GIF89a", 6))) return "image/gif";
        if (size >= 4 && (!memcmp(signature, "II*\0", 4) || !memcmp(signature, "MM\0*", 4))) return "image/tiff";
        if (size >= 2 && signature[0] == 'B' && signature[1] == 'M') return "image/bmp";
    }
    if (t == TYPE_DOCUMENT && size >= 4 && memcmp(signature, "%PDF", 4) == 0) return "application/pdf";
    return media_type(t, ext);
}

/* Which reader a "document" gets.
 *
 * --type document used to mean PDF unless the filename ended in .html. A
 * retained ForeignTwin body is named NAME.body, so every observed page or
 * README sent through as a document went to pdftotext and failed. The reader
 * is now chosen from the bytes first (a %PDF signature is a PDF whatever it is
 * called), then the media type the source declared (--mime, e.g. the receipt's
 * http_evidence.content_type), then the extension, then a sniff of the head.
 * Choosing a reader is not parsing: HTML and text documents are normalized to
 * readable text only; headings, links and structure are not extracted here. */
typedef enum { DOC_PDF, DOC_HTML, DOC_TEXT } DocKind;

static int mime_has(const char *mime, const char *want) {
    size_t n = strlen(want);
    if (!mime) return 0;
    while (*mime == ' ' || *mime == '\t') mime++;
    return strncasecmp(mime, want, n) == 0 &&
           (mime[n] == '\0' || mime[n] == ';' || mime[n] == ' ');
}

static DocKind document_kind(const char *path, const char *ext, const char *mime) {
    unsigned char head[4096];
    size_t got = 0, i = 0;
    FILE *f = fopen(path, "rb");
    if (f) { got = fread(head, 1, sizeof(head), f); fclose(f); }

    if (got >= 4 && memcmp(head, "%PDF", 4) == 0) return DOC_PDF;
    if (mime_has(mime, "application/pdf")) return DOC_PDF;
    if (mime_has(mime, "text/html") || mime_has(mime, "application/xhtml+xml")) return DOC_HTML;
    if (mime && strncasecmp(mime, "text/", 5) == 0) return DOC_TEXT;
    if (strcasecmp(ext, "pdf") == 0) return DOC_PDF;
    if (strcasecmp(ext, "html") == 0 || strcasecmp(ext, "htm") == 0) return DOC_HTML;

    if (got >= 3 && head[0] == 0xEF && head[1] == 0xBB && head[2] == 0xBF) i = 3;
    while (i < got && (head[i] == ' ' || head[i] == '\n' || head[i] == '\r' || head[i] == '\t')) i++;
    if (got - i >= 14 && !strncasecmp((const char *)head + i, "<!doctype html", 14)) return DOC_HTML;
    if (got - i >= 5 && !strncasecmp((const char *)head + i, "<html", 5)) return DOC_HTML;
    if (got == 0 || memchr(head, '\0', got)) return DOC_PDF;  /* binary: the PDF reader refuses honestly */
    return DOC_TEXT;
}

static const char *document_source_media(DocKind kind, const char *ext, const char *mime) {
    if (kind == DOC_PDF) return "application/pdf";
    if (kind == DOC_HTML) return "text/html";
    /* A fixed vocabulary: the caller's string is never echoed into JSON. */
    if (mime_has(mime, "text/markdown") || strcasecmp(ext, "md") == 0) return "text/markdown";
    if (mime_has(mime, "text/x-rst") || strcasecmp(ext, "rst") == 0) return "text/x-rst";
    return "text/plain";
}

static const char *normalized_media_type(IngestType t, const char *normalized_path, const char *source_ext) {
    switch (t) {
        case TYPE_AUDIO: return "audio/wav";
        case TYPE_TEXT: return "text/plain";
        case TYPE_IMAGE: return media_type(TYPE_IMAGE, extension(normalized_path));
        case TYPE_TABLE: return "text/csv";
        case TYPE_DOCUMENT: return "text/plain";
        case TYPE_URL: return strcmp(extension(normalized_path), "txt") == 0 ? "text/plain" : "audio/wav";
        default: return media_type(t, source_ext);
    }
}

/* ---------- normalizers (fork/exec external tools) ---------- */

static int run_cmd(const char *const argv[]) {
    pid_t pid = fork();
    if (pid < 0) return -1;
    if (pid == 0) {
        execvp(argv[0], (char *const *)argv);
        _exit(127);
    }
    int status;
    waitpid(pid, &status, 0);
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

static int normalize_audio(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    snprintf(out_path, out_sz, "%s/normalized.wav", outdir);
    const char *argv[] = {
        "ffmpeg", "-y", "-i", input,
        "-ac", "1", "-ar", "16000", "-sample_fmt", "s16",
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
        out_path, NULL
    };
    return run_cmd(argv);
}

static int normalize_text(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    snprintf(out_path, out_sz, "%s/normalized.txt", outdir);
    /* Strip BOM, normalize newlines, trim trailing whitespace */
    FILE *in = fopen(input, "rb");
    if (!in) return 1;
    FILE *out = fopen(out_path, "wb");
    if (!out) { fclose(in); return 1; }
    char line[MAX_LINE];
    while (fgets(line, sizeof(line), in)) {
        /* skip BOM */
        char *p = line;
        if ((unsigned char)p[0] == 0xEF && (unsigned char)p[1] == 0xBB && (unsigned char)p[2] == 0xBF)
            p += 3;
        /* trim trailing whitespace.  The newline fgets keeps has to be part of
         * what is trimmed: leaving it in and then printing "%s\n" doubled
         * every line ending, so "normalized" text grew a blank line per line.
         * A chunk cut by the buffer (no newline, not at end of file) is the
         * middle of one long line and is written as it came. */
        size_t len = strlen(p);
        int line_end = len > 0 && p[len-1] == '\n';
        if (!line_end && !feof(in)) { fputs(p, out); continue; }
        while (len > 0 && (p[len-1] == '\n' || p[len-1] == '\r' || p[len-1] == ' ' || p[len-1] == '\t'))
            len--;
        p[len] = '\0';
        fprintf(out, "%s\n", p);
    }
    fclose(in);
    fclose(out);
    return 0;
}

static int normalize_table(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    FILE *in = fopen(input, "rb");
    if (!in) return 1;
    snprintf(out_path, out_sz, "%s/normalized.csv", outdir);
    FILE *out = fopen(out_path, "wb");
    if (!out) { fclose(in); return 1; }
    char line[MAX_LINE]; int rows = 0;
    while (fgets(line,sizeof(line),in)) {
        size_t length=strlen(line);
        while(length && (line[length-1]=='\n'||line[length-1]=='\r'||line[length-1]==' '||line[length-1]=='\t')) line[--length]='\0';
        if (!length) continue;
        if (!strchr(line,',')) { fclose(in); fclose(out); unlink(out_path); return 1; }
        fprintf(out,"%s\n",line); ++rows;
    }
    fclose(in); fclose(out);
    if (rows < 2) { unlink(out_path); return 1; }
    return 0;
}

static int normalize_pdf_text(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    snprintf(out_path,out_sz,"%s/extracted.txt",outdir);

    /* Prefer a real PDF text extractor when present.  The normalized document
     * contract is UTF-8 text/plain, so a successful pdftotext result composes
     * directly with Query/Index/Embed rather than leaking PDF string bytes. */
    const char *pdf_argv[] = { "pdftotext", "-enc", "UTF-8", "-layout", input, out_path, NULL };
    if (run_cmd(pdf_argv) == 0) {
        struct stat st;
        if (stat(out_path, &st) == 0 && st.st_size > 0) goto extracted_text_ready;
        unlink(out_path);
    }

    /* Dependency-free fallback: retain only printable ASCII from literal PDF
     * strings.  Escaped bytes are filtered too; emitting arbitrary escaped
     * bytes while labelling the artifact text/plain breaks the ingest law. */
    FILE *in=fopen(input,"rb");
    if(!in) return 1;
    char header[5]={0};
    if(fread(header,1,4,in)!=4 || memcmp(header,"%PDF",4)){fclose(in);return 1;}
    rewind(in);
    FILE *out=fopen(out_path,"wb"); if(!out){fclose(in);return 1;}
    int c, written=0, in_string=0, escape=0;
    while((c=fgetc(in))!=EOF) {
        if(in_string) { if(escape){ if(c>=0x20&&c<0x7f){fputc(c,out);written++;} escape=0; } else if(c=='\\')escape=1; else if(c==')'){fputc('\n',out);in_string=0;} else if(c>=0x20&&c<0x7f){fputc(c,out);written++;} }
        else if(c=='(') in_string=1;
    }
    fclose(in); fclose(out);
    if(!written){unlink(out_path);return 1;}

extracted_text_ready: ;
    /* PDF text streams often contain literal CSV-like rows.  Preserve those
     * rows as a typed table sibling rather than flattening table evidence
     * into the extracted prose artifact. */
    FILE *text = fopen(out_path, "rb");
    char table_path[PATH_MAX];
    snprintf(table_path, sizeof(table_path), "%s/extracted-table.csv", outdir);
    FILE *table = text ? fopen(table_path, "wb") : NULL;
    int table_rows = 0;
    char line[MAX_LINE];
    if (text && table) {
        while (fgets(line, sizeof(line), text)) {
            if (strchr(line, ',')) { fputs(line, table); ++table_rows; }
        }
    }
    if (text) fclose(text);
    if (table) fclose(table);
    if (table_rows < 2) unlink(table_path);
    return 0;
}

static int normalize_image(const char *input, const char *outdir, char *out_path, size_t out_sz) {
    const char *source_media = source_media_type(TYPE_IMAGE, input, extension(input));
    const char *output_ext = strcmp(source_media, "image/jpeg") == 0 ? "jpg" : "png";
    snprintf(out_path, out_sz, "%s/normalized.%s", outdir, output_ext);
    const char *argv[] = { "magick", input, "-strip", "-resize", "2048x2048>", out_path, NULL };
    return run_cmd(argv);
}

/* ---------- yt-dlp URL intake ---------- */

static const char *resolve_ytdlp(void) {
    const char *env = getenv("BONFYRE_YTDLP");
    if (env && env[0]) return env;
    /* common fallback paths */
    static const char *paths[] = {
        /* macOS Homebrew */
        "/opt/homebrew/bin/yt-dlp",
        /* Linux / universal */
        "/usr/bin/yt-dlp",
        "/usr/local/bin/yt-dlp",
        NULL
    };
    for (int i = 0; paths[i]; i++) {
        if (access(paths[i], X_OK) == 0) return paths[i];
    }
    return "yt-dlp"; /* fall back to PATH lookup via execvp */
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

/* ---------- web page intake ---------- */

static const char *find_nocase(const char *hay, const char *needle) {
    size_t n = strlen(needle);
    for (; *hay; hay++) if (strncasecmp(hay, needle, n) == 0) return hay;
    return NULL;
}

static int tag_is(const char *tag, size_t len, const char *name) {
    size_t n = strlen(name);
    return len >= n && strncasecmp(tag, name, n) == 0 &&
           (len == n || tag[n] == ' ' || tag[n] == '/' || tag[n] == '\t' || tag[n] == '\n');
}

/* Readable text of an HTML document: scripts and styles dropped, block tags
 * become line breaks, h1-h6 become "#" heading lines so Paragraph keeps the
 * page's own structure.  Returns the number of non-space bytes written. */
static long html_to_text(const char *html, FILE *out) {
    static const char *blocks[] = {"p","div","br","li","tr","section","article","header","footer",
                                   "table","ul","ol","blockquote","pre","title",NULL};
    static const struct { const char *name; const char *text; } entities[] = {
        {"amp;","&"},{"lt;","<"},{"gt;",">"},{"quot;","\""},{"#39;","'"},{"apos;","'"},{"nbsp;"," "},{NULL,NULL}};
    long written = 0;
    int at_line_start = 1, blank_run = 2;
    for (const char *p = html; *p; ) {
        if (*p == '<') {
            const char *end = strchr(p, '>');
            const char *tag = p + 1;
            int closing = 0;
            size_t len;
            if (!strncmp(p, "<!--", 4)) { end = strstr(p, "-->"); if (!end) break; p = end + 3; continue; }
            if (!end) break;
            if (*tag == '/') { closing = 1; tag++; }
            len = (size_t)(end - tag);
            if (!closing && (tag_is(tag, len, "script") || tag_is(tag, len, "style") ||
                             tag_is(tag, len, "noscript") || tag_is(tag, len, "svg"))) {
                char close_tag[16];
                const char *stop;
                size_t n = 0;
                while (n < len && n < 10 && isalpha((unsigned char)tag[n])) n++;
                snprintf(close_tag, sizeof(close_tag), "</%.*s", (int)n, tag);
                stop = find_nocase(end, close_tag);
                if (!stop) break;
                end = strchr(stop, '>');
                if (!end) break;
                p = end + 1;
                continue;
            }
            if (len >= 2 && (tag[0] == 'h' || tag[0] == 'H') && tag[1] >= '1' && tag[1] <= '6' &&
                (len == 2 || isspace((unsigned char)tag[2]))) {
                if (!at_line_start) fputc('\n', out);
                if (blank_run < 1) fputc('\n', out);
                if (!closing) fputs("# ", out);
                at_line_start = closing; blank_run = closing ? 1 : 2;
                if (closing) fputc('\n', out);
            } else {
                for (int i = 0; blocks[i]; i++)
                    if (tag_is(tag, len, blocks[i])) {
                        if (!at_line_start) { fputc('\n', out); at_line_start = 1; blank_run = 0; }
                        if (blank_run < 1) { fputc('\n', out); blank_run = 1; }
                        break;
                    }
            }
            p = end + 1;
            continue;
        }
        if (*p == '&') {
            int matched = 0;
            for (int i = 0; entities[i].name; i++) {
                size_t n = strlen(entities[i].name);
                if (!strncmp(p + 1, entities[i].name, n)) {
                    fputs(entities[i].text, out);
                    if (entities[i].text[0] != ' ') { written++; at_line_start = 0; blank_run = 0; }
                    p += n + 1; matched = 1; break;
                }
            }
            if (matched) continue;
        }
        if (isspace((unsigned char)*p)) {
            if (!at_line_start) fputc(' ', out);
            while (isspace((unsigned char)*p)) p++;
            continue;
        }
        fputc(*p++, out);
        written++; at_line_start = 0; blank_run = 0;
    }
    fputc('\n', out);
    return written;
}

/* A page that exists only after its script runs needs a browser.  Cloudflare
 * Browser Rendering is one the estate has an account for: given the account id
 * and token the estate holds for it (bf_cloud_refs.h) the page is asked for as
 * that browser built it.  The token travels in a header file, never on a
 * command line, and the answer is kept only as the page it carried. */
static char *read_whole(const char *path, long *size);

static int render_page(const char *url, const char *outdir, char *rendered, size_t cap) {
    char account[128], token[512];
    char header_path[PATH_MAX], body_path[PATH_MAX], answer_path[PATH_MAX], endpoint[512];
    char header_arg[PATH_MAX + 2], body_arg[PATH_MAX + 2];
    char *answer, *html;
    long size = 0;
    FILE *f;
    int rc, fd;
    if (!bf_cloud_ref("cloudflare", "account_id", account, sizeof(account)) ||
        !bf_cloud_ref("cloudflare", "api_token", token, sizeof(token))) return 1;
    for (const char *c = url; *c; c++)
        if (*c == '"' || *c == '\\' || (unsigned char)*c < 0x20) return 1;
    for (const char *c = account; *c; c++)
        if (!isalnum((unsigned char)*c)) return 1;
    snprintf(header_path, sizeof(header_path), "%s/.render-header.XXXXXX", outdir);
    fd = mkstemp(header_path);
    if (fd < 0) return 1;
    dprintf(fd, "authorization: Bearer %s\n", token);
    close(fd);
    memset(token, 0, sizeof(token));
    snprintf(body_path, sizeof(body_path), "%s/.render-request.json", outdir);
    f = fopen(body_path, "w");
    if (!f) { unlink(header_path); return 1; }
    fprintf(f, "{\"url\":\"%s\",\"gotoOptions\":{\"waitUntil\":\"networkidle0\",\"timeout\":45000}}", url);
    fclose(f);
    snprintf(endpoint, sizeof(endpoint),
             "https://api.cloudflare.com/client/v4/accounts/%s/browser-rendering/content", account);
    snprintf(answer_path, sizeof(answer_path), "%s/.render-answer.json", outdir);
    snprintf(header_arg, sizeof(header_arg), "@%s", header_path);
    snprintf(body_arg, sizeof(body_arg), "@%s", body_path);
    const char *argv[] = {
        "curl", "-sS", "--fail", "--max-time", "120", "--retry", "3", "--retry-delay", "12",
        "-X", "POST", "-H", header_arg, "-H", "content-type: application/json",
        "--data-binary", body_arg, "-o", answer_path, endpoint, NULL
    };
    fprintf(stderr, "[ingest] asking Cloudflare Browser Rendering to build the page\n");
    rc = run_cmd(argv);
    unlink(header_path);
    unlink(body_path);
    if (rc != 0) { fprintf(stderr, "[ingest] browser rendering failed (curl rc=%d)\n", rc); unlink(answer_path); return 1; }
    answer = read_whole(answer_path, &size);
    unlink(answer_path);
    if (!answer) return 1;
    html = malloc((size_t)size + 1);
    if (!html || !strstr(answer, "\"success\":true") || !bf_json_str(answer, "result", html, (size_t)size + 1)) {
        fprintf(stderr, "[ingest] browser rendering returned no page\n");
        free(answer); free(html);
        return 1;
    }
    free(answer);
    snprintf(rendered, cap, "%s/rendered.html", outdir);
    f = fopen(rendered, "wb");
    if (!f) { free(html); return 1; }
    fputs(html, f);
    fclose(f);
    free(html);
    return 0;
}

static char *read_whole(const char *path, long *size) {
    FILE *in = fopen(path, "rb");
    char *data;
    if (!in) return NULL;
    fseek(in, 0, SEEK_END);
    *size = ftell(in);
    rewind(in);
    data = *size > 0 ? malloc((size_t)*size + 1) : NULL;
    if (!data || fread(data, 1, (size_t)*size, in) != (size_t)*size) { fclose(in); free(data); return NULL; }
    fclose(in);
    data[*size] = '\0';
    for (long i = 0; i < *size; i++) if (!data[i]) data[i] = ' ';
    return data;
}

#include "page_state.inc"

/* A URL that is not media is a document.  The page is kept as fetched and its
 * readable text becomes the normalized artifact.  A page that draws itself
 * from script is read from its embedded state, then through a browser if one
 * is available, and refused only when neither yields text. */
static int ingest_page(const char *url, const char *outdir, char *out_path, size_t out_sz) {
    char page_path[PATH_MAX];
    snprintf(page_path, sizeof(page_path), "%s/page.html", outdir);
    const char *fetch_argv[] = {
        "curl", "-sS", "-L", "--fail", "--max-time", "60", "--max-filesize", "33554432",
        "-A", "Mozilla/5.0 (compatible; bonfyre-ingest)", "-o", page_path, url, NULL
    };
    int rc = run_cmd(fetch_argv);
    if (rc != 0) {
        fprintf(stderr, "[ingest] page fetch failed (rc=%d)\n", rc);
        unlink(page_path);
        return 1;
    }
    FILE *in = fopen(page_path, "rb");
    if (!in) return 1;
    fseek(in, 0, SEEK_END);
    long size = ftell(in);
    rewind(in);
    char *html = size > 0 ? malloc((size_t)size + 1) : NULL;
    if (!html || fread(html, 1, (size_t)size, in) != (size_t)size) { fclose(in); free(html); return 1; }
    fclose(in);
    html[size] = '\0';
    for (long i = 0; i < size; i++) if (!html[i]) html[i] = ' ';
    snprintf(out_path, out_sz, "%s/normalized.txt", outdir);
    FILE *out = fopen(out_path, "wb");
    if (!out) { free(html); return 1; }
    long readable = html_to_text(html, out);
    fclose(out);
    if (readable < 200) {
        /* The words of a script-built page are in the page as data.  Read
         * them where they lie before asking anything to run the script. */
        long passages = 0, from_state;
        fprintf(stderr, "[ingest] page has %ld readable bytes outside script\n", readable);
        out = fopen(out_path, "wb");
        if (!out) { free(html); return 1; }
        from_state = page_state_to_text(html, out, &passages);
        fclose(out);
        if (from_state >= 200) {
            free(html);
            fprintf(stderr, "[ingest] page text: %ld bytes of prose in %ld passages read from the page's embedded state "
                            "(order kept; speakers and headings are not recovered)\n", from_state, passages);
            return 0;
        }
    }
    free(html);
    if (readable < 200) {
        char rendered[PATH_MAX];
        long rendered_size = 0;
        char *built;
        unlink(out_path);
        if (render_page(url, outdir, rendered, sizeof(rendered)) != 0) {
            fprintf(stderr, "[ingest] kept as page.html, not normalized: no browser was available to build it "
                            "(the estate holds no Cloudflare account id and token for it)\n");
            return 1;
        }
        built = read_whole(rendered, &rendered_size);
        out = built ? fopen(out_path, "wb") : NULL;
        if (!out) { free(built); return 1; }
        readable = html_to_text(built, out);
        free(built);
        fclose(out);
        if (readable < 200) {
            fprintf(stderr, "[ingest] rendered page still has only %ld readable bytes\n", readable);
            unlink(out_path);
            return 1;
        }
    }
    fprintf(stderr, "[ingest] page text: %ld readable bytes\n", readable);
    return 0;
}

static int ingest_url(const char *url, const char *outdir, char *out_path, size_t out_sz) {
    const char *ytdlp = resolve_ytdlp();
    fprintf(stderr, "[ingest] Downloading from URL via yt-dlp: %s\n", url);

    /* Step 1: Extract audio as WAV */
    char audio_template[PATH_MAX];
    snprintf(audio_template, sizeof(audio_template), "%s/downloaded.%%(ext)s", outdir);
    snprintf(out_path, out_sz, "%s/downloaded.wav", outdir);

    const char *dl_argv[] = {
        ytdlp,
        "--no-playlist",
        "-x",                       /* extract audio */
        "--audio-format", "wav",    /* convert to WAV */
        "--audio-quality", "0",     /* best quality */
        "-o", audio_template,
        url,
        NULL
    };
    int rc = run_cmd(dl_argv);
    if (rc != 0) {
        fprintf(stderr, "[ingest] no media at this URL (yt-dlp rc=%d); reading it as a page\n", rc);
        return ingest_page(url, outdir, out_path, out_sz);
    }

    /* Step 2: Extract metadata JSON */
    char meta_path[PATH_MAX];
    snprintf(meta_path, sizeof(meta_path), "%s/source-metadata.json", outdir);

    const char *meta_argv[] = {
        ytdlp,
        "--no-playlist",
        "--skip-download",
        "--write-info-json",
        "-o", meta_path,
        url,
        NULL
    };
    /* yt-dlp writes to <output>.info.json */
    run_cmd(meta_argv);  /* non-fatal if metadata fails */

    /* Rename .info.json if yt-dlp appended it */
    char info_path[PATH_MAX];
    snprintf(info_path, sizeof(info_path), "%s.info.json", meta_path);
    if (access(info_path, F_OK) == 0) {
        rename(info_path, meta_path);
    }

    /* Step 3: Extract key metadata fields for the manifest */
    char title_buf[512] = "unknown";
    char duration_buf[64] = "0";
    char uploader_buf[256] = "unknown";

    const char *title_argv[] = {
        ytdlp, "--no-playlist", "--skip-download",
        "--print", "%(title)s", url, NULL
    };
    run_cmd_capture(title_argv, title_buf, sizeof(title_buf));
    title_buf[strcspn(title_buf, "\r\n")] = '\0';

    const char *dur_argv[] = {
        ytdlp, "--no-playlist", "--skip-download",
        "--print", "%(duration)s", url, NULL
    };
    run_cmd_capture(dur_argv, duration_buf, sizeof(duration_buf));
    duration_buf[strcspn(duration_buf, "\r\n")] = '\0';

    const char *up_argv[] = {
        ytdlp, "--no-playlist", "--skip-download",
        "--print", "%(uploader)s", url, NULL
    };
    run_cmd_capture(up_argv, uploader_buf, sizeof(uploader_buf));
    uploader_buf[strcspn(uploader_buf, "\r\n")] = '\0';

    /* Write URL metadata as separate JSON */
    char url_meta_path[PATH_MAX];
    snprintf(url_meta_path, sizeof(url_meta_path), "%s/url-intake.json", outdir);
    FILE *uf = fopen(url_meta_path, "w");
    if (uf) {
        fprintf(uf,
            "{\n"
            "  \"url\": \"%s\",\n"
            "  \"title\": \"%s\",\n"
            "  \"duration_seconds\": %s,\n"
            "  \"uploader\": \"%s\",\n"
            "  \"audio_path\": \"%s\",\n"
            "  \"metadata_path\": \"%s\",\n"
            "  \"backend\": \"yt-dlp\"\n"
            "}\n",
            url, title_buf, duration_buf, uploader_buf,
            basename_of(out_path),
            access(meta_path, F_OK) == 0 ? "source-metadata.json" : "none");
        fclose(uf);
    }

    /* Step 4: Normalize the downloaded audio */
    char norm_path[PATH_MAX];
    snprintf(norm_path, sizeof(norm_path), "%s/normalized.wav", outdir);
    const char *norm_argv[] = {
        "ffmpeg", "-y", "-i", out_path,
        "-ac", "1", "-ar", "16000", "-sample_fmt", "s16",
        "-af", "loudnorm=I=-16:TP=-1.5:LRA=11",
        norm_path, NULL
    };
    if (run_cmd(norm_argv) == 0) {
        snprintf(out_path, out_sz, "%s", norm_path);
    }

    fprintf(stderr, "[ingest] URL intake complete: %s → %s\n", title_buf, basename_of(out_path));
    return 0;
}

/* ---------- SHA-256 (delegates to lib/libbonfyre) ---------- */

static int compute_sha256(const char *path, char *hash_out, size_t hash_sz) {
    if (hash_sz < 65) return 1;
    return bf_sha256_file(path, hash_out) == 0 ? 0 : 1;
}

static int write_fabric_output_manifest(const char *outdir, const char *normalized_path) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/fabric-output.manifest", outdir);
    FILE *output = fopen(path, "w");
    if (!output) return 1;
    /* The first body is the semantic WorkGraph output.  The two manifests are
     * retained as sibling artifacts for source/realization interpretation. */
    fprintf(output, "%s\nartifact.json\nintake-manifest.json\n", basename_of(normalized_path));
    fclose(output);
    return 0;
}

/* ---------- main ---------- */

int main(int argc, char *argv[]) {
    const char *input = NULL;
    const char *outdir = NULL;
    const char *type_hint = NULL;
    const char *source_mime = NULL;

    /* "help" is one of the operator surface's three read verbs, and a capability
     * is only advertised as reachable when its read verb exits 0 with output.
     * Without this, `bonfyre-ingest help` was parsed as an input filename, hit
     * the missing-outdir path, and exited 1 -- so the audio-ingest capability
     * reported absent even though the organ was installed and working. */
    enum { IG_STATUS };
    static const BfCommandEntry ingest_commands[] = {
        {"status", IG_STATUS, 0, NULL},
    };
    const BfCommandEntry *command = argc < 2 ? NULL : bf_command_lookup(ingest_commands,
        sizeof(ingest_commands) / sizeof(ingest_commands[0]), argv[1]);
    if (bf_help_requested(argc, argv) || (command && command->id == IG_STATUS)) {
        printf("bonfyre-ingest \u2014 normalize source media into pipeline artifacts\n\n"
               "Usage:\n"
               "  bonfyre-ingest <input> <output_dir> "
               "[--type audio|text|image|table|document|url] [--mime TYPE]\n\n"
               "Emits a normalized artifact plus a manifest describing what was\n"
               "detected. --type overrides detection rather than adding a guess.\n"
               "A document is read as PDF, HTML or text by its bytes, then --mime\n"
               "(the media type the source declared), then its extension.\n");
        return 0;
    }

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--type") == 0 && i + 1 < argc) {
            type_hint = argv[++i];
        } else if (strcmp(argv[i], "--mime") == 0 && i + 1 < argc) {
            source_mime = argv[++i];
        } else if (!input) {
            input = argv[i];
        } else if (!outdir) {
            outdir = argv[i];
        }
    }
    if (!input || !outdir) {
        fprintf(stderr, "Usage: bonfyre-ingest <input> <output_dir> [--type audio|text|image|table|document|url]\n");
        return 1;
    }

    if (ensure_dir(outdir) != 0) {
        fprintf(stderr, "Cannot create output dir: %s\n", outdir);
        return 1;
    }

    char ts[64];
    iso_timestamp(ts, sizeof(ts));

    IngestType itype = detect_type(input, type_hint);
    const char *ext = extension(input);
    const char *source_media = itype == TYPE_URL ? "text/uri-list" : source_media_type(itype, input, ext);
    DocKind doc_kind = DOC_PDF;
    if (itype == TYPE_DOCUMENT) {
        doc_kind = document_kind(input, ext, source_mime);
        source_media = document_source_media(doc_kind, ext, source_mime);
    }
    unsigned long sz = file_size(input);
    char source_hash[128] = "unknown";
    if (itype != TYPE_URL && compute_sha256(input, source_hash, sizeof(source_hash)) != 0) {
        fprintf(stderr, "[ingest] cannot hash source %s: %s\n", input, strerror(errno));
        return 1;
    }

    fprintf(stderr, "[ingest] type=%s input=%s size=%lu\n", type_str(itype), basename_of(input), sz);

    /* ---- Normalize ---- */
    char norm_path[PATH_MAX] = {0};
    int norm_ok = 1;
    switch (itype) {
        case TYPE_AUDIO:
            norm_ok = normalize_audio(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_TEXT:
            norm_ok = normalize_text(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_IMAGE:
            norm_ok = normalize_image(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_TABLE:
            norm_ok = normalize_table(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_DOCUMENT:
            /* A saved page is a document too; it is read by the same reader
             * as a fetched one. */
            if (doc_kind == DOC_TEXT) {
                norm_ok = normalize_text(input, outdir, norm_path, sizeof(norm_path));
                break;
            }
            if (doc_kind == DOC_HTML) {
                long page_size = 0;
                char *page = read_whole(input, &page_size);
                FILE *text_out;
                snprintf(norm_path, sizeof(norm_path), "%s/normalized.txt", outdir);
                text_out = page ? fopen(norm_path, "wb") : NULL;
                norm_ok = 1;
                if (text_out) { norm_ok = html_to_text(page, text_out) > 0 ? 0 : 1; fclose(text_out); }
                free(page);
                if (norm_ok) unlink(norm_path);
                break;
            }
            norm_ok = normalize_pdf_text(input, outdir, norm_path, sizeof(norm_path));
            break;
        case TYPE_URL:
            norm_ok = ingest_url(input, outdir, norm_path, sizeof(norm_path));
            break;
        default:
            /* copy as-is using C I/O (no fork) */
            snprintf(norm_path, sizeof(norm_path), "%s/%s", outdir, basename_of(input));
            {
                FILE *src = fopen(input, "rb");
                FILE *dst = src ? fopen(norm_path, "wb") : NULL;
                if (src && dst) {
                    char cpbuf[8192]; size_t n;
                    while ((n = fread(cpbuf, 1, sizeof(cpbuf), src)) > 0) fwrite(cpbuf, 1, n, dst);
                    norm_ok = 0;
                } else { norm_ok = 1; }
                if (src) fclose(src);
                if (dst) fclose(dst);
            }
            break;
    }
    if (norm_ok != 0) {
        /* Structured document/table inputs are not safely recoverable as a
         * raw byte copy: callers requested a typed representation. */
        if (itype == TYPE_URL) {
            /* Nothing was fetched or nothing readable came back; a manifest
             * saying "ready" over zero bytes would be a false intake. */
            fprintf(stderr, "[ingest] URL intake produced no artifact\n");
            return 1;
        }
        if (itype == TYPE_TABLE || itype == TYPE_DOCUMENT) {
            fprintf(stderr, "[ingest] normalization rejected typed %s input\n", type_str(itype));
            return 1;
        }
        fprintf(stderr, "[ingest] WARNING: normalization returned %d, continuing with raw copy\n", norm_ok);
        snprintf(norm_path, sizeof(norm_path), "%s/%s", outdir, basename_of(input));
    }

    /* ---- Hash ---- */
    char hash[128] = "unknown";
    if (compute_sha256(norm_path[0] ? norm_path : input, hash, sizeof(hash)) != 0) {
        fprintf(stderr, "[ingest] cannot hash normalized artifact %s: %s\n", norm_path, strerror(errno));
        return 1;
    }
    unsigned long norm_sz = file_size(norm_path);
    const char *normalized_media = normalized_media_type(itype, norm_path, ext);

    /* ---- Intake manifest ---- */
    char manifest_path[PATH_MAX];
    snprintf(manifest_path, sizeof(manifest_path), "%s/intake-manifest.json", outdir);
    FILE *mf = fopen(manifest_path, "w");
    if (!mf) { fprintf(stderr, "Cannot write manifest\n"); return 1; }
    fprintf(mf,
        "{\n"
        "  \"ingest_version\": \"%s\",\n"
        "  \"timestamp\": \"%s\",\n"
        "  \"source_file\": \"%s\",\n"
        "  \"source_bytes\": %lu,\n"
        "  \"detected_type\": \"%s\",\n"
        "  \"normalized_path\": \"%s\",\n"
        "  \"normalized_bytes\": %lu,\n"
        "  \"source_content_hash\": \"%s\",\n"
        "  \"normalized_content_hash\": \"%s\",\n"
        "  \"content_hash\": \"%s\",\n"
        "  \"source_media_type\": \"%s\",\n"
        "  \"normalized_media_type\": \"%s\",\n"
        "  \"media_type\": \"%s\",\n"
        "  \"status\": \"ready\"\n"
        "}\n",
        VERSION, ts, basename_of(input), sz, type_str(itype),
        basename_of(norm_path), norm_sz, source_hash, hash, hash,
        source_media, normalized_media, normalized_media);
    fclose(mf);

    /* ---- artifact.json ---- */
    char art_path[PATH_MAX];
    snprintf(art_path, sizeof(art_path), "%s/artifact.json", outdir);
    FILE *af = fopen(art_path, "w");
    if (!af) { fprintf(stderr, "Cannot write artifact.json\n"); return 1; }
    fprintf(af,
        "{\n"
        "  \"schema_version\": \"1.0.0\",\n"
        "  \"artifact_id\": \"ingest-%s\",\n"
        "  \"artifact_type\": \"ingest\",\n"
        "  \"created_at\": \"%s\",\n"
        "  \"source_system\": \"BonfyreIngest\",\n"
        "  \"atoms\": [\n"
        "    {\n"
        "      \"atom_id\": \"source-raw\",\n"
        "      \"content_hash\": \"%s\",\n"
        "      \"media_type\": \"%s\",\n"
        "      \"path\": \"%s\",\n"
        "      \"byte_size\": %lu,\n"
        "      \"label\": \"Raw intake asset\"\n"
        "    }\n"
        "  ],\n"
        "  \"operators\": [\n"
        "    {\n"
        "      \"operator_id\": \"op-normalize\",\n"
        "      \"op\": \"Normalize\",\n"
        "      \"inputs\": [\"source-raw\"],\n"
        "      \"output\": \"normalized\",\n"
        "      \"params\": { \"type\": \"%s\" },\n"
        "      \"version\": \"%s\",\n"
        "      \"deterministic\": true\n"
        "    }\n"
        "  ],\n"
        "  \"realizations\": [\n"
        "    {\n"
        "      \"realization_id\": \"normalized\",\n"
        "      \"media_type\": \"%s\",\n"
        "      \"path\": \"%s\",\n"
        "      \"content_hash\": \"%s\",\n"
        "      \"byte_size\": %lu,\n"
        "      \"pinned\": true,\n"
        "      \"produced_by\": \"op-normalize\",\n"
        "      \"label\": \"Normalized intake\"\n"
        "    }\n"
        "  ]\n"
        "}\n",
        hash, ts, source_hash, source_media,
        basename_of(input), sz, type_str(itype), VERSION,
        normalized_media, basename_of(norm_path), hash, norm_sz);
    fclose(af);
    if (write_fabric_output_manifest(outdir, norm_path) != 0) {
        fprintf(stderr, "Cannot write Fabric output manifest\n");
        return 1;
    }

    fprintf(stderr, "[ingest] -> %s  hash=%s  bytes=%lu\n", basename_of(norm_path), hash, norm_sz);
    return 0;
}
