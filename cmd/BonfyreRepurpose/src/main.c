/*
 * BonfyreRepurpose — Transform brief artifacts into social media formats
 *
 * Layer: Transform (pure — same input, same output)
 * Input: brief directory (brief.md from BonfyreBrief)
 * Output: tweet-thread.md, linkedin.md, carousel.md, youtube-desc.md, newsletter.md
 *
 * Usage:
 *   bonfyre-repurpose tweet-thread <brief-dir> <out-dir>
 *   bonfyre-repurpose linkedin     <brief-dir> <out-dir>
 *   bonfyre-repurpose carousel     <brief-dir> <out-dir>
 *   bonfyre-repurpose youtube-desc <brief-dir> <out-dir>
 *   bonfyre-repurpose newsletter   <brief-dir> <out-dir>
 *   bonfyre-repurpose all          <brief-dir> <out-dir>
 */

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <bonfyre.h>

/* ── Utilities ────────────────────────────────────────────────────── */

static int ensure_dir(const char *path) { return bf_ensure_dir(path); }

static void iso_timestamp(char *buf, size_t sz) { bf_iso_timestamp(buf, sz); }

/* Capitalize first alpha char of a string */
static void cap_first(char *s) {
    while (*s && !isalpha((unsigned char)*s)) s++;
    if (*s) *s = (char)toupper((unsigned char)*s);
}

/* Truncate text to max_len at last word boundary, append "..." if truncated */
static void truncate_at_word(const char *src, char *dst, size_t max_len) {
    size_t slen = strlen(src);
    if (slen <= max_len) {
        strcpy(dst, src);
        return;
    }
    size_t cut = max_len - 3; /* room for "..." */
    while (cut > 0 && src[cut] != ' ') cut--;
    if (cut == 0) cut = max_len - 3;
    memcpy(dst, src, cut);
    dst[cut] = '\0';
    strcat(dst, "...");
}

/* Strip internal IDs, video hashes, and app-slug prefixes from title.
 * e.g. "async-standup — 9IqbMERRIME" -> "Async Standup"
 *      "My Great Video Title" -> preserved as-is */
static void sanitize_title(const char *src, char *dst, size_t dst_sz) {
    /* If title contains " — " or " - " with a hash-looking suffix, strip it */
    const char *sep = strstr(src, " — ");
    if (!sep) sep = strstr(src, " - ");
    /* Check if the part after separator looks like a video ID (alnum, 6-20 chars) */
    int strip_suffix = 0;
    if (sep) {
        const char *after = sep + (strstr(src, " — ") == sep ? 5 : 3);
        size_t alen = strlen(after);
        if (alen >= 6 && alen <= 20) {
            int all_alnum = 1;
            for (size_t i = 0; i < alen; i++) {
                if (!isalnum((unsigned char)after[i]) && after[i] != '-' && after[i] != '_') {
                    all_alnum = 0; break;
                }
            }
            if (all_alnum) strip_suffix = 1;
        }
    }

    size_t copy_len;
    if (strip_suffix && sep) {
        copy_len = (size_t)(sep - src);
    } else {
        copy_len = strlen(src);
    }
    if (copy_len >= dst_sz) copy_len = dst_sz - 1;
    memcpy(dst, src, copy_len);
    dst[copy_len] = '\0';

    /* Replace hyphens with spaces if the result looks like a slug (no spaces) */
    int has_spaces = 0;
    for (size_t i = 0; i < copy_len; i++)
        if (dst[i] == ' ') { has_spaces = 1; break; }
    if (!has_spaces) {
        for (size_t i = 0; i < copy_len; i++)
            if (dst[i] == '-') dst[i] = ' ';
    }

    /* Title-case each word */
    int cap_next = 1;
    for (size_t i = 0; dst[i]; i++) {
        if (cap_next && isalpha((unsigned char)dst[i])) {
            dst[i] = (char)toupper((unsigned char)dst[i]);
            cap_next = 0;
        }
        if (dst[i] == ' ') cap_next = 1;
    }
}

/* ── Brief parser ─────────────────────────────────────────────────── */

#define MAX_LINES   2048
#define MAX_LINE    4096
#define MAX_ITEMS   64

typedef struct {
    char title[512];
    char clean_title[512]; /* sanitized version without IDs/slugs */
    char summary[MAX_ITEMS][MAX_LINE];
    int  summary_count;
    char actions[MAX_ITEMS][MAX_LINE];
    int  action_count;
    char deep[MAX_ITEMS][MAX_LINE];
    int  deep_count;
} Brief;

static void trim_bullet(char *dst, const char *src) {
    /* Strip leading "- " or "* " or whitespace */
    while (*src == ' ' || *src == '\t') src++;
    if (*src == '-' || *src == '*') { src++; if (*src == ' ') src++; }
    size_t len = strlen(src);
    while (len > 0 && (src[len-1] == '\n' || src[len-1] == '\r' || src[len-1] == ' '))
        len--;
    if (len >= MAX_LINE) len = MAX_LINE - 1;
    memcpy(dst, src, len);
    dst[len] = '\0';
}

static int parse_brief(const char *path, Brief *b) {
    FILE *fp = fopen(path, "r");
    if (!fp) { fprintf(stderr, "error: cannot open %s\n", path); return 1; }

    memset(b, 0, sizeof(*b));

    enum { SEC_NONE, SEC_TITLE, SEC_SUMMARY, SEC_ACTIONS, SEC_DEEP, SEC_TRANSCRIPT } section = SEC_NONE;
    char line[MAX_LINE];

    while (fgets(line, sizeof(line), fp)) {
        /* Detect sections */
        if (strncmp(line, "# ", 2) == 0 && section == SEC_NONE) {
            char *start = line + 2;
            while (*start == ' ') start++;
            size_t len = strlen(start);
            while (len > 0 && (start[len-1] == '\n' || start[len-1] == '\r'))
                len--;
            if (len >= sizeof(b->title)) len = sizeof(b->title) - 1;
            memcpy(b->title, start, len);
            b->title[len] = '\0';
            section = SEC_TITLE;
            continue;
        }
        if (strncmp(line, "## Summary", 10) == 0)      { section = SEC_SUMMARY; continue; }
        if (strncmp(line, "## Action", 9) == 0)         { section = SEC_ACTIONS; continue; }
        if (strncmp(line, "## Deep", 7) == 0)           { section = SEC_DEEP; continue; }
        if (strncmp(line, "## Transcript", 13) == 0)    { section = SEC_TRANSCRIPT; continue; }
        if (strncmp(line, "## ", 3) == 0)               { section = SEC_NONE; continue; }

        /* Skip blank lines */
        int blank = 1;
        for (const char *c = line; *c; c++) {
            if (!isspace((unsigned char)*c)) { blank = 0; break; }
        }
        if (blank) continue;

        /* Collect items */
        switch (section) {
            case SEC_SUMMARY:
                if (b->summary_count < MAX_ITEMS)
                    trim_bullet(b->summary[b->summary_count++], line);
                break;
            case SEC_ACTIONS:
                if (b->action_count < MAX_ITEMS)
                    trim_bullet(b->actions[b->action_count++], line);
                break;
            case SEC_DEEP:
                if (b->deep_count < MAX_ITEMS)
                    trim_bullet(b->deep[b->deep_count++], line);
                break;
            default:
                break;
        }
    }

    fclose(fp);
    return 0;
}

/* ── Format: Tweet Thread ─────────────────────────────────────────── */

static int emit_tweet_thread(const Brief *b, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/tweet-thread.md", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    char trunc[300];

    /* Hook tweet — use clean title (no video IDs), first summary as lead */
    fprintf(fp, "# Tweet Thread\n\n");
    fprintf(fp, "**1/**\n");
    if (b->clean_title[0]) {
        fprintf(fp, "%s\n\n", b->clean_title);
    }
    if (b->summary_count > 0) {
        truncate_at_word(b->summary[0], trunc, 200);
        cap_first(trunc);
        fprintf(fp, "%s\n\n", trunc);
    }
    fprintf(fp, "Thread 🧵👇\n\n");

    /* Body tweets — one per key summary point, truncated to ~270 chars */
    int tweet = 2;
    int limit = b->summary_count < 5 ? b->summary_count : 5;
    for (int i = 1; i < limit; i++) {
        truncate_at_word(b->summary[i], trunc, 270);
        cap_first(trunc);
        fprintf(fp, "**%d/**\n%s\n\n", tweet++, trunc);
    }

    /* Action items as a single tweet */
    if (b->action_count > 0) {
        fprintf(fp, "**%d/**\nKey takeaways:\n\n", tweet++);
        int al = b->action_count < 4 ? b->action_count : 4;
        for (int i = 0; i < al; i++) {
            truncate_at_word(b->actions[i], trunc, 250);
            cap_first(trunc);
            fprintf(fp, "→ %s\n", trunc);
        }
        fprintf(fp, "\n");
    }

    /* CTA */
    fprintf(fp, "**%d/**\n", tweet);
    fprintf(fp, "If this was useful, follow for more breakdowns like this.\n\n");
    fprintf(fp, "Repost the first tweet to help others find it.\n");

    fclose(fp);
    return 0;
}

/* ── Format: LinkedIn Post ────────────────────────────────────────── */

static int emit_linkedin(const Brief *b, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/linkedin.md", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    char trunc[512];

    fprintf(fp, "# LinkedIn Post\n\n");

    /* Hook line — use clean title */
    if (b->clean_title[0]) {
        fprintf(fp, "**%s**\n\n", b->clean_title);
    }

    /* Lead with first summary point */
    if (b->summary_count > 0) {
        truncate_at_word(b->summary[0], trunc, 400);
        cap_first(trunc);
        fprintf(fp, "%s\n\n", trunc);
    }

    /* Key points */
    if (b->summary_count > 1) {
        fprintf(fp, "Here's what stood out:\n\n");
        int limit = b->summary_count < 5 ? b->summary_count : 5;
        for (int i = 1; i < limit; i++) {
            truncate_at_word(b->summary[i], trunc, 300);
            cap_first(trunc);
            fprintf(fp, "→ %s\n", trunc);
        }
        fprintf(fp, "\n");
    }

    /* Deep insight — only if non-trivial */
    if (b->deep_count > 0 && strlen(b->deep[0]) > 20) {
        fprintf(fp, "The bigger picture:\n\n");
        truncate_at_word(b->deep[0], trunc, 400);
        cap_first(trunc);
        fprintf(fp, "%s\n\n", trunc);
    }

    /* Action / CTA */
    if (b->action_count > 0) {
        truncate_at_word(b->actions[0], trunc, 300);
        cap_first(trunc);
        fprintf(fp, "Next step: %s\n\n", trunc);
    }

    fprintf(fp, "---\n\n");
    fprintf(fp, "♻️ Repost if this resonated. Follow for more.\n");

    fclose(fp);
    return 0;
}

/* ── Format: Carousel (slides) ────────────────────────────────────── */

static int emit_carousel(const Brief *b, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/carousel.md", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    fprintf(fp, "# Carousel Slides\n\n");
    fprintf(fp, "Each slide below is one card. Keep text large, minimal.\n\n");

    /* Slide 1: Title / hook */
    fprintf(fp, "---\n\n");
    fprintf(fp, "## Slide 1 (Cover)\n\n");
    fprintf(fp, "**%s**\n\n", b->clean_title[0] ? b->clean_title : "Key Insights");
    if (b->summary_count > 0) {
        char trunc[300];
        truncate_at_word(b->summary[0], trunc, 280);
        cap_first(trunc);
        fprintf(fp, "%s\n\n", trunc);
    }

    /* Slides 2-5: Key points */
    int limit = b->summary_count < 5 ? b->summary_count : 5;
    for (int i = 1; i < limit; i++) {
        char trunc[300];
        truncate_at_word(b->summary[i], trunc, 280);
        cap_first(trunc);
        fprintf(fp, "---\n\n");
        fprintf(fp, "## Slide %d\n\n", i + 1);
        fprintf(fp, "%s\n\n", trunc);
    }

    /* Slide: Action items */
    if (b->action_count > 0) {
        fprintf(fp, "---\n\n");
        fprintf(fp, "## Slide %d (Takeaways)\n\n", limit + 1);
        int al = b->action_count < 4 ? b->action_count : 4;
        for (int i = 0; i < al; i++)
            fprintf(fp, "✅ %s\n", b->actions[i]);
        fprintf(fp, "\n");
    }

    /* CTA slide */
    fprintf(fp, "---\n\n");
    fprintf(fp, "## Slide %d (CTA)\n\n", limit + 2);
    fprintf(fp, "Save this post.\n\n");
    fprintf(fp, "Follow for more breakdowns.\n");

    fclose(fp);
    return 0;
}

/* ── Format: YouTube Description + Chapters ───────────────────────── */

static int emit_youtube_desc(const Brief *b, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/youtube-desc.md", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    char trunc[300];

    fprintf(fp, "# YouTube Description\n\n");

    /* Description */
    if (b->summary_count > 0) {
        truncate_at_word(b->summary[0], trunc, 280);
        cap_first(trunc);
        fprintf(fp, "%s\n\n", trunc);
    }

    fprintf(fp, "In this episode:\n");
    int limit = b->summary_count < 6 ? b->summary_count : 6;
    for (int i = 0; i < limit; i++) {
        truncate_at_word(b->summary[i], trunc, 250);
        cap_first(trunc);
        fprintf(fp, "• %s\n", trunc);
    }
    fprintf(fp, "\n");

    /* Chapters — estimate times by distributing evenly */
    fprintf(fp, "## Chapters\n\n");
    fprintf(fp, "0:00 Intro\n");
    for (int i = 0; i < limit && i < 5; i++) {
        char title[60];
        strncpy(title, b->summary[i], 55);
        title[55] = '\0';
        cap_first(title);
        char *dot = strchr(title, '.');
        if (dot && (dot - title) < 50) *(dot + 1) = '\0';
        /* Estimate chapter time — distribute evenly across ~10min video */
        int est_sec = (i + 1) * 120; /* ~2min per chapter */
        fprintf(fp, "%d:%02d %s\n", est_sec / 60, est_sec % 60, title);
    }
    fprintf(fp, "\n");

    /* Key takeaways for SEO */
    if (b->action_count > 0) {
        fprintf(fp, "## Key Takeaways\n\n");
        int al = b->action_count < 5 ? b->action_count : 5;
        for (int i = 0; i < al; i++) {
            truncate_at_word(b->actions[i], trunc, 250);
            cap_first(trunc);
            fprintf(fp, "→ %s\n", trunc);
        }
        fprintf(fp, "\n");
    }

    fprintf(fp, "---\n");
    fprintf(fp, "Subscribe for more episodes.\n");

    fclose(fp);
    return 0;
}

/* ── Format: Newsletter ───────────────────────────────────────────── */

static int emit_newsletter(const Brief *b, const char *out_dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/newsletter.md", out_dir);
    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    fprintf(fp, "# Newsletter Edition\n\n");

    /* Subject line */
    fprintf(fp, "**Subject:** %s\n\n", b->clean_title[0] ? b->clean_title : "This Week's Breakdown");
    fprintf(fp, "---\n\n");

    /* Intro */
    if (b->summary_count > 0) {
        char trunc[400];
        truncate_at_word(b->summary[0], trunc, 380);
        cap_first(trunc);
        fprintf(fp, "%s\n\n", trunc);
    }

    /* Body — key points */
    if (b->summary_count > 1) {
        fprintf(fp, "## What We Covered\n\n");
        int limit = b->summary_count < 6 ? b->summary_count : 6;
        for (int i = 1; i < limit; i++) {
            char trunc[400];
            truncate_at_word(b->summary[i], trunc, 380);
            cap_first(trunc);
            fprintf(fp, "**%d.** %s\n\n", i, trunc);
        }
    }

    /* Deep insight callout */
    if (b->deep_count > 0) {
        fprintf(fp, "## 💡 The Big Insight\n\n");
        fprintf(fp, "> %s\n\n", b->deep[0]);
    }

    /* Actions */
    if (b->action_count > 0) {
        fprintf(fp, "## Your Action Items\n\n");
        int al = b->action_count < 4 ? b->action_count : 4;
        for (int i = 0; i < al; i++)
            fprintf(fp, "- [ ] %s\n", b->actions[i]);
        fprintf(fp, "\n");
    }

    /* CTA */
    fprintf(fp, "---\n\n");
    fprintf(fp, "Reply to this email — I read every one.\n");

    fclose(fp);
    return 0;
}

/* ── Manifest writer ──────────────────────────────────────────────── */

static int write_manifest(const char *out_dir, const char *mode, const char **formats, int count) {
    char path[PATH_MAX];
    char ts[64];
    snprintf(path, sizeof(path), "%s/repurpose-manifest.json", out_dir);
    iso_timestamp(ts, sizeof(ts));

    FILE *fp = fopen(path, "w");
    if (!fp) return 1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"source_system\": \"BonfyreRepurpose\",\n");
    fprintf(fp, "  \"created_at\": \"%s\",\n", ts);
    fprintf(fp, "  \"mode\": \"%s\",\n", mode);
    fprintf(fp, "  \"formats\": [");
    for (int i = 0; i < count; i++) {
        fprintf(fp, "\"%s\"", formats[i]);
        if (i < count - 1) fprintf(fp, ", ");
    }
    fprintf(fp, "],\n");
    fprintf(fp, "  \"count\": %d\n", count);
    fprintf(fp, "}\n");

    fclose(fp);
    return 0;
}

/* ── Main ─────────────────────────────────────────────────────────── */

static void print_usage(void) {
    fprintf(stderr,
        "bonfyre-repurpose — turn brief artifacts into social media formats\n\n"
        "Usage:\n"
        "  bonfyre-repurpose <format> <brief-dir> <output-dir>\n\n"
        "Formats:\n"
        "  tweet-thread   Twitter/X thread (5-7 tweets)\n"
        "  linkedin       LinkedIn post (hook → insight → CTA)\n"
        "  carousel       Instagram/LinkedIn carousel slides\n"
        "  youtube-desc   YouTube description + chapters + SEO\n"
        "  newsletter     Email newsletter edition\n"
        "  all            Generate all formats\n\n"
        "Example:\n"
        "  bonfyre-repurpose all ./output/brief ./output/social\n");
}

int main(int argc, char **argv) {
    if (argc < 4) {
        print_usage();
        return 1;
    }

    const char *mode = argv[1];
    const char *brief_dir = argv[2];
    const char *out_dir = argv[3];

    /* Find brief.md */
    char brief_path[PATH_MAX];
    snprintf(brief_path, sizeof(brief_path), "%s/brief.md", brief_dir);

    FILE *test = fopen(brief_path, "r");
    if (!test) {
        /* Maybe brief_dir IS the file */
        snprintf(brief_path, sizeof(brief_path), "%s", brief_dir);
        test = fopen(brief_path, "r");
        if (!test) {
            fprintf(stderr, "error: cannot find brief.md in %s\n", brief_dir);
            return 1;
        }
    }
    fclose(test);

    Brief b;
    if (parse_brief(brief_path, &b) != 0) return 1;
    sanitize_title(b.title, b.clean_title, sizeof(b.clean_title));

    if (b.summary_count == 0 && b.action_count == 0) {
        fprintf(stderr, "error: brief has no summary or action items\n");
        return 1;
    }

    if (ensure_dir(out_dir) != 0) {
        fprintf(stderr, "error: cannot create output directory %s\n", out_dir);
        return 1;
    }

    int rc = 0;

    if (strcmp(mode, "tweet-thread") == 0) {
        rc = emit_tweet_thread(&b, out_dir);
        const char *f[] = {"tweet-thread"};
        if (rc == 0) write_manifest(out_dir, mode, f, 1);
    } else if (strcmp(mode, "linkedin") == 0) {
        rc = emit_linkedin(&b, out_dir);
        const char *f[] = {"linkedin"};
        if (rc == 0) write_manifest(out_dir, mode, f, 1);
    } else if (strcmp(mode, "carousel") == 0) {
        rc = emit_carousel(&b, out_dir);
        const char *f[] = {"carousel"};
        if (rc == 0) write_manifest(out_dir, mode, f, 1);
    } else if (strcmp(mode, "youtube-desc") == 0) {
        rc = emit_youtube_desc(&b, out_dir);
        const char *f[] = {"youtube-desc"};
        if (rc == 0) write_manifest(out_dir, mode, f, 1);
    } else if (strcmp(mode, "newsletter") == 0) {
        rc = emit_newsletter(&b, out_dir);
        const char *f[] = {"newsletter"};
        if (rc == 0) write_manifest(out_dir, mode, f, 1);
    } else if (strcmp(mode, "all") == 0) {
        rc |= emit_tweet_thread(&b, out_dir);
        rc |= emit_linkedin(&b, out_dir);
        rc |= emit_carousel(&b, out_dir);
        rc |= emit_youtube_desc(&b, out_dir);
        rc |= emit_newsletter(&b, out_dir);
        const char *f[] = {"tweet-thread", "linkedin", "carousel", "youtube-desc", "newsletter"};
        if (rc == 0) write_manifest(out_dir, mode, f, 5);
    } else {
        fprintf(stderr, "error: unknown format '%s'\n", mode);
        print_usage();
        return 1;
    }

    if (rc == 0) {
        printf("Repurposed: %s → %s (%s)\n", brief_path, out_dir, mode);
    }

    return rc;
}
