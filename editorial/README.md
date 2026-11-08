# Aurekai Journal — public articles and editorial source

The Aurekai Journal currently holds **46 edited articles**: sixteen essays based on earlier Aurekai/Bonfÿre work and thirty additional pieces examining external research, mathematical claims, tools, markets, standards, and original posts on X.

The editorial Markdown in `editorial/posts/*.md` is authoritative. Each article has a stable slug, canonical URL, category, icon, publication date, source lineage, and `status: edited-public`. Posts responding to outside material include a `source_url` for the primary source. These are independently written articles, **not raw chat exports or endorsements by the cited organizations**.

## Static publication

From the public repository root:

```sh
sh scripts/render-editorial.sh
sh scripts/render-editorial.sh --check
```

The shell wrapper uses the Python 3 standard library available on the Pages runner. It produces `site/blog/index.html`, one HTML page per article, `feed.xml`, `sitemap.xml`, and `post-index.tsv`. The published HTML is built from source inside GitHub Actions immediately before the Pages artifact is uploaded. The independent **Aurekai Journal Validation** workflow performs the same deterministic check on every editorial change.

No new installed JavaScript program is introduced, and the native code-budget ratchet remains unchanged. The original static journal's client-side search runs as a small inline enhancement; all article links and the feed work without it.

## Native publishing integration later

The TSV manifest maps each post to a stable `post:<slug>` identity and the original Markdown path, SHA-256, canonical URL, and cited primary source URL. BonfÿreCMS / Matrix / Source Library can consume these records later while retaining the existing public article URLs.

A public article must be reviewed before setting `status: edited-public`. Do not publish raw project conversation logs, private customer details, credentials, account tokens or unpublished integration contracts. Attributed quotations should be short and linked to their original context; mathematical examples must identify hypothetical inputs instead of presenting them as measured results.

## Editorial roadmap

The long-term journal can grow toward approximately 500 articles spanning independent business operations, supplier markets, local models, application grammars, physical resources, publishing, civic information, standards, applied mathematics, and careful challenges to popular technology claims. The goal is a useful research publication, not volume alone.
