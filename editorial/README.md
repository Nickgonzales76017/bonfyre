# Aurekai Journal — authored editorial source

The articles in `editorial/posts/*.md` are edited public essays derived from earlier Aurekai and Bonfÿre project conversations, technical experiments and research discussions. They are original editorial synthesis, **not exported chats or verbatim quotations**.

The static builder is `scripts/build-blog.mjs`. It writes a searchable index, one stable HTML URL per post, RSS, an XML sitemap and `post-index.tsv` for future CMS migration.

Run from the repository root:

```sh
node scripts/build-blog.mjs
node scripts/build-blog.mjs --check
node --test scripts/tests/blog-build.test.mjs
```

## Native publication later

Each Markdown front matter contains a stable slug, title, description, category, icon, publication date, status, topics and origin. The generated `site/blog/post-index.tsv` also includes a stable `post:<slug>` identity, source path, SHA-256 of the authored body and canonical URL.

When moving into BonfÿreCMS / Matrix / Source Library, retain the slug and canonical URL as the public coordinate and map author/content/source into the native owners. The static site is a delivery format; it creates **no** CMS, publication, financial or foreign-system effects now.

Only source documents marked `status: edited-public` enter the build. The editorial archive may later include private research notes with other statuses, but those must never be emitted by the static compiler.

The published articles use public-facing examples. Do not paste raw chat exports, credentials, private customer details or internal source bodies into these files.