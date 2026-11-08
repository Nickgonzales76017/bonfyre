# Aurekai Journal — editable editorial source

The source articles in `editorial/posts/*.md` are public-facing essays adapted from earlier Aurekai and Bonfÿre discussions, experiments and research. They are not raw chat exports or verbatim transcripts.

Each article has stable front matter: slug, title, description, category, icon, date, editorial status, topics and origin. Its corresponding static page is checked into `site/blog/<slug>/index.html` and served at `https://nickgonzales76017.github.io/bonfyre/blog/<slug>/`.

The static journal also contains an article index, subject search, RSS, XML sitemap and `site/blog/post-index.tsv`. The manifest carries the subject identity `post:<slug>`, source path and canonical URL for a later move to BonfÿreCMS, Matrix and Source Library.

**No new JavaScript service or generator is installed.** Bonfÿre's native source-size ratchet remains unchanged. For now the Markdown is the editable source and the HTML files are committed publication artifacts. After native CMS authoring is connected, these coordinates and identities can become output from the existing native publishing owners.

Articles use public examples and link to official sources and project pages. Do not add raw chat logs, account identifiers, credentials or private customer data to the public editorial directory.
