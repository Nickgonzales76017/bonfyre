# PandocFormatter

Universal format transformer via `pandoc`. Converts Bonfyre briefs and artifacts between formats.

## Subcommands
- `convert` — generic conversion (md → HTML/PDF/EPUB/etc.)
- `brief-to-html` — brief markdown to styled HTML5
- `brief-to-pdf` — brief to PDF (via wkhtmltopdf)
- `brief-to-epub` — brief to EPUB3
- `vtt-render` — VTT captions to readable HTML

## Usage
```bash
bash bin/pandoc_format.sh brief-to-html brief.md --out brief.html
bash bin/pandoc_format.sh convert transcript.md --to docx
```

## Install
```bash
brew install pandoc
```
