# PandocFormatter — actual file conversions and their limitations

This is a **format adapter**, not an authoritative document editor or proof of lossless conversion. [`bin/pandoc_format.sh`](bin/pandoc_format.sh) invokes an *installed* Pandoc command; for `brief-to-pdf`, it requires a working `wkhtmltopdf` engine. It operates on files supplied by the caller and emits a new file. No native account state, legal signature, source permission or upload is created.

[Full file/workflow guide](../../docs/real-workflows-and-interoperability.md#4-office-excel-and-paper-source-cell-and-page-provenance-survive-the-operation) · [BonfyreEmit](../../cmd/BonfyreEmit/src/main.c) · [BonfyreOCR](../../cmd/BonfyreOCR/src/main.swift) · [Books XLSX](../../cmd/BonfyreDecide/src/books_xlsx.c)

| Command | Source | Output | Dependency / important limit |
|---|---|---|---|
| `convert report.md --to docx` | Markdown document | `report.docx` | Pandoc; document structures and layout need independent inspection |
| `convert report.md --to html` | Markdown document | `report.html` | Pandoc standalone; no native site publishing |
| `brief-to-html brief.md --out brief.html` | Existing brief Markdown | HTML5 file | Pandoc; does not establish source citations or document review |
| `brief-to-pdf brief.md --out brief.pdf` | Existing brief Markdown | PDF via wkhtmltopdf | Requires Pandoc + wkhtmltopdf; no fallback in this shell wrapper |
| `brief-to-epub brief.md --out brief.epub` | Existing brief Markdown | EPUB3 | Pandoc; reader/device validation is separate |
| `vtt-render recording.vtt --out transcript.html` | WebVTT text | HTML via Pandoc | Shell script does not independently validate/correct caption timing or speakers |

~~~bash
bash components/PandocFormatter/bin/pandoc_format.sh convert transcript.md --to docx
bash components/PandocFormatter/bin/pandoc_format.sh brief-to-html brief.md --out brief.html
bash components/PandocFormatter/bin/pandoc_format.sh brief-to-pdf brief.md --out brief.pdf
bash components/PandocFormatter/bin/pandoc_format.sh brief-to-epub brief.md --out brief.epub
bash components/PandocFormatter/bin/pandoc_format.sh vtt-render captions.vtt --out captions.html
bash components/PandocFormatter/bin/pandoc_format.sh convert report.md --to html --dry-run
~~~

Install Pandoc according to your operating system (e.g. `brew install pandoc` on macOS). PDF also requires a suitable Pandoc PDF engine. The separate [BonfyreEmit C command](../../cmd/BonfyreEmit/src/main.c) emits HTML5, PDF (wkhtmltopdf with a weasyprint retry), EPUB3, plain text and RSS as a bundle; its behavior and output manifest should be checked at that owner, not presumed identical to this shell wrapper.

**Paper and spreadsheets are separate cases.** A scanned PDF may need [BonfyreOCR](../../cmd/BonfyreOCR/src/main.swift), which extracts text and positional lines with confidence from macOS Vision. Native [Books XLSX](../../cmd/BonfyreDecide/src/books_xlsx.c) reads specific workbook cells and builds review workbooks. A DOCX/PDF conversion is neither OCR nor a spreadsheet review.

**Safety and parity limits:** The current wrapper builds shell commands as strings and executes them via `eval`. Until it is changed to use an argument array, **do not pass untrusted filenames or format arguments to it**. Do not promise retention of tracked changes, formulas, embedded applications, page-faithful scanned text, legal signatures or arbitrary office-document features. Generated files need their own acceptance tests before being sent to customers.
