# Ecosystem Name Reservation Checklist

Reserve these names immediately before pushing the public repo.
Priority order listed below.

---

## Priority 1 — Must reserve before first public push

- [ ] **github.com/aurekai** (org)
  - URL: https://github.com/organizations/plan
  - Create org, then create repo `aurekai/aurekai`
  - Run `scripts/split-aurekai-repo.sh --push` after org exists

- [ ] **npm @aurekai scope**
  - URL: https://www.npmjs.com/org/create
  - Create org `aurekai`, publish `@aurekai/runtime@0.8.0-alpha.1`
  - Command: `npm publish --access public --tag alpha` (from Aurekai/)

- [ ] **PyPI aurekai**
  - URL: https://pypi.org/account/register/
  - Reserve: `pip install twine && twine upload` with a stub `aurekai` package
  - Stub setup: `pyproject.toml` with `name = "aurekai"` and `version = "0.8.0a1"`


Docker/GHCR publishing deferred until after alpha.1 to reduce early cost and maintenance.
Primary alpha distribution is GitHub Releases + npm + direct akai CLI.

---

## Priority 2 — Reserve before first public announcement

- [ ] **HuggingFace aurekai org**
  - URL: https://huggingface.co/organizations/new
  - Create repos: `aurekai/model-memory`, `aurekai/sae-dictionaries`, `aurekai/fpqx-alignments`

- [ ] **aurekai.ai** domain
  - Registrar: Namecheap / Cloudflare / Google Domains
  - Also reserve: `aurekai.dev`, `akai.sh`, `aurekai.com`

- [ ] **VS Code publisher aurekai**
  - URL: https://marketplace.visualstudio.com/manage
  - Create publisher ID `aurekai`
  - Display name: `Aurekai`

- [ ] **Open VSX publisher aurekai**
  - URL: https://open-vsx.org/user-settings/namespaces
  - Namespace: `aurekai`

---

## Priority 3 — Reserve before Homebrew/Helm GA

- [ ] **Homebrew tap aurekai/tap**
  - Create GitHub repo `aurekai/homebrew-tap`
  - Add formula `akai.rb` pointing to `akai-hyper-v0.8.0-alpha.1-*` binary
  - Test: `brew tap aurekai/tap && brew install akai`

- [ ] **Helm repo**
  - Host `https://charts.aurekai.ai` via GitHub Pages from `aurekai/aurekai` `gh-pages` branch
  - Run `helm package helm/aurekai-runtime && helm repo index .`

---

## Name → handle mapping

| Surface        | Name / Handle                        | Status |
|----------------|--------------------------------------|--------|
| GitHub org     | `aurekai`                            | [ ] |
| GitHub repo    | `aurekai/aurekai`                    | [ ] |
| npm scope      | `@aurekai`                           | [ ] |
| npm package    | `@aurekai/runtime`                   | [ ] |
| PyPI           | `aurekai`                            | [ ] |
| HuggingFace    | `aurekai`                            | [ ] |
| Domain         | `aurekai.ai`                         | [ ] |
| Domain         | `aurekai.dev`                        | [ ] |
| Domain         | `akai.sh`                            | [ ] |
| VS Code        | publisher `aurekai`                  | [ ] |
| Open VSX       | namespace `aurekai`                  | [ ] |
| Homebrew tap   | `aurekai/tap`                        | [ ] |
| Helm chart     | `aurekai-runtime`                    | [ ] |
| CLI handle     | `akai` (via npm bin)                 | [ ] |
