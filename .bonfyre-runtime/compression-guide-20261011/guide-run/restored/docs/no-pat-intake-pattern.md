# Bonfyre No-PAT Intake Pattern

This is the safer Pages-first intake pattern for Bonfyre demo apps when we do not yet have a GitHub App or server-side dispatch service.

## Goal

Remove browser-stored personal access tokens from Pages apps while preserving a GitHub-native workflow.

## Pattern

### In the browser

The app should:

1. accept a local file
2. generate an intake-safe filename
3. offer a download of that prepared file
4. open the correct GitHub repo folder in the web UI
5. instruct the user to upload and commit there
6. poll public artifact outputs from `raw.githubusercontent.com`

### In GitHub

GitHub itself remains the authenticated write surface:

- user uploads through GitHub web UI
- commit lands in `input/`
- reusable Bonfyre runtime workflow processes it
- Pages republishes the output

## Why this is better than browser PATs

- no token stored in `localStorage`
- no direct GitHub write API calls from client JS
- repo write permissions stay inside GitHub
- still works with a pure Pages deployment

## Limit

This is safer, but not yet seamless.

It still requires a user action in GitHub’s UI to upload and commit the file.

For fully seamless one-click intake, the next step is one of:

- GitHub App
- small serverless proxy
- dispatch endpoint backed by repo secrets

## Current examples

- `pages-town-box`
- `pages-explain-repo`

## Shared helper

Canonical helper source:

- `site/runtime/github-intake.js`
- `site/runtime/github-jobs.js`

Current app-local copies:

- `pages-town-box/site/js/github-intake.js`
- `pages-explain-repo/site/js/github-intake.js`
- `pages-*/site/js/github-jobs.js`

The split is intentional:

- `github-intake.js` handles prepared filenames, GitHub folder URLs, and public raw-file fetches
- `github-jobs.js` handles item persistence, intake preview wiring, status rendering, and polling scaffolding
