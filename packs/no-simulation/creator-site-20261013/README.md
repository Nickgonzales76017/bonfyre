# Published Creator audio workspace

The working sample is at https://aurekai-work-samples.pages.dev/audio/ and linked from the home-page sample catalogue. It uses the existing ServiceSite builder, CMS record, Cloudflare project and browser workbench store.

Three generated-tone tracks render through the verified 259-byte native C kernel in a module worker. Packaging retains only selected source frames, reducing raw PCM from 2,304,000 to 1,536,000 bytes. Each package has a content-addressed directory. Source and initial-mix verification precede playback; changed levels can be saved locally or downloaded as 16-bit WAV.

`local/verification.json` records eighteen passing browser checks, including missing and corrupted sources. `live/verification.json` records sixteen checks on the public route, including saved-level reload, browser Back, stale-tab refusal, playback, stop and actual WAV download. `local/download-verification.json` independently identifies the downloaded file as six-second, 48 kHz, two-channel signed 16-bit PCM.

`public-readback.json` matches all thirteen deployed HTML, CSS and audio-package assets byte for byte. `pages/browser-verification.json` audits all eight public routes at desktop and phone widths with no failures or writes. Existing catalogue and planner workflows pass 57 and 28 local browser checks. `focused-tests.log` records 30 passing tests with 198 assertions.

The first public verification exposed a transient Back-navigation readiness state. The client now clears readiness before terminating its worker on pagehide, and a restored history page reloads its worker. The earlier failed witness and first deployment readback are preserved separately. `previous-content.json`, `previous-cms-record.json` and `previous-deployment.json` retain the state before publication. The final deployment ID is in `public-readback.json`.

Atlas and ExperienceMap checks pass; `corpus-installation.json` records the refreshed local registry. `verification.json` pins changed source files. No outreach or live lead submission was made. Browser saves remain specific to the sample ID, native reference digest, browser profile and origin; there is no account sync, imported audio, effects or recording.
