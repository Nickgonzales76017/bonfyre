# Recording to searchable workspace

Turn a recording into a workspace where people can find an explanation, play that moment and save a note with its timestamp.

This walkthrough uses the complete six-minute [Composing Webb Telescope Images](https://science.nasa.gov/asset/webb/composing-webb-telescope-images/) video. Joseph DePasquale and Alyssa Pagan explain how telescope data becomes a color image. You will keep the video, generate its transcript and search across the conversation locally.

## Prepare the recording

Start in a configured Bonfÿre checkout with Bun, FFmpeg and the MediaPrep, Transcribe, Embed and Vec commands installed. The example uses the local Whisper base model and `nomic-embed-text-onnx`. Keep the commands in one shell.

The workspace accepts MP4, WAV, MP3, M4A and WebM files up to 25 MB and ten minutes. This command makes a compact viewing copy of the complete video, including its audio:

```sh Terminal
bonfyre_recording=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-recording-guide.XXXXXX")
printf '%s\n' "$bonfyre_recording"
bonfyre_video='https://assets.science.nasa.gov/content/dam/science/missions/webb/outreach/migrated/2023/STScI-01H70FA581K8JD844S42M4DWY8.mp4'
ffmpeg -nostdin -hide_banner -loglevel warning -y -i "$bonfyre_video" \
  -vf scale=960:-2 -c:v libx264 -preset veryfast -crf 30 \
  -maxrate 384k -bufsize 768k -c:a aac -b:a 48k -movflags +faststart \
  "$bonfyre_recording/webb-images.mp4"
bonfyre mediaprep inspect "$bonfyre_recording/webb-images.mp4" \
  > "$bonfyre_recording/media.json"
```

The result runs for about 360 seconds. For your own recording, start with a local file and keep its original alongside the working copy.

## Build the workspace

The recording command calls the existing transcription engine, keeps its timed output and groups split sentences into readable passages. The workspace stores the media, transcript and search index together.

```sh Terminal
bun components/BonfyreServiceSite/site.mjs recording \
  "$bonfyre_recording/webb-images.mp4" "$bonfyre_recording/workspace" \
  'Composing Webb telescope images' \
  --source-url 'https://science.nasa.gov/asset/webb/composing-webb-telescope-images/' \
  --credit 'NASA, ESA, CSA and the Space Telescope Science Institute · Joseph DePasquale and Alyssa Pagan' \
  > "$bonfyre_recording/import.json"
cat "$bonfyre_recording/import.json"
```

The receipt reports the duration and passage count. In the retained run, 95 recognition segments became 60 passages. The original recognition output stays under `workspace/.transcription`; the readable collection is `workspace/documents.json`.

## Open it

Port 8928 must be free. The first start indexes the transcript with the local embedding model; subsequent starts reuse that index.

```sh Terminal
bonfyre_recording_port=8928
bun components/BonfyreServiceSite/site.mjs documents \
  "$bonfyre_recording/workspace" --port "$bonfyre_recording_port" \
  > "$bonfyre_recording/serve.log" 2>&1 &
bonfyre_workspace_pid=$!
bonfyre_wait=0
until curl --fail --silent "http://127.0.0.1:$bonfyre_recording_port/" \
  > "$bonfyre_recording/workspace.html"; do
  kill -0 "$bonfyre_workspace_pid"
  bonfyre_wait=$((bonfyre_wait + 1))
  test "$bonfyre_wait" -lt 120
  sleep 1
done
printf 'Open http://127.0.0.1:%s/\n' "$bonfyre_recording_port"
```

Open the printed address. Try these questions:

| Question | Passage to find |
|---|---|
| Why do the raw telescope images look almost black? | Around 2:10, where the speakers discuss what those images contain |
| How many filters does the near infrared camera have? | Around 4:59, where they compare the cameras |
| How are colors assigned to infrared wavelengths? | Around 5:06, where they describe wavelength order |

Choose **Play** on a result. The video opens at that passage, and the transcript highlights the words as playback advances. Turn off **Follow playback** to read elsewhere while the video continues.

Write a note under the passage and choose **Save cited note**. Save another note at a different moment, then reload. Both notes should remain, and **Open cited source** should restore the matching timestamp. Notes stay in the current browser profile.

**Download transcript** saves the text with timestamps and the publisher's credit. **View original** opens the source page. Use **Exact phrase** when you want literal words rather than a question; a phrase absent from the recording returns no matches.

## Keep the result

The directory printed at the start contains the video, transcript, index and import receipt. Move the workspace to a lasting location before clearing temporary files. To stop this local server:

```sh Terminal
kill "$bonfyre_workspace_pid"
wait "$bonfyre_workspace_pid" || true
```

Run the `documents` command against that workspace directory to reopen it. Keep the same local address and browser profile to retrieve the notes saved there.

## Publish an edited article

A recording workspace is useful for review, study and finding source passages. An approved article can become a customer site page through `bonfyre site page`, with the source recording linked in its text. [White-label delivery](/white-label-delivery) covers customer sites and scoped assistant access; [request to books](/request-to-books) covers bookings and payment records.
