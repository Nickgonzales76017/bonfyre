#!/bin/sh
# What this proves: the organ reports what a detector found in real images,
# and writes nothing when it has no detector or could read no frame.
set -eu
bin=${1:?bonfyre-detect-objects required}
vision=${2:?bonfyre-detect-vision required}
picture=${3:?a real image containing a person or a face is required}
case "$bin" in /*) ;; *) bin="$(pwd)/$bin";; esac
case "$vision" in /*) ;; *) vision="$(pwd)/$vision";; esac
tmp=$(mktemp -d "${TMPDIR:-/tmp}/bonfyre-detect-test.XXXXXX")
trap 'rm -rf "$tmp"' EXIT INT TERM

mkdir -p "$tmp/frames" "$tmp/alone"
cp "$picture" "$tmp/frames/picture.jpg"

BONFYRE_YOLO_BINARY="$vision" "$bin" --input "$tmp/frames" --out "$tmp/out" --confidence 0.3 >/dev/null
grep -q '"frame_count": 1' "$tmp/out/objects.json"
grep -q '"frames_not_detected": 0' "$tmp/out/objects.json"
grep -q '"model": "apple-vision/' "$tmp/out/objects.json"
grep -Eq '"label": "(person|face)"' "$tmp/out/objects.json"
grep -q '"bbox"' "$tmp/out/picture.jpg.json"
! grep -q 'requires_model' "$tmp/out/objects.json"

# No detector anywhere: refuse, and leave nothing behind.
cp "$bin" "$tmp/alone/detect"
if env -i HOME="$tmp/nohome" PATH=/usr/bin "$tmp/alone/detect" --input "$tmp/frames" --out "$tmp/none" 2>/dev/null; then
  echo 'ran without a detector' >&2; exit 1
fi
[ ! -e "$tmp/none/objects.json" ]

# A file that is not an image is not a frame with zero detections.
mkdir -p "$tmp/bad"; : > "$tmp/bad/empty.jpg"
if BONFYRE_YOLO_BINARY="$vision" "$bin" --input "$tmp/bad" --out "$tmp/badout" >/dev/null 2>&1; then
  echo 'an unreadable frame was reported as detected' >&2; exit 1
fi
[ ! -e "$tmp/badout/objects.json" ]

# A detector that exits 0 without writing detections did not detect.
printf '#!/bin/sh\nexit 0\n' > "$tmp/liar"; chmod +x "$tmp/liar"
if BONFYRE_YOLO_BINARY="$tmp/liar" "$bin" --input "$tmp/frames" --out "$tmp/liarout" >/dev/null 2>&1; then
  echo 'a detector that wrote nothing was believed' >&2; exit 1
fi
[ ! -e "$tmp/liarout/objects.json" ]

printf '%s\n' 'detect objects: PASS'
