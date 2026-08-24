# ImageMagickOps

Deterministic image and waveform transforms via ImageMagick.

## Subcommands
- `thumbnail` — generate thumbnail at given size
- `waveform` — sox spectrogram → resized waveform PNG
- `resize` — resize image
- `convert` — format conversion
- `strip-meta` — remove EXIF/metadata

## Usage
```bash
bash bin/imagemagick_ops.sh thumbnail photo.jpg --size 128x128
bash bin/imagemagick_ops.sh waveform recording.wav --out wave.png
```

## Install
```bash
brew install imagemagick sox
```
