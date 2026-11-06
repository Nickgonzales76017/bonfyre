import { test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { findChrome } from "../capture/chrome-session.mjs";
import { compareDecodes, decodeWithFfmpeg, decodeWithWebCodecs } from "../engines/media-decode.mjs";

const ready = Boolean(findChrome()) && Boolean(Bun.which("ffmpeg"));
const run = argv => Bun.spawnSync(argv, { stdout: "ignore", stderr: "ignore" }).exitCode;

test.skipIf(!ready)("one decode operation, realised by the browser and by the native libraries, gives the same picture", async () => {
  const dir = mkdtempSync(join(tmpdir(), "bonfyre-media-"));
  try {
    const png = join(dir, "frame.png"), avif = join(dir, "frame.avif");
    expect(run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "testsrc2=size=320x240:rate=1", "-frames:v", "1", png])).toBe(0);
    // A lossless format: the two engines must return the very same pixels.
    const [browser, native] = [await decodeWithWebCodecs(png), await decodeWithFfmpeg(png)];
    for (const one of [browser, native]) expect(one).toMatchObject({ operation: "decode_image", width: 320, height: 240, frames: 1 });
    expect(browser.source_sha256).toBe(native.source_sha256);
    expect(browser.pixels_sha256).toBe(native.pixels_sha256);
    expect(compareDecodes(browser, native)).toMatchObject({ same_size: true, identical: true });
    expect([browser.engine, native.engine]).toEqual(["webcodecs", "ffmpeg"]);

    // AV1: decoding is defined exactly, the conversion to RGB is not, so the
    // two agree to within a couple of levels rather than bit for bit.
    if (run(["ffmpeg", "-v", "error", "-i", png, "-c:v", "libsvtav1", "-crf", "25", "-pix_fmt", "yuv420p", "-frames:v", "1", avif]) === 0) {
      const [b, n] = [await decodeWithWebCodecs(avif), await decodeWithFfmpeg(avif)];
      expect(n.codec).toBe("av1");
      const agreement = compareDecodes(b, n);
      expect(agreement.same_size).toBe(true);
      expect(agreement.worst_channel_difference).toBeLessThanOrEqual(4);
      expect(agreement.mean_difference).toBeLessThan(1);
      // And it is a different picture from the lossless one only by what the codec lost.
      expect(compareDecodes(b, browser).worst_channel_difference).toBeGreaterThan(0);
    }
    // A picture and a different picture are told apart.
    const other = join(dir, "other.png");
    run(["ffmpeg", "-v", "error", "-f", "lavfi", "-i", "smptebars=size=320x240:rate=1", "-frames:v", "1", other]);
    expect(compareDecodes(browser, await decodeWithFfmpeg(other)).identical).toBe(false);
    await expect(decodeWithWebCodecs(join(dir, "notes.txt"))).rejects.toThrow("not an image type");
  } finally { rmSync(dir, { recursive: true, force: true }); }
}, 180_000);
