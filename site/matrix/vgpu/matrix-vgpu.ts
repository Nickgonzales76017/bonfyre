/* Progressive visual projection: it does not own Matrix state or auth. */
import { clock, effect, frameLoop, init, surface } from "vgpu";

const canvas = document.querySelector<HTMLCanvasElement>("#matrix-gpu");
const reduced = globalThis.matchMedia?.("(prefers-reduced-motion: reduce)").matches;
if (canvas && "gpu" in navigator && !reduced) {
  try {
    const gpu = await init();
    const target = surface(gpu, canvas, { dpr: [1, 1.5] });
    const field = effect(gpu, `
struct Params { time: f32, texel: vec2f, accent: vec3f, density: f32 }
@group(0) @binding(0) var<uniform> params: Params;
@fragment fn fs_main(@location(0) uv: vec2f) -> @location(0) vec4f {
  let p = (uv - vec2f(0.5)) * vec2f(1.7, 1.0);
  let radius = length(p);
  let rings = 0.5 + 0.5 * sin(radius * 40.0 - params.time * 0.45);
  let lanes = 0.5 + 0.5 * sin((p.x - p.y) * 28.0 + params.time * 0.18);
  let pulse = smoothstep(0.72, 0.08, radius) * rings * lanes;
  let fade = smoothstep(1.12, 0.06, radius);
  return vec4f(params.accent * pulse * fade, pulse * fade * 0.36);
}`, { set: { params: { time: 0, texel: target.texelSize, accent: [0.23, 0.48, 0.16], density: 1 } } });
    target.onResize(() => field.set({ params: { texel: target.texelSize } }));
    const time = clock(gpu);
    frameLoop(gpu, (frame) => { field.set({ params: { time: time.time } }); frame.pass(target, field); });
  } catch { canvas.remove(); }
} else canvas?.remove();
