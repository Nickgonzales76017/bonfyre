// Runs simulations off the page's thread so editing never stalls.
import { simulate } from './process-sim.mjs';
self.onmessage = ({ data: { id, model, options } }) => {
  try {
    const started = performance.now(), result = simulate(model, options);
    // Waits are summarised here; the page needs a histogram, not every number.
    for (const s of result.stations) { const w = s.waits; let top = 1; for (const x of w) if (x > top) top = x; const bins = Array(30).fill(0);
      for (const x of w) bins[Math.min(29, Math.floor(x / top * 30))]++; s.histogram = { top, bins, count: w.length }; delete s.waits; }
    self.postMessage({ id, result, ms: performance.now() - started });
  } catch (error) { self.postMessage({ id, error: error.message }); }
};
