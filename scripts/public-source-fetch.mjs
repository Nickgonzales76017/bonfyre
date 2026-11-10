// One machine-readership gate for public JSON, 402 discovery, and future collectors.
import { robotsAllows } from "../cmd/BonfyreSourceFrontier/tools/robots_policy.mjs";

export async function boundedText(response, limit) {
  const length = Number(response.headers.get("content-length"));
  if (Number.isFinite(length) && length > limit) throw new Error(`response exceeds ${limit} bytes`);
  if (!response.body) throw new Error("response has no body");
  const reader = response.body.getReader(), chunks = [];
  let bytes = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytes += value.byteLength;
      if (bytes > limit) throw new Error(`response exceeds ${limit} bytes`);
      chunks.push(value);
    }
  } catch (error) { await reader.cancel(); throw error; }
  finally { reader.releaseLock(); }
  const body = new Uint8Array(bytes);
  let offset = 0;
  for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
  return new TextDecoder("utf-8", { fatal: false }).decode(body);
}

export async function publicSourceFetch(url, { origin, agent, fetcher = fetch, robots = new Map(),
  method = "GET", accept = "application/json", body, timeoutMs = 30000 } = {}) {
  const target = new URL(url);
  if (target.protocol !== "https:" || target.origin !== origin)
    throw new Error(`source request left its admitted origin: ${target.href}`);
  if (target.username || target.password) throw new Error("source request contains credentials");
  if (!robots.has(origin)) {
    const policy = await fetcher(new URL("/robots.txt", origin), { method: "GET", redirect: "error",
      headers: { "User-Agent": agent }, signal: AbortSignal.timeout(timeoutMs) });
    if (!policy.ok) throw new Error(`robots policy unavailable: HTTP ${policy.status}`);
    robots.set(origin, await boundedText(policy, 1024 * 1024));
  }
  if (!robotsAllows(robots.get(origin), agent, target.href))
    throw new Error(`robots policy refuses ${target.pathname}`);
  return fetcher(target, { method, redirect: "error", headers: { "User-Agent": agent, Accept: accept,
    ...(body === undefined ? {} : { "Content-Type": "application/json" }) }, body,
    signal: AbortSignal.timeout(timeoutMs) });
}
