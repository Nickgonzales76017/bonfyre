const base = process.env.BONFYRE_LIVE_BASE_URL ||
  (process.env.BONFYRE_LIVE_WITNESS ? 'https://aurekai.live' : undefined);

export const liveSurfaceEnabled = Boolean(base);

export async function surface(path) {
  if (!base) throw new Error('BONFYRE_LIVE_BASE_URL is required for live surface witnesses');
  const response = await fetch(new URL(path, base), {
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(45_000),
  });
  let body;
  try {
    body = await response.json();
  } catch {
    body = {};
  }
  return { status: response.status, body };
}

export async function surfaceHtml(path) {
  if (!base) throw new Error('BONFYRE_LIVE_BASE_URL is required for live surface witnesses');
  const response = await fetch(new URL(path, base), {
    headers: { Accept: 'text/html' },
    signal: AbortSignal.timeout(45_000),
  });
  return { status: response.status, body: await response.text() };
}

export async function rows(path) {
  const { status, body } = await surface(path);
  if (status !== 200) throw new Error(`${path}: HTTP ${status} ${JSON.stringify(body)}`);
  if (!Array.isArray(body.records)) throw new Error(`${path}: missing records[]`);
  return body.records;
}
