// Python JSON compatibility for hashes and receipts during native cutover.
export const canonicalObject = value => Array.isArray(value) ? value.map(canonicalObject)
  : value && typeof value === "object"
    ? Object.fromEntries(Object.keys(value).sort().map(key => [key, canonicalObject(value[key])]))
    : value;

export const canonicalJson = value => JSON.stringify(canonicalObject(value));

export const canonicalAscii = value => canonicalJson(value).replace(/[^\x00-\x7F]/gu, char => {
  const code = char.codePointAt(0);
  if (code <= 0xffff) return `\\u${code.toString(16).padStart(4, "0")}`;
  const offset = code - 0x10000;
  return `\\u${(0xd800 + (offset >> 10)).toString(16)}\\u${(0xdc00 + (offset & 1023)).toString(16)}`;
});

export const pythonFlat = value => Array.isArray(value) ? `[${value.map(pythonFlat).join(", ")}]`
  : value && typeof value === "object"
    ? `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}: ${pythonFlat(value[key])}`).join(", ")}}`
    : JSON.stringify(value);

export const prettyCanonical = value => JSON.stringify(canonicalObject(value), null, 2);
