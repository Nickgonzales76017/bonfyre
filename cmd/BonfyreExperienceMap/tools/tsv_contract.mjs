// UTF-8 TSV with the CSV quoting rules used by the retired compiler.
import { readFileSync } from "node:fs";

export function parseDelimited(input, delimiter = "\t") {
  const rows = [];
  let row = [], cell = "", quoted = false;
  for (let index = 0; index < input.length; index++) {
    const char = input[index];
    if (char === '"') {
      if (quoted && input[index + 1] === '"') { cell += '"'; index++; }
      else quoted = !quoted;
    } else if (char === delimiter && !quoted) {
      row.push(cell); cell = "";
    } else if ((char === "\n" || char === "\r") && !quoted) {
      if (char === "\r" && input[index + 1] === "\n") index++;
      row.push(cell); cell = "";
      if (row.some(value => value !== "")) rows.push(row);
      row = [];
    } else cell += char;
  }
  if (quoted) throw new Error("unterminated quoted delimited field");
  if (cell || row.length) { row.push(cell); rows.push(row); }
  return rows;
}

export function readTsv(path) {
  const [header, ...rows] = parseDelimited(readFileSync(path, "utf8"));
  if (!header) throw new Error(`empty TSV: ${path}`);
  return rows.map((cells, index) => {
    if (cells.length !== header.length) throw new Error(`TSV ${path} row ${index + 2}: ${cells.length} cells for ${header.length} columns`);
    return Object.fromEntries(header.map((name, column) => [name, cells[column]]));
  });
}

export function encodeDelimited(rows, delimiter = "\t") {
  const cell = value => {
    const text = String(value ?? "");
    return /["\r\n\t]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
  };
  return rows.map(row => row.map(cell).join(delimiter)).join("\n") + (rows.length ? "\n" : "");
}
