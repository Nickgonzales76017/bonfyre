import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const schemaDir = join(dirname(dirname(fileURLToPath(import.meta.url))), "schemas");

export const schemas = new Map();
for (const file of readdirSync(schemaDir).filter((name) => name.endsWith(".json"))) {
  const schema = JSON.parse(readFileSync(join(schemaDir, file), "utf8"));
  schemas.set(file, schema);
  if (schema.$id) schemas.set(schema.$id, schema);
}

const DATE_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$/;

function typeOf(value) {
  if (value === null) return "null";
  if (Array.isArray(value)) return "array";
  if (Number.isInteger(value)) return "integer";
  return typeof value;
}

function typeMatches(value, want) {
  const actual = typeOf(value);
  if (want === "number") return actual === "number" || actual === "integer";
  return actual === want;
}

/** Validates against the draft-2020-12 subset the Aurekai schemas actually use. */
export function validate(instance, schema, path = "$", errors = []) {
  if (schema.$ref) {
    const target = schemas.get(schema.$ref) || schemas.get(schema.$ref.split("/").pop());
    if (!target) {
      errors.push(`${path}: unresolved $ref ${schema.$ref}`);
      return errors;
    }
    return validate(instance, target, path, errors);
  }

  if (schema.type) {
    const want = Array.isArray(schema.type) ? schema.type : [schema.type];
    if (!want.some((entry) => typeMatches(instance, entry))) {
      errors.push(`${path}: expected ${want.join("|")}, got ${typeOf(instance)}`);
      return errors;
    }
  }
  if ("const" in schema && instance !== schema.const) {
    errors.push(`${path}: expected const ${JSON.stringify(schema.const)}`);
  }
  if (schema.enum && !schema.enum.includes(instance)) {
    errors.push(`${path}: ${JSON.stringify(instance)} not in enum [${schema.enum.join(", ")}]`);
  }
  if (typeof instance === "number") {
    if ("minimum" in schema && instance < schema.minimum) errors.push(`${path}: ${instance} < ${schema.minimum}`);
    if ("maximum" in schema && instance > schema.maximum) errors.push(`${path}: ${instance} > ${schema.maximum}`);
  }
  if (typeof instance === "string" && schema.format === "date-time" && !DATE_TIME.test(instance)) {
    errors.push(`${path}: not a date-time: ${instance}`);
  }
  if (Array.isArray(instance)) {
    if ("minItems" in schema && instance.length < schema.minItems) {
      errors.push(`${path}: ${instance.length} items < minItems ${schema.minItems}`);
    }
    if (schema.uniqueItems) {
      const seen = instance.map((entry) => JSON.stringify(entry));
      if (new Set(seen).size !== seen.length) errors.push(`${path}: items not unique`);
    }
    if (schema.items) instance.forEach((entry, i) => validate(entry, schema.items, `${path}[${i}]`, errors));
  }
  if (instance && typeof instance === "object" && !Array.isArray(instance)) {
    for (const key of schema.required || []) {
      if (!(key in instance)) errors.push(`${path}: missing required '${key}'`);
    }
    for (const [key, value] of Object.entries(instance)) {
      const propertySchema = schema.properties?.[key];
      if (propertySchema) validate(value, propertySchema, `${path}.${key}`, errors);
      else if (schema.additionalProperties === false) errors.push(`${path}: unexpected property '${key}'`);
      else if (typeof schema.additionalProperties === "object") {
        validate(value, schema.additionalProperties, `${path}.${key}`, errors);
      }
    }
  }
  return errors;
}
