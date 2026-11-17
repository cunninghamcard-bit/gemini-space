const INT32_MAX = 2_147_483_647;

const DROPPED_KEYWORDS = new Set([
  "$schema",
  "propertyNames",
  "additionalProperties",
  "$id",
  "$defs",
  "definitions",
]);

/**
 * Sanitizes a JSON Schema object returned by `agy` (Gemini API uppercase types
 * or JSON Schema draft keywords) so Gemini Web Connectors accept `tools/list`
 * without requiring an external rewrite proxy.
 */
export function cleanGeminiToolSchema(node: unknown): Record<string, unknown> {
  const cleaned = cleanNode(node);
  if (cleaned && typeof cleaned === "object" && !Array.isArray(cleaned)) {
    const obj = cleaned as Record<string, unknown>;
    if (!obj.type) obj.type = "object";
    return obj;
  }
  return { type: "object" };
}

function cleanNode(node: unknown): unknown {
  if (Array.isArray(node)) {
    return node.map(cleanNode);
  }
  if (!node || typeof node !== "object") {
    return node;
  }

  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(node)) {
    if (key === "properties" && value && typeof value === "object" && !Array.isArray(value)) {
      out.properties = Object.fromEntries(
        Object.entries(value).map(([propKey, propVal]) => [propKey, cleanNode(propVal)]),
      );
      continue;
    }

    if (DROPPED_KEYWORDS.has(key)) continue;

    if (key === "type" && typeof value === "string") {
      out.type = value.toLowerCase();
      continue;
    }

    if (key === "const") {
      out.enum = [value];
      continue;
    }

    if (key === "exclusiveMinimum" && typeof value === "number") {
      if (out.minimum === undefined) {
        out.minimum = value + (Number.isInteger(value) ? 1 : 0);
      }
      continue;
    }

    if (key === "exclusiveMaximum" && typeof value === "number") {
      if (out.maximum === undefined && Math.abs(value) <= INT32_MAX) {
        out.maximum = value - (Number.isInteger(value) ? 1 : 0);
      }
      continue;
    }

    if ((key === "maximum" || key === "minimum") && typeof value === "number" && Math.abs(value) > INT32_MAX) {
      continue;
    }

    out[key] = cleanNode(value);
  }

  return out;
}
