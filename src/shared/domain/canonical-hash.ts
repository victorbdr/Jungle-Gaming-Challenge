import { createHash } from 'node:crypto';

/**
 * JSON canônico: chaves de objetos ordenadas lexicograficamente (code points), sem espaços,
 * `undefined` omitido; arrays preservam a ordem. Strings/números/booleanos/null via JSON.stringify.
 */
export const canonicalJson = (value: unknown): string => {
  if (value === null || typeof value !== 'object') {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((v) => (v === undefined ? 'null' : canonicalJson(v))).join(',')}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${canonicalJson(v)}`).join(',')}}`;
};

/** sha256 hex do JSON canônico. */
export const sha256Canonical = (value: unknown): string => createHash('sha256').update(canonicalJson(value), 'utf8').digest('hex');
