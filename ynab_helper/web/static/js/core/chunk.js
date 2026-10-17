// Splitting work into server-sized batches (pure; no DOM).

/** The most ids one POST /api/dedupe/delete accepts (maintenance.dedupe.MAX_DELETE_IDS). */
export const DEDUPE_CHUNK_SIZE = 200;

/** Split `items` into consecutive chunks of at most `size` (order kept). */
export function chunk(items, size = DEDUPE_CHUNK_SIZE) {
  if (!Number.isInteger(size) || size < 1) throw new RangeError("chunk size must be a positive integer");
  const out = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}
