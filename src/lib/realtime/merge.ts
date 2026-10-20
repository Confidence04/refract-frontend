/**
 * Merge layer for realtime topics.
 *
 * Feature hooks receive streamed events and a REST snapshot; this module
 * provides the small, pure helpers they share so merge behaviour stays
 * consistent across topics. Feature-specific merge rules live with the
 * feature hooks (see issue #210 scope).
 */

export type MergeFn<T> = (current: T | undefined, incoming: T) => T;

/**
 * Default merge strategy: the latest value wins.
 * Suitable for snapshot-style topics where each event carries full state.
 */
export const replaceMerge: MergeFn<unknown> = (_current, incoming) => incoming;

/**
 * Merge a REST snapshot with any events that arrived while the snapshot was
 * in flight. Events are applied in arrival order on top of the snapshot so a
 * slow fetch never clobbers fresher streamed data.
 */
export function mergeSnapshot<T>(
  snapshot: T | undefined,
  pendingEvents: readonly T[],
  merge: MergeFn<T> = replaceMerge as MergeFn<T>,
): T | undefined {
  let result = snapshot;
  for (const event of pendingEvents) {
    result = merge(result, event);
  }
  return result;
}

/**
 * Append-style merge for list topics: keeps the newest `limit` items and
 * de-duplicates by `id` when present.
 */
export function appendMerge<T extends { id?: string | number }>(
  limit = 100,
): MergeFn<T[]> {
  return (current, incoming) => {
    const base = current ?? [];
    const next = incoming.id === undefined
      ? [...base, incoming]
      : [...base.filter((item) => item.id !== incoming.id), incoming];
    return next.length > limit ? next.slice(next.length - limit) : next;
  };
}
