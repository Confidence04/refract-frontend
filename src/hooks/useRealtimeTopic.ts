import { useCallback, useEffect, useRef, useState } from 'react';
import { mergeSnapshot, type MergeFn } from '../lib/realtime/merge';

export type RealtimeStatus = 'idle' | 'loading' | 'live' | 'polling' | 'error';

export interface UseRealtimeTopicOptions<T> {
  /** Fetches the REST snapshot used as the initial/fallback value. */
  snapshotFetcher?: () => Promise<T>;
  /** Merge rule applied to streamed events (defaults to latest-wins). */
  merge?: MergeFn<T>;
  /** When false the hook skips realtime and relies on polling only. */
  enabled?: boolean;
  /** Injectable polling strategy used when realtime is disabled/unavailable. */
  pollIntervalMs?: number;
  /** Whether the realtime feature flag is on. */
  realtimeEnabled?: boolean;
  /** Whether the realtime connection is healthy. */
  connected?: boolean;
  /** Subscribes to a topic; returns an unsubscribe function. */
  subscribe?: (topic: string, onEvent: (event: T) => void) => () => void;
}

export interface UseRealtimeTopicResult<T> {
  data: T | undefined;
  status: RealtimeStatus;
  lastEventAt: number | null;
}

const DEFAULT_POLL_INTERVAL_MS = 30_000;

/**
 * Generic React binding over the realtime transport + merge layers.
 *
 * Subscribes on mount, tears down on unmount, reacts to connection health,
 * and falls back to REST polling when realtime is disabled or unavailable.
 * Stable keys prevent duplicate subscriptions for identical topics.
 */
export function useRealtimeTopic<T>(
  topic: string,
  options: UseRealtimeTopicOptions<T> = {},
): UseRealtimeTopicResult<T> {
  const {
    snapshotFetcher,
    merge,
    enabled = true,
    pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
    realtimeEnabled = true,
    connected = true,
    subscribe,
  } = options;

  const [data, setData] = useState<T | undefined>(undefined);
  const [status, setStatus] = useState<RealtimeStatus>('idle');
  const [lastEventAt, setLastEventAt] = useState<number | null>(null);

  // Keep the latest merge/fetcher in refs so effects don't resubscribe on
  // every render (avoids resubscribe storms).
  const mergeRef = useRef(merge);
  mergeRef.current = merge;
  const fetcherRef = useRef(snapshotFetcher);
  fetcherRef.current = snapshotFetcher;

  const pendingEventsRef = useRef<T[]>([]);
  const mountedRef = useRef(true);

  const applyEvent = useCallback((event: T) => {
    if (!mountedRef.current) return;
    setData((current) => (mergeRef.current ?? ((_c, i) => i))(current, event));
    setLastEventAt(Date.now());
  }, []);

  const fetchSnapshot = useCallback(async () => {
    const fetcher = fetcherRef.current;
    if (!fetcher) return;
    try {
      const snapshot = await fetcher();
      if (!mountedRef.current) return;
      const pending = pendingEventsRef.current;
      pendingEventsRef.current = [];
      setData(mergeSnapshot(snapshot, pending, mergeRef.current));
    } catch {
      if (mountedRef.current) setStatus('error');
    }
  }, []);

  const realtimeActive = enabled && realtimeEnabled && connected && !!subscribe;

  // Snapshot fetch on mount / when the topic changes.
  useEffect(() => {
    mountedRef.current = true;
    setStatus('loading');
    void fetchSnapshot();
    return () => {
      mountedRef.current = false;
    };
  }, [topic, fetchSnapshot]);

  // Realtime subscription lifecycle.
  useEffect(() => {
    if (!realtimeActive || !subscribe) return;
    setStatus('live');
    const unsubscribe = subscribe(topic, (event) => {
      if (pendingEventsRef.current.length > 0) {
        pendingEventsRef.current.push(event);
      }
      applyEvent(event);
    });
    return () => {
      unsubscribe();
    };
  }, [topic, realtimeActive, subscribe, applyEvent]);

  // REST polling fallback when realtime is disabled or unavailable.
  useEffect(() => {
    if (!enabled || realtimeActive) return;
    setStatus('polling');
    const interval = setInterval(() => {
      void fetchSnapshot();
    }, pollIntervalMs);
    return () => clearInterval(interval);
  }, [enabled, realtimeActive, pollIntervalMs, fetchSnapshot]);

  return { data, status, lastEventAt };
}
