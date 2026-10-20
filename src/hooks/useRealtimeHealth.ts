"use client";

import { useEffect, useState } from "react";

export type RealtimeHealthState =
  | "live"
  | "connecting"
  | "reconnecting"
  | "offline";

export interface RealtimeHealth {
  state: RealtimeHealthState;
  lastEventAt: number | null;
}

const INITIAL_HEALTH: RealtimeHealth = {
  state: "connecting",
  lastEventAt: null,
};

/**
 * Reads the realtime connection health from the transport layer.
 *
 * This hook is intentionally transport-agnostic: it subscribes to a
 * global health source (if present) and falls back to a neutral
 * "connecting" state so SSR and client render match on first paint.
 */
export function useRealtimeHealth(): RealtimeHealth {
  const [health, setHealth] = useState<RealtimeHealth>(INITIAL_HEALTH);

  useEffect(() => {
    const source = (
      globalThis as {
        __realtimeHealth?: {
          getState?: () => RealtimeHealthState;
          subscribe?: (
            listener: (state: RealtimeHealthState) => void,
          ) => () => void;
        };
      }
    ).__realtimeHealth;

    if (!source) {
      return;
    }

    if (typeof source.getState === "function") {
      const current = source.getState();
      setHealth({ state: current, lastEventAt: Date.now() });
    }

    if (typeof source.subscribe === "function") {
      const unsubscribe = source.subscribe((state) => {
        setHealth({ state, lastEventAt: Date.now() });
      });
      return unsubscribe;
    }

    return;
  }, []);

  return health;
}
