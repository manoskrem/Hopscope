import { useSyncExternalStore } from 'react';
import type { GraphDelta, GraphSnapshot } from '../contract/wire';
import { applyDelta, applySnapshot, EMPTY_STATE, type GraphState } from './reducer';

/**
 * A tiny dependency-free external store for the topology, consumed via
 * useSyncExternalStore. getState returns a stable reference between mutations
 * (each apply* swaps in a new immutable GraphState), which is exactly what
 * useSyncExternalStore needs.
 */
export class GraphStore {
  private state: GraphState = EMPTY_STATE;
  private readonly listeners = new Set<() => void>();

  getState = (): GraphState => this.state;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  applySnapshot(snapshot: GraphSnapshot): void {
    this.state = applySnapshot(snapshot);
    this.emit();
  }

  applyDelta(delta: GraphDelta): void {
    this.state = applyDelta(this.state, delta);
    this.emit();
  }

  reset(): void {
    this.state = EMPTY_STATE;
    this.emit();
  }

  private emit(): void {
    for (const listener of this.listeners) listener();
  }
}

/** App-wide singleton wired to the live WebSocket in App.tsx. */
export const graphStore = new GraphStore();

export function useGraphState(store: GraphStore = graphStore): GraphState {
  return useSyncExternalStore(store.subscribe, store.getState, store.getState);
}
