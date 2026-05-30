import { describe, expect, it } from 'vitest';
import { resolveWsUrl } from '../url';

describe('resolveWsUrl', () => {
  it('uses ws:// for an http page', () => {
    expect(resolveWsUrl({ protocol: 'http:', host: 'localhost:5173' })).toBe(
      'ws://localhost:5173/ws',
    );
  });

  it('uses wss:// for an https page', () => {
    expect(resolveWsUrl({ protocol: 'https:', host: 'hopscope.example.com' })).toBe(
      'wss://hopscope.example.com/ws',
    );
  });

  it('honors a custom path', () => {
    expect(resolveWsUrl({ protocol: 'http:', host: 'h:8085' }, '/stream')).toBe(
      'ws://h:8085/stream',
    );
  });
});
