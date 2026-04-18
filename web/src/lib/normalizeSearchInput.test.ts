import { describe, it, expect } from 'vitest';
import { normalizeSearchInput } from './normalizeSearchInput';

describe('normalizeSearchInput', () => {
  it('trims leading and trailing whitespace', () => {
    expect(normalizeSearchInput('  hello  ')).toBe('hello');
  });

  it('strips a trailing zero-width space', () => {
    expect(normalizeSearchInput('12345678000195\u200B')).toBe('12345678000195');
  });

  it('strips embedded zero-width joiners and non-joiners', () => {
    expect(normalizeSearchInput('abc\u200Cdef\u200Dghi')).toBe('abcdefghi');
  });

  it('strips a leading BOM', () => {
    expect(normalizeSearchInput('\uFEFFhello')).toBe('hello');
  });

  it('preserves interior spaces when stripping zero-width', () => {
    expect(normalizeSearchInput('a\u200B b\u200D c')).toBe('a b c');
  });
});
