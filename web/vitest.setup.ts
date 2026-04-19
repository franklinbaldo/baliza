import '@testing-library/jest-dom';
import { setVitestCucumberConfiguration } from '@amiceli/vitest-cucumber';

const splitTags = (raw: string | undefined): string[] =>
  raw
    ? raw
        .split(',')
        .map((t) => t.trim().replace(/^@/, ''))
        .filter(Boolean)
    : [];

const includedTags = splitTags(process.env.VITEST_INCLUDE_TAGS);
const excludeTags = splitTags(process.env.VITEST_EXCLUDE_TAGS);

setVitestCucumberConfiguration({
  includeTags: includedTags,
  // Default excludes roadmap-tagged scenarios. @planned scenarios throw stub
  // errors by contract; @wip scenarios fail on purpose to document gaps. Both
  // are opt-in via VITEST_INCLUDE_TAGS or VITEST_EXCLUDE_TAGS overrides.
  excludeTags: excludeTags.length > 0 ? excludeTags : ['planned', 'wip'],
  predefinedSteps: [],
  mappedExamples: {},
});

if (typeof window !== 'undefined' && !window.matchMedia) {
  Object.defineProperty(window, 'matchMedia', {
    writable: true,
    value: (_query: string) => ({
      matches: false,
      media: _query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });
}

if (typeof Element !== 'undefined' && !Element.prototype.animate) {
  Element.prototype.animate = () =>
    ({ cancel: () => {}, finish: () => {}, addEventListener: () => {} }) as unknown as Animation;
}
