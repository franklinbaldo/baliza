import '@testing-library/jest-dom';
import { setVitestCucumberConfiguration } from '@amiceli/vitest-cucumber';

const splitTags = (raw: string | undefined): string[] =>
  raw
    ? raw
        .split(/[,\s]+/)
        .map((t) => t.trim().replace(/^@/, ''))
        .filter(Boolean)
    : [];

const includeTags = splitTags(process.env.VITEST_INCLUDE_TAGS);
const excludeTags = splitTags(process.env.VITEST_EXCLUDE_TAGS);

setVitestCucumberConfiguration({
  includeTags,
  // Default excludes roadmap-tagged scenarios. @planned scenarios throw stub
  // errors by contract; @wip scenarios fail on purpose to document gaps. Both
  // are opt-in via VITEST_INCLUDE_TAGS or VITEST_EXCLUDE_TAGS overrides (pass
  // `ignore` to effectively disable the default exclusion).
  excludeTags: excludeTags.length > 0 ? excludeTags : ['wip', 'planned'],
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
