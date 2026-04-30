export { render, mockFetchError } from '../shared';

export function plannedStep(description: string): never {
  throw new Error(`planned: ${description} not yet implemented`);
}

export function wipStep(description: string): never {
  throw new Error(`wip: ${description} expected to fail until implemented`);
}

export function noop(): void {}
