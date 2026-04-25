# Plan Review: Refactor EntityDetailLayout

## 1. Create `EntityDetailLayout.svelte`
Move the common scaffolding into a new layout component. It will handle:
- Empty states (`!id`, `!idValid`) via `EntityNotFound`.
- Loading skeleton and error banner.
- The Archive fallback info banner.
- The standard header (`.hub-header`), displaying the kicker, SVG icon, title, and a `metaRow` snippet.
- Providing a standard Svelte 5 `children` snippet for the specific layout content.

## 2. Shared Styles & CSS Extraction
Since the prompt calls out the normalization constraint ("call them out in the PR description rather than silently normalizing them"), I will pass a CSS variable `--header-margin-bottom` to `EntityDetailLayout` for `CityDetailView` to preserve its `var(--space-xl)` instead of `var(--space-2xl)`.
I will also extract a small shared component `<ContractCard>` for the `recent-list` items, as the HTML, classes, and logic for displaying recent bids is identical across the three components (with `SupplierDetailView` adding a buyer name). This fits the permission to "lift any small shared atoms that fall out naturally".

## 3. Migrate `AgencyDetailView.svelte`
- Replace top-level boilerplate with `<EntityDetailLayout>`.
- Pass properties: `id`, `idValid`, `loading`, `error`, `data`, `kicker`, `iconHref`, `title`, `archivedParticao`.
- Provide `metaRow` snippet and `headerActions` snippet.
- Inside `children`: Use `EmptyState` directly inside, then the `rollup-grid` and `recent-list`.
- Use `<ContractCard>` for items.
- Remove duplicated CSS.

## 4. Migrate `SupplierDetailView.svelte`
- Replace top-level boilerplate with `<EntityDetailLayout>`.
- Use `<ContractCard showBuyer={true}>` for the recent list items.
- Remove duplicated CSS.

## 5. Migrate `CityDetailView.svelte`
- Replace top-level boilerplate with `<EntityDetailLayout>`.
- Preserve the `stats-row` rendering before `EmptyState` as currently done.
- Supply `--header-margin-bottom: var(--space-xl)` to preserve visual exactness.
- Remove duplicated CSS.

## Pre-commit
- Run `pre_commit_instructions` before submitting.

Does this plan look correct and cover all requirements?
