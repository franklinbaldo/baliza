# Baliza Design Constitution

## Curva & Concreto

*"A curva e para o usuario; o concreto e para os dados."*

Baliza is a PNCP monitoring interface, so the visual identity must be civic, legible, and fast. The product should feel Brazilian-modernist at first glance without turning procurement data into decoration.

## Principles

**1. Build for analysts and oversight, not for impressions.**
The page exists to help someone monitor, find, compare, and audit public procurement data from the PNCP.

**2. Content is the interface.**
Tables, numbers, manifest logs, and structural hierarchy are the design.

**3. Curves on chrome and heroes; sharp on data.**
Niemeyer curves belong on navigation chrome, empty states, hero panels, and primary actions. Dense data cards, tables, and definition lists stay mostly sharp.

**4. Azulejo is ornament, never noise.**
Bulcao-inspired patterns can appear on chrome, section dividers, empty states, and hover surfaces. Behind text, pattern opacity must stay subtle enough for WCAG AA.

**5. One accent cannot do every job.**
Volpi red is reserved for brand and primary actions. Brasilia azul is information and focus. Mata verde is success. Ouro is warning. Tijolo is decorative warmth.

**6. Radius is a closed enum.**
Use only `--radius-0`, `--radius-sm`, `--radius-lg`, `--radius-arch`, and `--radius-pill`. Adding another radius requires this document to change.

**7. Density is good when structure is strong.**
Do not fear information-rich dashboards. A dense page with clear hierarchy is better than a sparse page with hidden metrics.

**8. Speed is part of aesthetics.**
Fast pages feel intelligent. We fetch static manifests and let the client do the work quickly.

**9. HTML first, CSS second, JavaScript last.**
Astro generates static shells. Svelte is reserved for interactive data islands.

**10. Truthful metrics over vanity.**
Show successful syncs, quarantine counts, timestamps, and source links without hiding failure modes.

## Pico CSS Primitives Mapping

Baliza's design language is executed through Pico primitives plus a thin `global.css` extension layer.

- Cards and panels: use `<article>`.
- Page and section headers: use `<header><hgroup><h2>...</h2><p>...</p></hgroup></header>`.
- Navigation: use `<nav>`, lists, and semantic links.
- Disclosure and filters: use `<details>` and `<summary>` when appropriate.
- Loading states: use `aria-busy="true"`; use `.is-skeleton` only for skeleton placeholders already defined in `global.css`.
- Error and alert messages: use `<article role="alert" data-invalid="true">`.
- Neutral async/status messages: use `[role="status"]`.
- Buttons: use `<button>`, `<button class="outline">`, `<button class="secondary">`, or `<a role="button">` when the element is truly navigational.
- Actions layout: use the global `.actions` utility.
- Badges and compact metadata: use `[data-badge]`.
- Icons or icon-leading labels: use `[data-icon]`.
- Horizontal tables: wrap `<table>` in `.table-scroll`.

*Note: Baliza does not create a parallel component styling system. Pico is the primitive layer; `global.css` is the civic design-system layer; Svelte/Astro components provide behavior and composition, not local visual law.*

## Practical Defaults

- Use `Fraunces` for display hierarchy, `Manrope` for UI text, and `JetBrains Mono` for metrics and identifiers.
- Use `--font-logo` only for the Baliza wordmark or hero-scale brand moments.
- Use `.azulejo-*` primitives for ornamental surfaces and `.section-divider` instead of plain decorative rules.
- Primary CTAs should use `.bossa-pill` or inherit the primary button pill treatment.
- Never nest curved containers inside curved containers. Keep repeated data cards sharp with a small left stripe when tone is needed.
