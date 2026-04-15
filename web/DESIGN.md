# Baliza Design Constitution

This document adapts the "Brazilian Modernism" and "Data-First" philosophy for the Baliza PNCP data pipeline project.

## Principles

**1. Build for analysts and oversight, not for impressions.**
The page exists to help someone monitor, find, compare, and audit public procurement data from the PNCP. Not to look "modern" just for the sake of it.

**2. Content is the interface.**
Do not treat data as filler. The tables, numbers, manifest logs, and structural hierarchy *are* the design.

**3. Density is good when structure is strong.**
Do not fear information-rich dashboards. A dense page with a clear visual hierarchy is far better than a sparse page with hidden metrics.

**4. Legibility beats style.**
Readable text, razor-sharp numbers (using `JetBrains Mono` for metrics), and sane contrast. If a visual choice hurts data reading, it loses.

**5. Speed is part of aesthetics.**
Fast pages feel intelligent. Slow pages feel sloppy. We fetch static manifests and let the client do the work instantly.

**6. HTML first, CSS second, JavaScript last.**
We use Astro to generate static shells. Svelte is reserved only for interactive data islands (Heatmaps, Live Tables).

**7. Make links do real work.**
Use links to connect directly to the raw Internet Archive ZIP files and Parquet datasets for transparency.

**8. Prefer permanence over novelty.**
Stable URLs and timeless layouts. A dashboard should age like a reliable terminal interface.

**9. Expose structure instead of hiding it.**
Show quarantine counts, sync timestamps, and exact file paths. Users should feel the underlying order of the data pipeline.

**10. Truthful metrics over vanity.**
Display the exact amount of data successfully synced vs. quarantined without hiding failures behind happy-path animations.

---

## Practical defaults

- **Use typography for hierarchy:** Size, weight, spacing, and font-family (`DM Serif Display` for headers, `Inter` for body, `Mono` for data).
- **Keep navigation stable:** Predictable layouts across the dashboard.
- **Design durable documents:** Someone opening the dashboard two years from now should still immediately grasp the data flow.
