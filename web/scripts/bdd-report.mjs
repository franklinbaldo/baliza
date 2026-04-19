#!/usr/bin/env node
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const featuresRoot = resolve(__dirname, '..', 'features');

const STATUSES = /** @type {const} */ (['green', 'wip', 'planned']);
const JOURNEYS = [1, 2, 3, 4, 5, 6, 7];

function walkFeatures(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) {
      out.push(...walkFeatures(full));
    } else if (entry.endsWith('.feature')) {
      out.push(full);
    }
  }
  return out;
}

function tagsOnLine(line) {
  return [...line.matchAll(/@[A-Za-z][A-Za-z0-9-_]*/g)].map((m) =>
    m[0].slice(1).toLowerCase(),
  );
}

function parseFeature(path) {
  const lines = readFileSync(path, 'utf8').split(/\r?\n/);
  let pendingTags = [];
  let featureTags = [];
  let inFeature = false;
  const scenarios = [];

  for (const raw of lines) {
    const line = raw.trim();
    if (!line) {
      // blank line resets nothing; tags remain attached to next non-blank
      continue;
    }
    if (line.startsWith('#')) {
      continue;
    }
    if (line.startsWith('@')) {
      pendingTags.push(...tagsOnLine(line));
      continue;
    }
    if (line.startsWith('Feature:')) {
      featureTags = pendingTags;
      pendingTags = [];
      inFeature = true;
      continue;
    }
    if (inFeature && (line.startsWith('Scenario:') || line.startsWith('Scenario Outline:'))) {
      scenarios.push({
        tags: [...featureTags, ...pendingTags],
        name: line.replace(/^Scenario(?: Outline)?:\s*/, ''),
      });
      pendingTags = [];
      continue;
    }
    // Any non-tag, non-scenario, non-feature line clears pending tags so they
    // don't leak onto the next scenario.
    pendingTags = [];
  }

  return scenarios;
}

function bucket(scenario) {
  const journeys = scenario.tags
    .filter((t) => /^journey\d+$/.test(t))
    .map((t) => Number(t.replace('journey', '')));
  const status = STATUSES.find((s) => scenario.tags.includes(s)) ?? 'untagged';
  return { journeys, status };
}

const featureFiles = walkFeatures(featuresRoot);
const counts = new Map();
for (const j of JOURNEYS) {
  counts.set(j, { green: 0, wip: 0, planned: 0, untagged: 0 });
}
const totals = { green: 0, wip: 0, planned: 0, untagged: 0 };
let unattributed = 0;

for (const file of featureFiles) {
  for (const scenario of parseFeature(file)) {
    const { journeys, status } = bucket(scenario);
    if (status in totals) totals[status]++;
    if (journeys.length === 0) {
      unattributed++;
      continue;
    }
    for (const j of journeys) {
      const row = counts.get(j);
      if (row && status in row) row[status]++;
    }
  }
}

const grandTotal = totals.green + totals.wip + totals.planned + totals.untagged;
const greenPct = grandTotal > 0 ? ((totals.green / grandTotal) * 100).toFixed(1) : '0.0';

const lines = [];
lines.push('# Baliza BDD coverage report');
lines.push('');
lines.push(`Source: ${relative(resolve(__dirname, '..'), featuresRoot)}`);
lines.push(`Total scenarios: ${grandTotal} (green ${totals.green}, wip ${totals.wip}, planned ${totals.planned}, untagged ${totals.untagged})`);
lines.push(`Green ratio: ${greenPct}%`);
lines.push('');
lines.push('| Journey | @green | @wip | @planned | Total |');
lines.push('|---------|-------:|-----:|---------:|------:|');
for (const j of JOURNEYS) {
  const row = counts.get(j);
  const total = row.green + row.wip + row.planned + row.untagged;
  lines.push(`| journey${j} | ${row.green} | ${row.wip} | ${row.planned} | ${total} |`);
}
lines.push('');
if (unattributed > 0) {
  lines.push(`Note: ${unattributed} scenarios are not attributed to any journey tag.`);
}

process.stdout.write(lines.join('\n') + '\n');
