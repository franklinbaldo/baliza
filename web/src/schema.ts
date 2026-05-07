import { z } from 'zod';

const ResourceStatsSchema = z.object({
  row_count: z.number(),
  quarantine_count: z.number(),
  partition_count: z.number(),
  days_on_ia: z.number(),
});

export type ResourceStats = z.infer<typeof ResourceStatsSchema>;

export const SyncStatsSchema = z.object({
  total_contracts: z.preprocess((val) => Number(val), z.number()),
  total_quarantine: z.preprocess((val) => Number(val), z.number()),
  days_on_ia: z.preprocess((val) => Number(val), z.number()),
  // ISO-8601 timestamp of when the snapshot was produced. Optional because
  // the committed JSON predates this field and existing BDD fixtures omit it.
  generated_at: z.string().optional(),
  // Per-resource breakdown produced by build_sync_stats.py. Optional so the
  // committed zero-baseline (which predates this field) still validates.
  resources: z.record(z.string(), ResourceStatsSchema).optional(),
});

export type SyncStats = z.infer<typeof SyncStatsSchema>;

