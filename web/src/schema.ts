import { z } from 'zod';

export const SyncStatsSchema = z.object({
  total_contracts: z.preprocess((val) => Number(val), z.number()),
  total_quarantine: z.preprocess((val) => Number(val), z.number()),
  days_on_ia: z.preprocess((val) => Number(val), z.number()),
  // ISO-8601 timestamp of when the snapshot was produced. Optional because
  // the committed JSON predates this field and existing BDD fixtures omit it.
  generated_at: z.string().optional(),
});

export type SyncStats = z.infer<typeof SyncStatsSchema>;
