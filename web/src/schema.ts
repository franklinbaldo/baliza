import { z } from 'zod';

export const SyncStatsSchema = z.object({
  total_contracts: z.preprocess((val) => Number(val), z.number()),
  total_quarantine: z.preprocess((val) => Number(val), z.number()),
  days_on_ia: z.preprocess((val) => Number(val), z.number()),
});

export type SyncStats = z.infer<typeof SyncStatsSchema>;
