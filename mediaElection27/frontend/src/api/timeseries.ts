import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { apiGet } from "@/api/client";
import { CandidateSchema } from "@/api/candidates";
import { SourceSchema } from "@/api/sources";

export const TimeseriesPointSchema = z.object({
  day: z.string(), // ISO date
  candidate_id: z.number().int(),
  source_id: z.number().int(),
  n_mentions: z.number().int(),
  n_articles: z.number().int(),
});
export type TimeseriesPoint = z.infer<typeof TimeseriesPointSchema>;

export const TimeseriesResponseSchema = z.object({
  points: z.array(TimeseriesPointSchema),
  candidates: z.array(CandidateSchema),
  sources: z.array(SourceSchema),
  from: z.string(),
  to: z.string(),
  tz: z.string(),
  extractor: z.string(),
  extractor_version: z.string(),
  n_total_mentions: z.number().int(),
});
export type TimeseriesResponse = z.infer<typeof TimeseriesResponseSchema>;

export interface TimeseriesParams {
  candidateIds?: number[];
  sourceIds?: number[];
  from?: string; // YYYY-MM-DD
  to?: string;
  tz?: string;
}

export function useTimeseries(params: TimeseriesParams) {
  const { candidateIds, sourceIds, from, to, tz } = params;
  return useQuery({
    queryKey: ["timeseries", { candidateIds, sourceIds, from, to, tz }],
    queryFn: async () => {
      const data = await apiGet<unknown>("/timeseries", {
        candidate_id: candidateIds,
        source_id: sourceIds,
        from,
        to,
        tz,
      });
      return TimeseriesResponseSchema.parse(data);
    },
  });
}
