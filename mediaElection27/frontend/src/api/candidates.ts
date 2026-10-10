import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { apiGet } from "@/api/client";

export const CandidateSchema = z.object({
  id: z.number().int(),
  slug: z.string(),
  display_name: z.string(),
  party: z.string().nullable(),
  lean: z.string().nullable(),
  declared_at: z.string().nullable(),
  eligible: z.boolean(),
  notes: z.string().nullable(),
  n_aliases: z.number().int().nullable(),
});
export type Candidate = z.infer<typeof CandidateSchema>;

export const CandidatesResponseSchema = z.array(CandidateSchema);

export function useCandidates() {
  return useQuery({
    queryKey: ["candidates"],
    queryFn: async () => {
      const data = await apiGet<unknown>("/candidates");
      return CandidatesResponseSchema.parse(data);
    },
    staleTime: 5 * 60_000, // candidates change rarely
  });
}
