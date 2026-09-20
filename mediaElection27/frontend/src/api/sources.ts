import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { apiGet } from "@/api/client";

export const SourceSchema = z.object({
  id: z.number().int(),
  slug: z.string(),
  outlet: z.string(),
  section: z.string().nullable(),
  feed_url: z.string(),
  lean: z.string().nullable(),
  is_active: z.boolean(),
});
export type Source = z.infer<typeof SourceSchema>;

export const SourcesResponseSchema = z.array(SourceSchema);

export function useSources(includeInactive = false) {
  return useQuery({
    queryKey: ["sources", { includeInactive }],
    queryFn: async () => {
      const data = await apiGet<unknown>("/sources", {
        include_inactive: includeInactive,
      });
      return SourcesResponseSchema.parse(data);
    },
    staleTime: 5 * 60_000,
  });
}
