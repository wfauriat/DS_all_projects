import { useQuery } from "@tanstack/react-query";
import { z } from "zod";

import { apiGet } from "@/api/client";

export const ArticleSchema = z.object({
  id: z.number().int(),
  source_id: z.number().int(),
  outlet: z.string().nullable(),
  title: z.string(),
  summary: z.string().nullable(),
  url: z.string(),
  published_at: z.string(),
  fetched_at: z.string(),
  candidate_ids: z.array(z.number().int()).default([]),
});
export type Article = z.infer<typeof ArticleSchema>;

export const ArticleListSchema = z.object({
  items: z.array(ArticleSchema),
  total: z.number().int(),
  limit: z.number().int(),
  offset: z.number().int(),
});
export type ArticleList = z.infer<typeof ArticleListSchema>;

export interface ArticlesParams {
  candidateIds?: number[];
  hasMention?: boolean;
  sourceIds?: number[];
  from?: string;
  to?: string;
  limit?: number;
  offset?: number;
}

export function useArticles(params: ArticlesParams) {
  const { candidateIds, hasMention, sourceIds, from, to, limit = 20, offset = 0 } = params;
  return useQuery({
    queryKey: ["articles", { candidateIds, hasMention, sourceIds, from, to, limit, offset }],
    queryFn: async () => {
      const data = await apiGet<unknown>("/articles", {
        candidate_id: candidateIds,
        has_mention: hasMention,
        source_id: sourceIds,
        from,
        to,
        limit,
        offset,
      });
      return ArticleListSchema.parse(data);
    },
  });
}
