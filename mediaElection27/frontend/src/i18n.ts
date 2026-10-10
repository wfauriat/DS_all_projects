/** Single-locale (fr-FR) string table. Keep keys descriptive; values can stay
 *  inline if a refactor to a real i18n library happens later. */

export const t = {
  app: {
    title: "mediaElection27",
    subtitle: "Couverture média de la présidentielle 2027",
  },
  nav: {
    dashboard: "Tendances",
    leaderboard: "Classement",
    share: "Part de voix",
    sources: "Par média",
    articles: "Articles",
  },
  filters: {
    candidates: "Candidats",
    selectAll: "Tout sélectionner",
    clearAll: "Tout désélectionner",
    allArticles: "Tous les articles ingérés",
    allArticlesHint: "Inclut les articles sans mention de candidat",
    noMentionHint: "Filtre actif : uniquement les articles sans aucune mention.",
    period: "Période",
    from: "Du",
    to: "Au",
    eligible: "Éligibles",
    ineligible: "Inéligibles / suivis",
  },
  chart: {
    title: "Mentions par jour",
    yAxisLabel: "Nombre de mentions",
    noData: "Aucune mention dans cette période. Élargissez la fenêtre temporelle ou sélectionnez d'autres candidats.",
    loading: "Chargement…",
  },
  stats: {
    totalMentions: "Mentions totales",
    articles: "Articles",
    activeSources: "Médias actifs",
    period: "Période sélectionnée",
  },
  share: {
    title: "Part de voix par jour",
    subtitle: "Pourcentage de mentions par candidat, ramené à 100 % chaque jour",
    tooltipTotal: (n: number) => `${n.toLocaleString("fr-FR")} mentions au total`,
  },
  sources: {
    title: "Drilldown par média",
    subtitle: "Mentions par candidat à l'intérieur d'un seul média",
    filterTitle: "Média",
    chartTitle: (outlet: string) => `Mentions chez ${outlet}`,
    pickPrompt: "Sélectionnez un média dans le panneau de gauche.",
    allLabel: "Tous les médias",
  },
  articles: {
    title: "Articles",
    subtitle: "Articles ingérés correspondant aux filtres",
    col: {
      published: "Date",
      title: "Titre",
      outlet: "Média",
      candidates: "Candidats",
    },
    count: (total: number) => `${total.toLocaleString("fr-FR")} articles`,
    pageOf: (from: number, to: number, total: number) =>
      `${from.toLocaleString("fr-FR")}–${to.toLocaleString("fr-FR")} sur ${total.toLocaleString("fr-FR")}`,
    prev: "Précédent",
    next: "Suivant",
    open: "Ouvrir l'article",
    empty: "Aucun article ne correspond aux filtres. Élargissez la fenêtre ou changez de candidat/média.",
  },
  leaderboard: {
    title: "Classement des candidats",
    subtitle: "Trié par nombre de mentions sur la période sélectionnée",
    col: {
      candidate: "Candidat",
      totalMentions: "Mentions",
      outlets: "Médias",
      latest: "Dernière mention",
      trend: "Tendance",
    },
    ineligibleBadge: "Inéligible",
  },
  errors: {
    apiUnreachable: "API indisponible. Vérifiez que `make api` est en cours d'exécution.",
    parseError: "Réponse de l'API invalide.",
    unknown: "Une erreur s'est produite.",
  },
  footer: {
    methodology:
      "Données : flux RSS publics. Mentions extraites par appariement par mots-clés (extracteur keyword v1).",
    sourceCode: "Code source",
  },
} as const;
