package control

machineReadership: {
    families: [
        {name: "ai-training-or-retrieval", pattern: "gptbot|claudebot|anthropic|perplexitybot|ccbot|bytespider|amazonbot|applebot-extended|meta-externalagent|google-extended"},
        {name: "search", pattern: "googlebot|bingbot|applebot|duckduckbot|yandexbot|baiduspider"},
        {name: "social-preview", pattern: "twitterbot|facebookexternalhit|linkedinbot|slackbot|discordbot|pinterest|whatsapp|telegrambot"},
        {name: "seo-analytics", pattern: "ahrefsbot|semrushbot|mj12bot|dotbot|screamingfrog"},
        {name: "archive", pattern: "ia_archiver|archive\\.org_bot|wayback"},
        {name: "feed", pattern: "feedly|feedfetcher|rssbot|superfeedr"},
        {name: "monitoring", pattern: "uptimerobot|pingdom|statuscake|datadog"},
    ]
    discovery_prefixes: ["/robots.txt", "/sitemap", "/feed", "/.well-known"]
    cannot_infer: "a user agent is a claim, not a credential; access logs cannot establish actor identity, rights, representation value, or authority"
}
