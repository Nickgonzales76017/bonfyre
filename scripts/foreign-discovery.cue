foreignDiscovery: {
  max_body_bytes: 8000000
  max_head_bytes: 200000
  max_auth_bytes: 200000
  observation_timeout_ms: 120000
  known_carriers: [
    {path: "/sitemap.xml", kind: "sitemap"},
    {path: "/sitemap_index.xml", kind: "sitemap"},
    {path: "/feed", kind: "feed"},
    {path: "/feed.xml", kind: "feed"},
    {path: "/rss.xml", kind: "feed"},
    {path: "/atom.xml", kind: "feed"},
    {path: "/feed.json", kind: "feed"},
  ]
  descriptors: [
    {path: "/.well-known/webfinger", family: "activitypub-discovery"},
    {path: "/.well-known/nodeinfo", family: "activitypub-discovery"},
    {path: "/.well-known/host-meta", family: "activitypub-discovery"},
    {path: "/.well-known/mcp.json", family: "mcp-descriptor-candidate"},
    {path: "/.well-known/agent.json", family: "a2a-agent-card-candidate"},
    {path: "/.well-known/oauth-protected-resource", family: "oauth-resource-candidate"},
    {path: "/.well-known/oauth-authorization-server", family: "oauth-server-candidate"},
    {path: "/.well-known/openid-configuration", family: "openid-configuration-candidate"},
    {path: "/.well-known/assetlinks.json", family: "android-app-link-candidate"},
    {path: "/.well-known/apple-app-site-association", family: "apple-universal-link-candidate"},
    {path: "/.well-known/ai-plugin.json", family: "ai-plugin-candidate"},
  ]
  carrier_rank: [
    {carrier: "websub", field: "websub_hubs", why: "a hub will push changes, so nothing needs polling"},
    {carrier: "activitypub", field: "activitypub", why: "a federated actor can deliver activities to us"},
    {carrier: "feed", field: "feeds", why: "a feed enumerates changes with stable item identity"},
    {carrier: "sitemap", field: "sitemaps", why: "lastmod says what moved without refetching the site"},
    {carrier: "structured-metadata", field: "structured_types", why: "the pages declare typed entities, so nothing needs to be inferred from prose"},
    {carrier: "conditional-http", field: "conditional", why: "a validator lets a re-read cost headers rather than a body"},
  ]
  descriptor_limit: "A descriptor is a candidate carrier only; it is not a live endpoint, admitted provider, authorized tool, payment route, identity assertion, or effect right."
  auth_limit: "An auth.md file is a candidate registration guide only; it is not an identity assertion, credential, remote account binding, authorized tool, authorization, payment approval, or effect right."
  declined_why: "the source declines automated reading"
  default_why: "nothing machine-legible was declared, so reading this source costs a full fetch and an interpretation; that is the expensive path and it is the last resort, not the default"
  cannot_infer: "a public discovery response does not establish a live endpoint, source truth, provider capability, adoption, identity, or authority"
}
