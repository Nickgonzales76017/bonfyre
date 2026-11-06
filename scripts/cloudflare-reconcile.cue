package cloudflarereconcile

// Auth is a distinct sibling of the OAuth issuer on the work territory.
tunnelID: "495b264d-9a9d-4676-a5a7-e2ae6b45c1a3"
nativeService: "http://127.0.0.1:28901"
// The OAuth authority bridge answers these paths on the apex that existing
// connectors hold as issuer and resource. On the Oracle estate nginx made this
// split; behind the tunnel the tunnel has to, or the address organ answers 404.
gateway: {
  host:    "aurekai.work"
  service: "http://127.0.0.1:8791"
  path:    "^/(oauth/(authorize|token)|mcp|auth\\.md|\\.well-known/(oauth-authorization-server|openid-configuration|oauth-protected-resource/mcp))$"
}
territories: [
  {root: "aurekai.life", member: "auth.aurekai.life"},
  {root: "aurekai.live", member: "auth.aurekai.live"},
  {root: "aurekai.shop", member: "auth.aurekai.shop"},
  {root: "aurekai.work", member: "signin.aurekai.work"},
]
additionalTunnelHosts: ["theleaflounge.store"]
// Canonical machine-control hosts. The placement plane requires both to bypass
// the broad estate Worker and reach the native gateway through the tunnel.
controlHosts: [
  {root: "aurekai.work", member: "mcp.aurekai.work"},
  {root: "aurekai.work", member: "auth.aurekai.work"},
]
legacyMemberPaths: ["/_surface*", "/login*", "/signup*", "/logout*", "/account*", "/saved*", "/studio*", "/cms*"]

// Observed publisher-host login redirects should not spend Worker invocations.
// Start with the measured high-volume territory. Auth hosts and POSTs stay native.
memberReadRedirects: [{root: "aurekai.life", paths: ["/login"]}]
