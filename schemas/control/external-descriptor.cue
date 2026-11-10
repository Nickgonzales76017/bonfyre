// SourcePassport validates an observed foreign publishing descriptor before it
// can join the cheap-candidate frontier. It deliberately says nothing about
// endpoint liveness, provider admission, credentials, payment, or authority:
// those are later owners and cannot be inferred from public JSON.
package control

#ProtocolDescriptor: {
	family: "activitypub-discovery" | "mcp-descriptor-candidate" |
		"a2a-agent-card-candidate" | "oauth-resource-candidate" |
		"oauth-server-candidate" | "openid-configuration-candidate" |
		"auth-md-candidate" |
		"android-app-link-candidate" | "apple-universal-link-candidate" |
		"ai-plugin-candidate"
	descriptor: =~"^https?://"
	truth_level: "declared_descriptor"
	limit: string
}

#SourcePassport: {
	root: =~"^https?://"
	robots_allows_us: bool
	best_carrier: "none" | "websub" | "activitypub" | "feed" | "sitemap" |
		"structured-metadata" | "conditional-http" | "html-extraction"
	protocol_descriptors: [...#ProtocolDescriptor]
}

// This is deliberately an intent envelope, not a credential or token store.
// A public OAuth/OIDC declaration can nominate a future external-auth ceremony;
// it cannot identify an Aurekai actor, bind a remote account, or authorize a
// remote effect. Credentials stay with a dedicated credential/effect boundary.
#ExternalAuthIntent: {
	descriptor: #ProtocolDescriptor & {
		family: "oauth-resource-candidate" | "oauth-server-candidate" |
			"openid-configuration-candidate"
	}
	// auth.md may explain an agent-registration ceremony, but it cannot supply
	// the authorization-server metadata or independently start that ceremony.
	registration_guide?: #ProtocolDescriptor & {
		family: "auth-md-candidate"
	}
	actor_closure: string
	institution_closure?: string
	purpose: string
	redirect_address: =~"^https://"
	flow: "authorization-code-pkce" | "device-authorization"
	credential_state: "not_started"
	limit: "An auth intent is not an external identity, remote account binding, credential, provider admission, remote authorization, payment approval, or effect right; auth.md is optional guidance only and cannot authorize the intent."
}
