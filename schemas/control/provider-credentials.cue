package control

// Credentials a provider needs before this estate can read from it, declared
// so the private credentials page can say exactly what is required, where a
// person gets it, what the estate can generate for itself, how the credential
// is applied to a request, and how it is verified -- without the page or any
// agent ever showing a stored secret again.
//
// This is the dedicated credential boundary #ExternalAuthIntent points at
// (external-descriptor.cue).  A stored credential is possession, never
// authority: it does not identify an Aurekai actor, admit a provider, approve
// spend or grant an effect.  cmd/BonfyreCredentials seals every value with
// AES-256-GCM under a key file only the service user can read, serves its page
// on loopback only, and writes no value into any receipt, log or answer.
//
// Templates: {field} is a stored field, {epoch} the request time in seconds,
// {sha1:a+b+epoch} the SHA-1 hex of fields and the epoch concatenated in order,
// {basic:a+b} base64 of "a:b".  A provider imported from an OpenAPI document
// is declared the same way from its securitySchemes at import time.

#CredentialField: {
    field: string & =~"^[a-z][a-z0-9_]*$"
    label: string & != ""
    kind: "api_key" | "secret" | "client_id" | "client_secret" | "account_id" | "token" | "username" | "password" |
          "webhook_secret" | "url" | "zone_id" | "project_id" | "organization_id" | "signing_key"
    sensitive: bool
    required: bool
    // The estate can make this value itself; a person never has to invent it.
    generator: *"" | "random-hex-32" | "random-base64url-32" | "uuid"
    // A value the provider itself publishes for anyone (a documented demo
    // key), used until a person sets their own.
    public_default?: string & != ""
    help: string & != ""
}

#CredentialHeader: { name: string & =~"^[A-Za-z0-9-]+$", value: string & != "" }
#CredentialQuery: { name: string & =~"^[A-Za-z0-9_.-]+$", value: string & != "" }

#CredentialProvider: {
    provider: string & =~"^[a-z0-9-]+$"
    name: string & != ""
    purpose: string & != ""
    get_it: string & =~"^https://"
    terms: *"" | =~"^https://"
    fields: [#CredentialField, ...#CredentialField]
    apply: {
        headers: *[] | [...#CredentialHeader]
        query: *[] | [...#CredentialQuery]
    }
    verify: *{} | {url: string & =~"^https://", expect: int & >=200 & <300}
    // A client-credentials flow the boundary exchanges itself; {oauth_token}
    // in a header template is the exchanged token.
    oauth?: {token_url: string & =~"^https://", client_id_field: string, client_secret_field: string, scope?: string}
    // Values this estate gives the provider (a callback or webhook address), shown next to the fields.
    tell_them: *[] | [...{label: string, value: string}]
    passports: *[] | [...string]
    cannot_infer: string & != ""
}

nativeCredentialProviders: [...#CredentialProvider] & [
    {
        provider: "podcast-index", name: "Podcast Index"
        purpose: "Discovers podcasts and new episodes by show identity for Podcasts and Listen."
        get_it: "https://api.podcastindex.org/signup", terms: "https://podcastindex.org/"
        fields: [
            {field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, help: "Shown on the Podcast Index developer page after sign-up."},
            {field: "api_secret", label: "API secret", kind: "secret", sensitive: true, required: true, help: "Shown once with the key; it signs every request."},
        ]
        apply: headers: [
            {name: "X-Auth-Key", value: "{api_key}"},
            {name: "X-Auth-Date", value: "{epoch}"},
            {name: "Authorization", value: "{sha1:api_key+api_secret+epoch}"},
        ]
        verify: {url: "https://api.podcastindex.org/api/1.0/recent/episodes?max=1", expect: 200}
        passports: ["podcast-index"]
        cannot_infer: "a working key is access to the index, not an endorsement of any show"
    },
    {
        provider: "acoustid", name: "AcoustID"
        purpose: "Identifies a recording from its acoustic fingerprint and joins it to MusicBrainz."
        get_it: "https://acoustid.org/new-application", terms: "https://acoustid.org/webservice"
        fields: [{field: "client_key", label: "Application API key", kind: "api_key", sensitive: true, required: true, help: "Register an application; the free service is for non-commercial use."}]
        apply: query: [{name: "client", value: "{client_key}"}]
        verify: {url: "https://api.acoustid.org/v2/lookup?format=json&trackid=9ff43b6a-4f16-427c-93c2-92307ca505e0", expect: 200}
        passports: ["acoustid"]
        cannot_infer: "a fingerprint match is acoustic similarity, not the same performance or a right to use it"
    },
    {
        provider: "opencorporates", name: "OpenCorporates"
        purpose: "Resolves companies, officers and filings for company pages and property work."
        get_it: "https://opencorporates.com/api_accounts/new", terms: "https://opencorporates.com/legal/licence"
        fields: [{field: "api_token", label: "API token", kind: "token", sensitive: true, required: true, help: "Issued with an API account; open-data use is share-alike."}]
        apply: query: [{name: "api_token", value: "{api_token}"}]
        verify: {url: "https://api.opencorporates.com/v0.4/companies/search?q=bonfyre&per_page=1", expect: 200}
        passports: ["opencorporates"]
        cannot_infer: "a company record is not a statement that the company operates, owns anything or can be contacted"
    },
    {
        provider: "fred", name: "FRED (St. Louis Fed)"
        purpose: "Metro economic series (employment, prices, housing) as context on places and properties."
        get_it: "https://fred.stlouisfed.org/docs/api/api_key.html", terms: "https://fred.stlouisfed.org/legal/"
        fields: [{field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, help: "Request a key from your FRED account page."}]
        apply: query: [{name: "api_key", value: "{api_key}"}]
        verify: {url: "https://api.stlouisfed.org/fred/series?series_id=GNPCA&file_type=json", expect: 200}
        passports: ["fred"]
        cannot_infer: "a metro series says nothing about any one property or person"
    },
    {
        provider: "api-data-gov", name: "api.data.gov (NASA, Smithsonian and other federal APIs)"
        purpose: "Raises the rate limits of NASA's picture of the day, the Smithsonian open collection and other federal sources beyond the shared demo key."
        get_it: "https://api.data.gov/signup/", terms: "https://api.data.gov/"
        fields: [{field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, public_default: "DEMO_KEY", help: "Emailed immediately after sign-up. Until then the documented DEMO_KEY is used, at its lower hourly limit."}]
        apply: query: [{name: "api_key", value: "{api_key}"}]
        verify: {url: "https://api.nasa.gov/planetary/apod", expect: 200}
        cannot_infer: "a key is a rate allowance, not federal endorsement"
    },
    {
        provider: "europeana", name: "Europeana"
        purpose: "Cultural heritage pictures and objects from European museums, with per-item rights."
        get_it: "https://pro.europeana.eu/page/get-api", terms: "https://pro.europeana.eu/page/terms-of-use"
        fields: [{field: "wskey", label: "API key (wskey)", kind: "api_key", sensitive: true, required: true, help: "Requested from Europeana Pro; delivered by email."}]
        apply: query: [{name: "wskey", value: "{wskey}"}]
        verify: {url: "https://api.europeana.eu/record/v2/search.json?query=dallas&rows=1", expect: 200}
        cannot_infer: "an object's listed rights are the providing institution's statement"
    },
    {
        provider: "youtube-data", name: "YouTube Data API"
        purpose: "Observes public video metadata (titles, channels, statistics) for publications and creators."
        get_it: "https://console.cloud.google.com/apis/credentials", terms: "https://developers.google.com/youtube/terms/api-services-terms-of-service"
        fields: [{field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, help: "Create an API key in a Google Cloud project with the YouTube Data API enabled."}]
        apply: query: [{name: "key", value: "{api_key}"}]
        verify: {url: "https://www.googleapis.com/youtube/v3/videos?part=id&chart=mostPopular&maxResults=1", expect: 200}
        cannot_infer: "public metadata is not a licence to download, re-host or transform a video"
    },
    {
        provider: "openai", name: "OpenAI API"
        purpose: "Model calls routed through the provider frontier when a task's capability allocation selects OpenAI."
        get_it: "https://platform.openai.com/api-keys", terms: "https://openai.com/policies/terms-of-use"
        fields: [
            {field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, help: "A project key is preferred over a user key."},
            {field: "organization_id", label: "Organization ID", kind: "organization_id", sensitive: false, required: false, help: "Only if the key belongs to several organizations."},
            {field: "project_id", label: "Project ID", kind: "project_id", sensitive: false, required: false, help: "Only if requests should be attributed to a specific project."},
        ]
        apply: headers: [{name: "Authorization", value: "Bearer {api_key}"}]
        verify: {url: "https://api.openai.com/v1/models", expect: 200}
        cannot_infer: "a key is spend capacity, never a spend approval"
    },
    {
        provider: "anthropic", name: "Anthropic API"
        purpose: "Model calls routed through the provider frontier when a task's capability allocation selects Claude."
        get_it: "https://console.anthropic.com/settings/keys", terms: "https://www.anthropic.com/legal/commercial-terms"
        fields: [{field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, help: "Created in the console under API keys."}]
        apply: headers: [{name: "x-api-key", value: "{api_key}"}, {name: "anthropic-version", value: "2023-06-01"}]
        verify: {url: "https://api.anthropic.com/v1/models", expect: 200}
        cannot_infer: "a key is spend capacity, never a spend approval"
    },
    {
        provider: "mistral", name: "Mistral API"
        purpose: "Publication embeddings and model calls already measured in the provider state fold."
        get_it: "https://console.mistral.ai/api-keys", terms: "https://mistral.ai/terms/"
        fields: [{field: "api_key", label: "API key", kind: "api_key", sensitive: true, required: true, help: "Created in La Plateforme under API keys."}]
        apply: headers: [{name: "Authorization", value: "Bearer {api_key}"}]
        verify: {url: "https://api.mistral.ai/v1/models", expect: 200}
        cannot_infer: "a key is spend capacity, never a spend approval"
    },
    {
        provider: "github", name: "GitHub"
        purpose: "Higher-rate reads of releases, repositories and advisories for Releases and Advisories."
        get_it: "https://github.com/settings/personal-access-tokens/new", terms: "https://docs.github.com/site-policy/github-terms/github-terms-of-service"
        fields: [{field: "token", label: "Fine-grained token (read-only, public repositories)", kind: "token", sensitive: true, required: true, help: "Grant only public repository read access."}]
        apply: headers: [{name: "Authorization", value: "Bearer {token}"}]
        verify: {url: "https://api.github.com/rate_limit", expect: 200}
        cannot_infer: "a token's reach is what it was scoped to, not what the estate may change"
    },
    {
        provider: "cloudflare", name: "Cloudflare"
        purpose: "Observing and deliberately publishing the owned domains' DNS and tunnel configuration."
        get_it: "https://dash.cloudflare.com/profile/api-tokens", terms: "https://www.cloudflare.com/terms/"
        fields: [
            {field: "api_token", label: "API token", kind: "token", sensitive: true, required: true, help: "Scope it to the zones Aurekai owns, DNS read and edit only."},
            {field: "account_id", label: "Account ID", kind: "account_id", sensitive: false, required: true, help: "Shown on the account overview page."},
            {field: "zone_id", label: "Zone ID", kind: "zone_id", sensitive: false, required: false, help: "Shown on the zone overview page."},
        ]
        apply: headers: [{name: "Authorization", value: "Bearer {api_token}"}]
        verify: {url: "https://api.cloudflare.com/client/v4/user/tokens/verify", expect: 200}
        cannot_infer: "possession of a domain credential is not authority over a subject, institution or publication (DomainControl)"
    },
    {
        provider: "stripe", name: "Stripe"
        purpose: "Payment and settlement observation for offers; every effect still needs its own approval."
        get_it: "https://dashboard.stripe.com/apikeys", terms: "https://stripe.com/legal/ssa"
        fields: [
            {field: "secret_key", label: "Restricted secret key", kind: "secret", sensitive: true, required: true, help: "Use a restricted key with read access unless an approved effect needs more."},
            {field: "publishable_key", label: "Publishable key", kind: "api_key", sensitive: false, required: false, help: "Safe to appear in a page; stored here so it has one source."},
            {field: "account_id", label: "Account ID", kind: "account_id", sensitive: false, required: false, help: "acct_... from the dashboard's account settings."},
            {field: "webhook_secret", label: "Webhook signing secret", kind: "webhook_secret", sensitive: true, required: false, help: "Stripe creates this when you add the endpoint below; paste it here."},
        ]
        apply: headers: [{name: "Authorization", value: "Bearer {secret_key}"}]
        verify: {url: "https://api.stripe.com/v1/balance", expect: 200}
        tell_them: [{label: "Webhook endpoint to add in Stripe", value: "https://aurekai.work/hooks/stripe"}]
        cannot_infer: "a payment credential never approves a payment; FinancialMembrane and a person do"
    },
    {
        provider: "aurekai-webhooks", name: "Aurekai webhook signing"
        purpose: "Signs what Aurekai sends to other services and verifies what they send back."
        get_it: "https://aurekai.work/"
        fields: [
            {field: "signing_secret", label: "Signing secret", kind: "signing_key", sensitive: true, required: true, generator: "random-base64url-32", help: "Generated here; give it to a receiving service once."},
        ]
        apply: headers: []
        cannot_infer: "a valid signature says who sent a message, not that what it says is true"
    },
    {
        provider: "aurekai-api-client", name: "Aurekai API client"
        purpose: "Credentials for a service or script that calls Aurekai's own API on the operator's behalf."
        get_it: "https://aurekai.work/"
        fields: [
            {field: "client_id", label: "Client ID", kind: "client_id", sensitive: false, required: true, generator: "uuid", help: "Generated here."},
            {field: "client_secret", label: "Client secret", kind: "client_secret", sensitive: true, required: true, generator: "random-base64url-32", help: "Generated here and shown once."},
        ]
        apply: headers: [{name: "Authorization", value: "Basic {basic:client_id+client_secret}"}]
        cannot_infer: "a client credential identifies a caller; every request still recompiles its authority"
    },
]
