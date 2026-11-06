providerRefresh: {
  agent: "BonfyreProviderFrontier/2.0 (provider refresh; aurekai.life)"
  timeout_ms: 25000
  successful_statuses: [200, 201, 202, 204, 404]
  refused_statuses: [401, 403]
  auth: {
    bearer: {header: "Authorization", prefix: "Bearer "}
    token: {header: "Authorization", prefix: "token "}
    "x-api-key": {header: "x-api-key", prefix: "", additional: {"anthropic-version": "2023-06-01"}}
    query: {parameter: "key"}
  }
  cloudflare: {
    token_env: "CF_TOKEN"
    account_env: "CF_ACCT"
    credential_label: "CLOUDFLARE_info"
    base: "https://api.cloudflare.com/client/v4/accounts/"
    probes: [
      {provider: "cloudflare-workers-ai", plane: "edge-inference", path: "/ai/models/search?per_page=1"},
      {provider: "cloudflare-vectorize", plane: "vector", path: "/vectorize/v2/indexes"},
      {provider: "cloudflare-queues", plane: "queue", path: "/queues"},
      {provider: "cloudflare-r2", plane: "artifact-store", path: "/r2/buckets"},
      {provider: "cloudflare-d1", plane: "data", path: "/d1/database"},
      {provider: "cloudflare-workflows", plane: "workflow", path: "/workflows"},
      {provider: "cloudflare-pages", plane: "surface", path: "/pages/projects?per_page=1"},
      {provider: "cloudflare-workers", plane: "edge-runtime", path: "/workers/scripts"},
      {provider: "cloudflare-kv", plane: "state", path: "/storage/kv/namespaces"},
    ]
  }
  endpoints: [
    {provider: "openai", plane: "cloud-llm", url: "https://api.openai.com/v1/models", key_env: "OPENAI_API_KEY", auth: "bearer"},
    {provider: "anthropic", plane: "cloud-llm", url: "https://api.anthropic.com/v1/models", key_env: "ANTHROPIC_API_KEY", auth: "x-api-key"},
    {provider: "gemini", plane: "cloud-llm", url: "https://generativelanguage.googleapis.com/v1beta/models", key_env: "GEMINI_API_KEY", auth: "query"},
    {provider: "groq", plane: "cloud-llm", url: "https://api.groq.com/openai/v1/models", key_env: "GROQ_API_KEY", auth: "bearer"},
    {provider: "perplexity", plane: "cloud-llm", url: "https://api.perplexity.ai/chat/completions", key_env: "PERPLEXITY_API_KEY", auth: "bearer"},
    {provider: "mistral", plane: "cloud-llm", url: "https://api.mistral.ai/v1/models", key_env: "MISTRAL_API_KEY", auth: "bearer"},
    {provider: "xai", plane: "cloud-llm", url: "https://api.x.ai/v1/models", key_env: "XAI_API_KEY", auth: "bearer"},
    {provider: "cohere", plane: "cloud-llm", url: "https://api.cohere.com/v1/models", key_env: "COHERE_API_KEY", auth: "bearer"},
    {provider: "deepseek", plane: "cloud-llm", url: "https://api.deepseek.com/models", key_env: "DEEPSEEK_API_KEY", auth: "bearer"},
    {provider: "openrouter", plane: "meta-router", url: "https://openrouter.ai/api/v1/models", key_env: "OPENROUTER_API_KEY", auth: "bearer"},
    {provider: "together-ai", plane: "cloud-inference", url: "https://api.together.xyz/v1/models", key_env: "TOGETHER_API_KEY", auth: "bearer"},
    {provider: "fireworks-ai", plane: "cloud-inference", url: "https://api.fireworks.ai/inference/v1/models", key_env: "FIREWORKS_API_KEY", auth: "bearer"},
    {provider: "cerebras", plane: "cloud-inference", url: "https://api.cerebras.ai/v1/models", key_env: "CEREBRAS_API_KEY", auth: "bearer"},
    {provider: "deepinfra", plane: "cloud-inference", url: "https://api.deepinfra.com/v1/openai/models", key_env: "DEEPINFRA_API_KEY", auth: "bearer"},
    {provider: "replicate", plane: "cloud-inference", url: "https://api.replicate.com/v1/models", key_env: "REPLICATE_API_TOKEN", auth: "bearer"},
    {provider: "baseten", plane: "cloud-inference", url: "https://api.baseten.co/v1/models", key_env: "BASETEN_API_KEY", auth: "bearer"},
    {provider: "modal", plane: "compute-inference", url: "https://api.modal.com/v1/apps", key_env: "MODAL_TOKEN_ID", auth: "bearer"},
    {provider: "fal-ai", plane: "media-inference", url: "https://fal.run/health", key_env: "FAL_KEY", auth: "bearer"},
    {provider: "novita", plane: "cloud-inference", url: "https://api.novita.ai/v3/openai/models", key_env: "NOVITA_API_KEY", auth: "bearer"},
    {provider: "huggingface-inference-providers", plane: "meta-router", url: "https://huggingface.co/api/whoami-v2", key_env: "HF_TOKEN", auth: "bearer"},
    {provider: "supabase", plane: "data", key_env: "SUPABASE_ACCESS_TOKEN", auth: "bearer"},
    {provider: "neon", plane: "data", url: "https://console.neon.tech/api/v2/projects", key_env: "NEON_API_KEY", auth: "bearer"},
    {provider: "turso", plane: "data", url: "https://api.turso.tech/v1/organizations", key_env: "TURSO_API_TOKEN", auth: "bearer"},
    {provider: "upstash", plane: "state", key_env: "UPSTASH_REDIS_REST_TOKEN", auth: "bearer"},
    {provider: "render", plane: "compute", url: "https://api.render.com/v1/owners?limit=1", key_env: "RENDER_API_KEY", auth: "bearer"},
    {provider: "vercel", plane: "surface", url: "https://api.vercel.com/v2/user", key_env: "VERCEL_TOKEN", auth: "bearer"},
    {provider: "railway", plane: "compute", key_env: "RAILWAY_TOKEN", auth: "bearer"},
    {provider: "fly", plane: "compute", key_env: "FLY_API_TOKEN", auth: "bearer"},
    {provider: "github", plane: "tooling", url: "https://api.github.com/user", key_env: "GITHUB_PAT", auth: "token"},
    {provider: "firecrawl", plane: "ingest", url: "https://api.firecrawl.dev/v1/team/credit-usage", key_env: "FIRECRAWL_API_KEY", auth: "bearer"},
    {provider: "posthog", plane: "telemetry", url: "https://us.posthog.com/api/projects/", key_env: "POSTHOG_API_KEY", auth: "bearer"},
    {provider: "sentry", plane: "telemetry", url: "https://sentry.io/api/0/organizations/", key_env: "SENTRY_AUTH_TOKEN", auth: "bearer"},
  ]
  local: {
    hvm4: "runtime", egglog: "rewrite", verus: "proof", burn: "inference",
    tract: "inference", lance: "vector", daft: "data", yaff: "compiler",
    automerge: "state", feldera: "stream", hydro: "placement", cubecl: "gpu",
    gigatoken: "compression", restate: "workflow", "llama-cpp": "local-inference",
    vllm: "local-inference", sglang: "local-inference", tgi: "local-inference",
    mlx: "local-inference", "onnx-runtime": "local-inference", litellm: "meta-router",
    "ollama-local": "local-llm", "agent-deck": "agent-execution", cavemem: "memory",
    bernstein: "workflow", rtk: "tooling",
  }
  local_reason: "a library or local runtime, not an endpoint that says who you are"
  unconfigured_reason: "no independently probeable endpoint has been declared"
  cannot_infer: "reachable means a provider answered as itself with the credential this estate holds; it is not capability, not pricing, not quota, and not permission to spend anything"
}
