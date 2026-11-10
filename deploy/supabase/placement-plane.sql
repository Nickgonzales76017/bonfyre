-- Generated from schemas/control/placement-plane.cue with:
-- cue export schemas/control -e placementPlane --out json | bun tools/compile_placement_plane.mjs --input /dev/stdin --out /tmp/placement
-- Kept as a reviewable migration input; never place private signing keys here.
create table if not exists placement_generation (
  generation text primary key,
  declaration_digest text not null,
  compiled_at timestamptz not null default now(),
  frontier text not null,
  active boolean not null default false
);

create table if not exists placement_service (
  generation text not null references placement_generation(generation) on delete cascade,
  service_id text not null,
  semantic_owner text not null,
  primary_provider text not null,
  demand jsonb not null,
  public_origin text,
  primary key (generation, service_id)
);

create table if not exists placement_receipt (
  receipt_id text primary key,
  generation text not null references placement_generation(generation),
  service_id text not null,
  provider text not null,
  outcome text not null check (outcome in ('planned','applied','healthy','degraded','retired')),
  observed_at timestamptz not null default now(),
  evidence_ref text not null,
  detail jsonb not null default '{}'::jsonb
);

create table if not exists oauth_authorization_transaction (
  code_digest text primary key,
  client_id text not null,
  redirect_uri text not null,
  code_challenge text not null,
  scope text not null,
  sealed_session text not null,
  expires_at timestamptz not null,
  created_at timestamptz not null default now()
);
create index if not exists oauth_authorization_transaction_expiry
  on oauth_authorization_transaction (expires_at);

create or replace function placement_consume_oauth_transaction(p_code_digest text)
returns table (client_id text, redirect_uri text, code_challenge text, scope text, sealed_session text)
language sql
security definer
set search_path = public
as $$
  delete from oauth_authorization_transaction
  where code_digest = p_code_digest and expires_at > now()
  returning client_id, redirect_uri, code_challenge, scope, sealed_session;
$$;
