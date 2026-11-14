-- Connected instruments: account-owned measurement projection, never an
-- account issuer or provider execution owner. Applied in Supabase as
-- account_owned_connected_measurement_projection, 2026-11-09.
create table public.account_measurement (
 observation_id text primary key check (observation_id ~ '^[a-f0-9]{64}$'),
 subject text not null check (
   subject ~ '^(user:[1-9][0-9]{0,9}|institution:[a-z0-9][a-z0-9-]{0,63}|household:[a-z0-9][a-z0-9-]{0,63})$'),
 provider text not null check (provider ~ '^[a-z][a-z0-9_]{0,47}$'),
 account_ref text not null check (account_ref ~ '^[a-z0-9][a-z0-9._:-]{0,95}$'),
 metric text not null check (metric ~ '^[a-z][a-z0-9_]{0,79}$'),
 unit text not null check (unit in ('count','percent','usd','ms','bytes')),
 period_start date not null,
 period_end date not null,
 period_kind text not null check (period_kind in ('day','week','month','custom','lifetime')),
 value numeric(22,4) not null check (value>=0),
 source_digest text not null check (source_digest ~ '^[a-f0-9]{64}$'),
 source_observed_at timestamptz not null,
 ingested_at timestamptz not null default now(),
 constraint account_measurement_period check(period_end>=period_start and period_end-period_start<=36600)
);
comment on table public.account_measurement is
 'Immutable replaceable account measurements. Source receipts, grants and publication remain native. No credentials or raw source bodies.';
create index account_measurement_subject_period on public.account_measurement
 (subject,provider,period_end desc,ingested_at desc);
alter table public.account_measurement enable row level security;
revoke all on public.account_measurement from public,anon,authenticated;
grant select,insert on public.account_measurement to service_role;
-- Only the trusted native gateway derives a subject from a verified session.
-- The service_role privilege here is not an account authorization grant.
create function public.account_measurement_portfolio(
 p_subject text,p_provider text default null,p_limit integer default 100
) returns table(
 provider text,account_ref text,metric text,unit text,
 period_start date,period_end date,period_kind text,
 value numeric,source_digest text,source_observed_at timestamptz,
 ingested_at timestamptz
)
language sql stable security invoker
set search_path=''
as $portfolio$
 select x.provider,x.account_ref,x.metric,x.unit,x.period_start,x.period_end,
        x.period_kind,x.value,x.source_digest,x.source_observed_at,x.ingested_at
 from (
   select distinct on(m.provider,m.account_ref,m.metric,m.period_start,m.period_end)
    m.provider,m.account_ref,m.metric,m.unit,m.period_start,m.period_end,
    m.period_kind,m.value,m.source_digest,m.source_observed_at,m.ingested_at,m.observation_id
   from public.account_measurement m
   where m.subject=p_subject
     and (p_provider is null or m.provider=p_provider)
     and length(p_subject)<=96
   order by m.provider,m.account_ref,m.metric,m.period_start,m.period_end,
            m.source_observed_at desc,m.ingested_at desc,m.observation_id desc
 ) x
 order by x.period_end desc,x.provider,x.metric
 limit least(greatest(coalesce(p_limit,100),1),100);
$portfolio$;
revoke all on function public.account_measurement_portfolio(text,text,integer)
 from public,anon,authenticated;
grant execute on function public.account_measurement_portfolio(text,text,integer)
 to service_role;
