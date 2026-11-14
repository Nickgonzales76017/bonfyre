-- Bonfyre historical provider receipt projection.
-- Applied to exwxrwjayumtoljjxufh as migration 20261109201550.
-- Read-only service-role aggregate; not provider health or routing authority.
create function public.provider_receipt_summary(
  p_since timestamptz default now() - interval '365 days',
  p_limit integer default 50
)
returns table (
  provider_ref text,
  call_class text,
  attempts bigint,
  successes bigint,
  first_observed timestamptz,
  last_observed timestamptz
)
language sql stable security invoker
set search_path = ''
as $summary$
  select r.provider_ref, r.call_class,
         count(*)::bigint, count(*) filter (where r.ok)::bigint,
         min(r.observed_at), max(r.observed_at)
  from public.bonfyre_power_user_provider_receipts r
  where p_since >= now() - interval '5 years' and p_since <= now()
    and r.observed_at >= p_since and r.observed_at <= now()
  group by r.provider_ref, r.call_class
  order by count(*) desc, r.provider_ref
  limit least(greatest(coalesce(p_limit,50),1),100);
$summary$;
revoke all on function public.provider_receipt_summary(timestamptz,integer)
from public, anon, authenticated;
grant execute on function public.provider_receipt_summary(timestamptz,integer)
to service_role;
-- Event triggers are internal database behavior, not ordinary API functions.
revoke all on function public.rls_auto_enable() from public,anon,authenticated;
