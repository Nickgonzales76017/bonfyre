-- A reviewed, fail-closed publication projection; live estate_search is untouched.
-- Only the native publication owner may prove rights, identity and receipt.
create table if not exists public.estate_search_publication (
  address text primary key references public.estate_item(address) on delete cascade,
  source text not null,
  item_key text not null,
  item_digest bytea not null check (octet_length(item_digest)=32),
  publication_receipt bytea not null check (octet_length(publication_receipt)=32),
  reviewed_at timestamptz not null,
  withdrawn_at timestamptz,
  constraint estate_search_publication_withdrawal
    check (withdrawn_at is null or withdrawn_at>=reviewed_at)
);
comment on table public.estate_search_publication is
  'Replaceable native-reviewed public-search admission; never a canonical publication authority.';
alter table public.estate_search_publication enable row level security;
revoke all on table public.estate_search_publication from public,anon,authenticated;
grant select,insert,update,delete on table public.estate_search_publication to service_role;
-- Transitional RPC: switch public consumers only after native publication review,
-- exact backfill, and four-apex release acceptance.
create or replace function public.estate_search_reviewed(q text,want integer default 40)
returns table(address text,source text,kind text,title text,publisher text,
              href text,image text,at bigint,rank real)
language sql stable security definer set search_path=''
as $reviewed$
  select i.address,i.source,i.kind,i.title,i.publisher,i.href,i.image,i.at,
    pg_catalog.ts_rank(
      pg_catalog.to_tsvector('english',
        coalesce(i.title,'')||' '||coalesce(i.body,'')||' '||coalesce(i.publisher,'')),
      pg_catalog.websearch_to_tsquery('english',q)) as rank
  from public.estate_item i
  join public.estate_search_publication p on p.address=i.address
  where p.source=i.source and p.item_key=i.key
    and p.withdrawn_at is null
    and p.item_digest=pg_catalog.sha256(
      pg_catalog.convert_to(pg_catalog.row_to_json(i)::text,'UTF8'))
    and q is not null and length(q) between 1 and 200
    and pg_catalog.to_tsvector('english',
      coalesce(i.title,'')||' '||coalesce(i.body,'')||' '||coalesce(i.publisher,''))
      @@ pg_catalog.websearch_to_tsquery('english',q)
  order by rank desc,i.at desc
  limit least(greatest(coalesce(want,40),1),200);
$reviewed$;
revoke all on function public.estate_search_reviewed(text,integer)
  from public,anon,authenticated;
grant execute on function public.estate_search_reviewed(text,integer)
  to anon,authenticated,service_role;
