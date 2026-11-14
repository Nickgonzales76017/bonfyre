-- Server-owned remote page tier for existing Dense Packs. Never a native memory
-- grant, semantic owner, publication authority, or public page endpoint.
create or replace function public.dense_pack_memory_directory(
 p_namespace bytea,p_root bytea,p_after_bucket integer default -1,
 p_max_pages integer default 32
) returns table(bucket integer,record_count integer,codec smallint,
 raw_bytes integer,stored_bytes integer,page_digest bytea,encoded_digest bytea)
language plpgsql stable security invoker set search_path=''
as $memory_dir$
begin
 if pg_catalog.octet_length(p_namespace)<>32 or pg_catalog.octet_length(p_root)<>32
  or p_namespace is null or p_root is null or p_after_bucket is null
  or p_after_bucket not between -1 and 65535 or p_max_pages is null
  or p_max_pages not between 1 and 64 then
  raise exception 'invalid dense memory directory request' using errcode='22023';
 end if;
 return query
 select pg.bucket,pg.record_count,pg.codec,pg.raw_bytes,
        pg_catalog.octet_length(pg.payload)::integer,
        pg.page_digest,pg.encoded_digest
 from public.dense_pack_root r
 join public.dense_pack_page pg
  on pg.namespace_digest=r.namespace_digest and pg.root_digest=r.root_digest
 where r.namespace_digest=p_namespace and r.root_digest=p_root
   and pg.bucket>p_after_bucket
 order by pg.bucket
 limit p_max_pages;
end
$memory_dir$;
revoke all on function public.dense_pack_memory_directory(bytea,bytea,integer,integer)
 from public,anon,authenticated;
grant execute on function public.dense_pack_memory_directory(bytea,bytea,integer,integer)
 to service_role;

-- Read-through prefetch with a resumable bucket cursor. Every stored page is
-- <=1 MiB, so the >=1 MiB minimum budget guarantees forward progress.
-- The stored-byte cap does not include REST/base64/hex transfer overhead.
create or replace function public.dense_pack_memory_window(
 p_namespace bytea,p_root bytea,p_after_bucket integer default -1,
 p_max_pages integer default 4,p_budget_bytes integer default 1048576
) returns table(bucket integer,record_count integer,codec smallint,
 raw_bytes integer,stored_bytes integer,payload bytea,
 page_digest bytea,encoded_digest bytea)
language plpgsql stable security invoker set search_path=''
as $memory_window$
begin
 if pg_catalog.octet_length(p_namespace)<>32 or pg_catalog.octet_length(p_root)<>32
  or p_namespace is null or p_root is null or p_after_bucket is null
  or p_after_bucket not between -1 and 65535 or p_max_pages is null
  or p_max_pages not between 1 and 8 or p_budget_bytes is null
  or p_budget_bytes not between 1048576 and 4194304 then
  raise exception 'invalid dense memory page window' using errcode='22023';
 end if;
 return query
 with candidate as materialized (
  select pg.bucket,pg.record_count,pg.codec,pg.raw_bytes,pg.payload,
         pg.page_digest,pg.encoded_digest
  from public.dense_pack_root r
  join public.dense_pack_page pg
   on pg.namespace_digest=r.namespace_digest and pg.root_digest=r.root_digest
  where r.namespace_digest=p_namespace and r.root_digest=p_root
    and pg.bucket>p_after_bucket
  order by pg.bucket
  limit p_max_pages
 ), metered as (
  select c.*,sum(pg_catalog.octet_length(c.payload)) over(order by c.bucket) as total_bytes
  from candidate c
 )
 select m.bucket,m.record_count,m.codec,m.raw_bytes,
        pg_catalog.octet_length(m.payload)::integer,m.payload,
        m.page_digest,m.encoded_digest
 from metered m
 where m.total_bytes<=p_budget_bytes
 order by m.bucket;
end
$memory_window$;
revoke all on function public.dense_pack_memory_window(bytea,bytea,integer,integer,integer)
 from public,anon,authenticated;
grant execute on function public.dense_pack_memory_window(bytea,bytea,integer,integer,integer)
 to service_role;
