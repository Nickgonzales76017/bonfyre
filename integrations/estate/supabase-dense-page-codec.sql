-- Dense page compression v2: byte-verified Zstandard framing.
-- A compressed page retains BOTH the original structural digest and the
-- encoded payload digest. No authority or current-generation claim is added.
alter table public.dense_pack_page
 add column encoded_digest bytea not null check(octet_length(encoded_digest)=32);
alter table public.dense_pack_page drop constraint dense_pack_page_codec_check;
alter table public.dense_pack_page
 add constraint dense_pack_page_codec_check check(codec in (0,1));
alter table public.dense_pack_page drop constraint dense_page_identity;
alter table public.dense_pack_page add constraint dense_page_identity check(
 encoded_digest=sha256(payload)
 and (codec=1 or (raw_bytes=octet_length(payload) and page_digest=sha256(payload)))
);
-- The original identity-only candidate reader continues to return RAW pages.
-- A new version carries both digests and the encoding explicitly.
create or replace function public.dense_pack_candidate_page(
 p_namespace bytea,p_root bytea,p_key_digest bytea
) returns table(bucket integer,record_count integer,payload bytea,page_digest bytea)
language sql stable security invoker set search_path=''
as $raw_page$
 select pg.bucket,pg.record_count,pg.payload,pg.page_digest
 from public.dense_pack_root r
 join public.dense_pack_page pg
   on pg.namespace_digest=r.namespace_digest and pg.root_digest=r.root_digest
 where r.namespace_digest=p_namespace and r.root_digest=p_root
   and pg.codec=0
   and octet_length(p_key_digest)=32
   and pg.bucket=((pg_catalog.get_byte(p_key_digest,0)*256+
     pg_catalog.get_byte(p_key_digest,1))>>(16-r.bucket_bits))
 limit 1;
$raw_page$;
create function public.dense_pack_candidate_page_v2(
 p_namespace bytea,p_root bytea,p_key_digest bytea
) returns table(
 bucket integer,record_count integer,codec smallint,raw_bytes integer,
 payload bytea,page_digest bytea,encoded_digest bytea
)
language sql stable security invoker set search_path=''
as $encoded_page$
 select pg.bucket,pg.record_count,pg.codec,pg.raw_bytes,
        pg.payload,pg.page_digest,pg.encoded_digest
 from public.dense_pack_root r
 join public.dense_pack_page pg
   on pg.namespace_digest=r.namespace_digest and pg.root_digest=r.root_digest
 where r.namespace_digest=p_namespace and r.root_digest=p_root
   and octet_length(p_key_digest)=32
   and pg.bucket=((pg_catalog.get_byte(p_key_digest,0)*256+
     pg_catalog.get_byte(p_key_digest,1))>>(16-r.bucket_bits))
 limit 1;
$encoded_page$;
revoke all on function public.dense_pack_candidate_page_v2(bytea,bytea,bytea)
 from public,anon,authenticated;
grant execute on function public.dense_pack_candidate_page_v2(bytea,bytea,bytea)
 to service_role;
