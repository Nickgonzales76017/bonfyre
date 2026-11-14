-- Aurekai Dense Packs v1. Replaceable physical representations only.
-- Canonical truth, grants, evidence, publication and effects remain native.
-- A namespace is a SHA-256 of a server-verified Bonfyre closure coordinate;
-- never accept it from an untrusted client or use a database row as a grant.
create table public.dense_pack_dictionary (
 namespace_digest bytea not null check(octet_length(namespace_digest)=32),
 dictionary_digest bytea not null check(octet_length(dictionary_digest)=32),
 payload bytea not null check(octet_length(payload)<=1048576),
 created_at timestamptz not null default now(),
 primary key(namespace_digest,dictionary_digest),
 constraint dense_dictionary_exact_digest check(sha256(payload)=dictionary_digest)
);
create table public.dense_pack_atom (
 namespace_digest bytea not null check(octet_length(namespace_digest)=32),
 atom_digest bytea not null check(octet_length(atom_digest)=32),
 dictionary_digest bytea,
 codec smallint not null check(codec in (0,1,2)),
 raw_bytes integer not null check(raw_bytes between 0 and 1048576),
 payload bytea not null check(octet_length(payload)<=1048576),
 created_at timestamptz not null default now(),
 primary key(namespace_digest,atom_digest),
 foreign key(namespace_digest,dictionary_digest)
   references public.dense_pack_dictionary(namespace_digest,dictionary_digest),
 constraint dense_atom_dictionary_contract check(
   (codec=2 and dictionary_digest is not null) or
   (codec in (0,1) and dictionary_digest is null)),
 constraint dense_atom_exact_raw check(
   codec<>0 or (raw_bytes=octet_length(payload) and sha256(payload)=atom_digest))
);
create table public.dense_pack_passport (
 namespace_digest bytea not null check(octet_length(namespace_digest)=32),
 passport_digest bytea not null check(octet_length(passport_digest)=32),
 native_owner text not null check(native_owner ~ '^Bonfyre[A-Za-z0-9]{1,63}$'),
 subject_family text not null check(subject_family ~ '^[a-z][a-z0-9_:-]{0,79}$'),
 representation_family text not null check(representation_family ~ '^[a-z][a-z0-9_:-]{0,79}$'),
 version text not null check(length(version) between 1 and 48),
 exactness_class text not null check(exactness_class in ('exact','latent')),
 measurement_state text not null check(measurement_state in ('declared','measured')),
 quality_loss_budget numeric(12,9) not null check(quality_loss_budget>=0),
 basis_digest bytea check(octet_length(basis_digest)=32),
 binding_digest bytea check(octet_length(binding_digest)=32),
 exception_digest bytea check(octet_length(exception_digest)=32),
 lift_ref text not null check(length(lift_ref) between 1 and 240),
 thaw_ref text,
 source_generation_digest bytea not null check(octet_length(source_generation_digest)=32),
 source_receipt_digest bytea not null check(octet_length(source_receipt_digest)=32),
 created_at timestamptz not null default now(),
 primary key(namespace_digest,passport_digest),
 constraint dense_exact_lossless check(exactness_class<>'exact' or quality_loss_budget=0),
 constraint dense_latent_thaw check(exactness_class<>'latent' or length(thaw_ref)>0)
);
create table public.dense_pack_root (
 namespace_digest bytea not null check(octet_length(namespace_digest)=32),
 root_digest bytea not null check(octet_length(root_digest)=32),
 passport_digest bytea not null check(octet_length(passport_digest)=32),
 family text not null check(family in (
   'record_factor','address_frontier','pack_overlay',
   'embedding_projection','ghost_reference','pointer_pack','receipt_index')),
 bucket_bits smallint not null check(bucket_bits between 0 and 16),
 record_count integer not null check(record_count between 1 and 100000000),
 page_count integer not null check(page_count between 1 and 65536),
 source_bytes bigint not null check(source_bytes>=0),
 native_verified_receipt bytea not null check(octet_length(native_verified_receipt)=32),
 created_at timestamptz not null default now(),
 primary key(namespace_digest,root_digest),
 foreign key(namespace_digest,passport_digest)
   references public.dense_pack_passport(namespace_digest,passport_digest)
);
create table public.dense_pack_page (
 namespace_digest bytea not null check(octet_length(namespace_digest)=32),
 root_digest bytea not null check(octet_length(root_digest)=32),
 bucket integer not null check(bucket between 0 and 65535),
 record_count integer not null check(record_count between 1 and 4096),
 raw_bytes integer not null check(raw_bytes between 8 and 1048576),
 codec smallint not null default 0 check(codec=0),
 payload bytea not null check(octet_length(payload) between 8 and 1048576),
 page_digest bytea not null check(octet_length(page_digest)=32),
 primary key(namespace_digest,root_digest,bucket),
 foreign key(namespace_digest,root_digest)
   references public.dense_pack_root(namespace_digest,root_digest),
 constraint dense_page_identity check(
   raw_bytes=octet_length(payload) and page_digest=sha256(payload))
);
-- Geometry is a representation attribute, not an authorized ANN or a model result.
-- No pgvector/HNSW index is created without a comparable measured passport.
create table public.dense_pack_embedding_geometry (
 namespace_digest bytea not null check(octet_length(namespace_digest)=32),
 passport_digest bytea not null check(octet_length(passport_digest)=32),
 space_digest bytea not null check(octet_length(space_digest)=32),
 dimensions integer not null check(dimensions between 1 and 65536),
 scalar_encoding text not null check(scalar_encoding in ('f32','f16','i8','bit1')),
 comparability_state text not null check(comparability_state in ('defer','measured','admit')),
 similarity_metric text not null check(similarity_metric in ('cosine','inner_product','l2','none')),
 created_at timestamptz not null default now(),
 primary key(namespace_digest,passport_digest),
 foreign key(namespace_digest,passport_digest)
   references public.dense_pack_passport(namespace_digest,passport_digest),
 constraint dense_geometry_not_auto_admitted check(comparability_state<>'admit' or similarity_metric<>'none')
);
comment on table public.dense_pack_root is
 'A physical locator, not a native activation, provider admission, Ghost design acceptance, BitNet decision, financial record, or proof of hydration.';
comment on table public.dense_pack_embedding_geometry is
 'Typed native model representation passport projection; cross-model similarity requires a separate native comparability contract. Quantization never implies quality.';
-- Exposed public schema, but no public direct data plane. Only a server-side
-- sealed service role obtains bytes after rechecking the exact native closure.
alter table public.dense_pack_dictionary enable row level security;
alter table public.dense_pack_atom enable row level security;
alter table public.dense_pack_passport enable row level security;
alter table public.dense_pack_root enable row level security;
alter table public.dense_pack_page enable row level security;
alter table public.dense_pack_embedding_geometry enable row level security;
revoke all on public.dense_pack_dictionary,public.dense_pack_atom,
 public.dense_pack_passport,public.dense_pack_root,public.dense_pack_page,
 public.dense_pack_embedding_geometry from public,anon,authenticated;
grant select,insert on public.dense_pack_dictionary,public.dense_pack_atom,
 public.dense_pack_passport,public.dense_pack_root,public.dense_pack_page,
 public.dense_pack_embedding_geometry to service_role;
create function public.dense_pack_candidate_page(
 p_namespace bytea,p_root bytea,p_key_digest bytea
) returns table(bucket integer,record_count integer,payload bytea,page_digest bytea)
language sql stable security invoker set search_path=''
as $dense_page$
  select pg.bucket,pg.record_count,pg.payload,pg.page_digest
  from public.dense_pack_root r
  join public.dense_pack_page pg
    on pg.namespace_digest=r.namespace_digest and pg.root_digest=r.root_digest
  where r.namespace_digest=p_namespace and r.root_digest=p_root
    and octet_length(p_key_digest)=32
    and pg.bucket=(
      (pg_catalog.get_byte(p_key_digest,0)*256+pg_catalog.get_byte(p_key_digest,1))
      >> (16-r.bucket_bits))
  limit 1;
$dense_page$;
revoke all on function public.dense_pack_candidate_page(bytea,bytea,bytea)
 from public,anon,authenticated;
grant execute on function public.dense_pack_candidate_page(bytea,bytea,bytea)
 to service_role;
