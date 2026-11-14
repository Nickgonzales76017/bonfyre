-- Transactional live Supabase smoke: the fixture is always rolled back.
-- Run under the trusted migration/maintenance role, not anon/authenticated.
begin;
insert into public.dense_pack_dictionary(namespace_digest,dictionary_digest,payload)
values(sha256('bf-dense-smoke-namespace'::bytea),sha256(''::bytea),''::bytea);
insert into public.dense_pack_atom(namespace_digest,atom_digest,codec,raw_bytes,payload)
values(sha256('bf-dense-smoke-namespace'::bytea),sha256('test'::bytea),0,4,'test'::bytea);
insert into public.dense_pack_passport(
 namespace_digest,passport_digest,native_owner,subject_family,representation_family,
 version,exactness_class,measurement_state,quality_loss_budget,lift_ref,
 source_generation_digest,source_receipt_digest)
values(
 sha256('bf-dense-smoke-namespace'::bytea),sha256('bf-dense-smoke-passport'::bytea),
 'BonfyreCompress','record_factor','bonfyre_record_factor_zstd','2',
 'exact','measured',0,'BonfyreCompress:factor-read',
 sha256('bf-dense-smoke-generation'::bytea),sha256('bf-dense-smoke-receipt'::bytea));
insert into public.dense_pack_root(
 namespace_digest,root_digest,passport_digest,family,bucket_bits,record_count,
 page_count,source_bytes,native_verified_receipt)
values(
 sha256('bf-dense-smoke-namespace'::bytea),sha256('bf-dense-smoke-root'::bytea),
 sha256('bf-dense-smoke-passport'::bytea),'record_factor',0,1,1,4,
 sha256('bf-dense-smoke-receipt'::bytea));
with page as (
 select decode('4246445001000001','hex') ||
 sha256('bf-dense-smoke-key'::bytea) || sha256('test'::bytea) ||
 decode('00000004','hex') || decode('0001','hex') ||
 sha256('test'::bytea) as raw
)
insert into public.dense_pack_page(
 namespace_digest,root_digest,bucket,record_count,raw_bytes,codec,
 payload,page_digest,encoded_digest)
select
 sha256('bf-dense-smoke-namespace'::bytea),sha256('bf-dense-smoke-root'::bytea),
 0,1,octet_length(raw),0,raw,sha256(raw),sha256(raw)
from page;
do $check$
declare found integer; verified integer;
begin
 select count(*) into found
 from public.dense_pack_candidate_page_v2(
 sha256('bf-dense-smoke-namespace'::bytea),
 sha256('bf-dense-smoke-root'::bytea),
 sha256('bf-dense-smoke-key'::bytea)
 ) p
 where p.codec=0 and p.raw_bytes=110 and p.record_count=1
   and p.page_digest=sha256(p.payload)
   and p.encoded_digest=sha256(p.payload);
 if found<>1 then raise exception 'dense v2 smoke: exact candidate missing'; end if;
 select count(*) into found
 from public.dense_pack_candidate_page_v2(
 sha256('bf-dense-smoke-foreign-namespace'::bytea),
 sha256('bf-dense-smoke-root'::bytea),
 sha256('bf-dense-smoke-key'::bytea));
 if found<>0 then raise exception 'dense v2 smoke: cross-namespace leak'; end if;
 select count(*) into found
 from public.dense_pack_candidate_page(
 sha256('bf-dense-smoke-namespace'::bytea),
 sha256('bf-dense-smoke-root'::bytea),
 sha256('bf-dense-smoke-key'::bytea));
 if found<>1 then raise exception 'dense v1 smoke: compatibility break'; end if;
 verified:=0;
 begin
   insert into public.dense_pack_page(
     namespace_digest,root_digest,bucket,record_count,raw_bytes,codec,
     payload,page_digest,encoded_digest)
   values(
     sha256('bf-dense-smoke-namespace'::bytea),
     sha256('bf-dense-smoke-root'::bytea),1,1,8,0,
     decode('4246445001000001','hex'),sha256(decode('4246445001000001','hex')),
     sha256('wrong'::bytea));
 exception when check_violation then verified:=1;
 end;
 if verified<>1 then raise exception 'dense v2 smoke: tamper was accepted'; end if;
end $check$;
do $atom_check$
declare matches integer; rejected integer;
begin
 select count(*) into matches from public.dense_pack_atom_slice(
   sha256('bf-dense-smoke-namespace'::bytea),
   array[sha256('test'::bytea)]
 ) a
 where a.raw_bytes=4 and a.codec=0 and a.payload='test'::bytea
   and a.atom_digest=sha256(a.payload);
 if matches<>1 then raise exception 'dense atom smoke: exact body missing'; end if;
 select count(*) into matches from public.dense_pack_atom_slice(
   sha256('bf-dense-smoke-foreign-namespace'::bytea),
   array[sha256('test'::bytea)]);
 if matches<>0 then raise exception 'dense atom smoke: cross-namespace leak'; end if;
 rejected:=0;
 begin
   perform count(*) from public.dense_pack_atom_slice(
     sha256('bf-dense-smoke-namespace'::bytea),
     array_fill(sha256('test'::bytea),array[17]));
 exception when invalid_parameter_value then rejected:=1;
 end;
 if rejected<>1 then raise exception 'dense atom smoke: unbounded query accepted'; end if;
end $atom_check$;
rollback;
