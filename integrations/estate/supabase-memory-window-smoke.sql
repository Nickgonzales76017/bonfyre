-- All synthetic remote-memory fixtures are rolled back after testing.
begin;
insert into public.dense_pack_passport(
 namespace_digest,passport_digest,native_owner,subject_family,representation_family,
 version,exactness_class,measurement_state,quality_loss_budget,lift_ref,
 source_generation_digest,source_receipt_digest)
values(
 pg_catalog.sha256('bf-remote-memory-smoke-namespace'::bytea),
 pg_catalog.sha256('bf-remote-memory-smoke-passport'::bytea),
 'BonfyreCompress','receipt_index','bonfyre_record_factor_zstd','2',
 'exact','measured',0,'BonfyreCompress:factor-read',
 pg_catalog.sha256('bf-remote-memory-smoke-generation'::bytea),
 pg_catalog.sha256('bf-remote-memory-smoke-receipt'::bytea));
insert into public.dense_pack_root(
 namespace_digest,root_digest,passport_digest,family,bucket_bits,
 record_count,page_count,source_bytes,native_verified_receipt)
values(
 pg_catalog.sha256('bf-remote-memory-smoke-namespace'::bytea),
 pg_catalog.sha256('bf-remote-memory-smoke-root'::bytea),
 pg_catalog.sha256('bf-remote-memory-smoke-passport'::bytea),
 'receipt_index',1,2,2,8,
 pg_catalog.sha256('bf-remote-memory-smoke-receipt'::bytea));
with p as (
 select pg_catalog.decode('4246445001010001','hex')
        ||case when bucket=0 then pg_catalog.decode(pg_catalog.repeat('00',32),'hex')
              else pg_catalog.decode(pg_catalog.repeat('ff',32),'hex') end
        ||pg_catalog.sha256('test'::bytea)
        ||pg_catalog.decode('00000004','hex')
        ||pg_catalog.decode('0001','hex')
        ||pg_catalog.sha256('test'::bytea) as body,bucket
 from (values(0),(1)) v(bucket)
)
insert into public.dense_pack_page(
 namespace_digest,root_digest,bucket,record_count,raw_bytes,codec,
 payload,page_digest,encoded_digest)
select pg_catalog.sha256('bf-remote-memory-smoke-namespace'::bytea),
       pg_catalog.sha256('bf-remote-memory-smoke-root'::bytea),
       p.bucket,1,pg_catalog.octet_length(p.body),0,p.body,
       pg_catalog.sha256(p.body),pg_catalog.sha256(p.body)
from p;
do $witness$
declare ns bytea:=pg_catalog.sha256('bf-remote-memory-smoke-namespace'::bytea);
        root bytea:=pg_catalog.sha256('bf-remote-memory-smoke-root'::bytea);
        n integer; b integer; rejected boolean:=false;
begin
 select count(*) into n from public.dense_pack_memory_directory(ns,root);
 if n<>2 then raise exception 'directory omitted physical pages'; end if;
 select count(*) into n from public.dense_pack_memory_window(ns,root);
 if n<>2 then raise exception 'window omitted physical pages'; end if;
 select count(*) into n from public.dense_pack_memory_window(ns,root,-1,1);
 if n<>1 then raise exception 'window page count was not bounded'; end if;
 select max(bucket) into b from public.dense_pack_memory_window(ns,root,-1,1);
 if b<>0 then raise exception 'first cursor incorrect'; end if;
 select min(bucket) into b from public.dense_pack_memory_window(ns,root,b,1);
 if b<>1 then raise exception 'resume cursor incorrect'; end if;
 select count(*) into n from public.dense_pack_memory_window(ns,root,1,4);
 if n<>0 then raise exception 'past-end window nonempty'; end if;
 select count(*) into n from public.dense_pack_memory_window(
  pg_catalog.sha256('bf-remote-memory-smoke-foreign'::bytea),root);
 if n<>0 then raise exception 'cross-namespace exposure'; end if;
 select count(*) into n from public.dense_pack_memory_window(
  ns,pg_catalog.sha256('bf-remote-memory-smoke-foreign-root'::bytea));
 if n<>0 then raise exception 'cross-root exposure'; end if;
 select count(*) into n from public.dense_pack_memory_window(ns,root)
  where pg_catalog.sha256(payload)=encoded_digest
    and page_digest=encoded_digest and stored_bytes=110 and raw_bytes=110;
 if n<>2 then raise exception 'encoded payload or size incorrect'; end if;
 begin
  perform * from public.dense_pack_memory_window(ns,root,-1,8,1024);
 exception when invalid_parameter_value then rejected:=true;
 end;
 if not rejected then raise exception 'tiny budget accepted'; end if;
 if pg_catalog.has_function_privilege('anon','public.dense_pack_memory_window(bytea,bytea,integer,integer,integer)','EXECUTE')
  or pg_catalog.has_function_privilege('authenticated','public.dense_pack_memory_window(bytea,bytea,integer,integer,integer)','EXECUTE')
  or pg_catalog.has_function_privilege('anon','public.dense_pack_memory_directory(bytea,bytea,integer,integer)','EXECUTE')
  or pg_catalog.has_function_privilege('authenticated','public.dense_pack_memory_directory(bytea,bytea,integer,integer)','EXECUTE')
 then raise exception 'public caller can page private data'; end if;
end $witness$;
rollback;
