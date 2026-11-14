-- Dense Packs: bounded immutable-atom fetch for a native-verified page candidate.
-- A caller must first pass the native closure and read the verified pointer page.
-- Never expose this RPC to unauthenticated / client JWT users.
create function public.dense_pack_atom_slice(
 p_namespace bytea,p_digests bytea[]
) returns table(
 atom_digest bytea,raw_bytes integer,codec smallint,
 dictionary_digest bytea,payload bytea
)
language plpgsql stable security invoker set search_path=''
as $atom_slice$
begin
 if p_namespace is null or pg_catalog.octet_length(p_namespace)<>32 or
    p_digests is null or pg_catalog.cardinality(p_digests)<1 or
    pg_catalog.cardinality(p_digests)>16 or
    exists(select 1 from pg_catalog.unnest(p_digests) as x(digest)
           where x.digest is null or pg_catalog.octet_length(x.digest)<>32)
 then
   raise exception 'dense atom slice: invalid bounded lookup' using errcode='22023';
 end if;
 return query
   select a.atom_digest,a.raw_bytes,a.codec,a.dictionary_digest,a.payload
   from public.dense_pack_atom a
   where a.namespace_digest=p_namespace and a.atom_digest=any(p_digests)
   order by a.atom_digest
   limit 16;
end;
$atom_slice$;
revoke all on function public.dense_pack_atom_slice(bytea,bytea[])
 from public,anon,authenticated;
grant execute on function public.dense_pack_atom_slice(bytea,bytea[])
 to service_role;
comment on function public.dense_pack_atom_slice(bytea,bytea[]) is
 'Derived data bytes only, 16 immutable atoms maximum. Caller supplies a native-verified closure, not a browser-selected namespace.';
