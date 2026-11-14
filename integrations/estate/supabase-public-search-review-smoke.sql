-- Transactional smoke; no publications or fixtures survive rollback.
begin;
insert into public.estate_item(address,source,key,kind,title,body,href)
values('bf-public-search-smoke-20261110','bonfyre-test','search-smoke-01',
       'article','Bfreviewgateonly synthetic title','Synthetic content',
       'https://example.invalid/search-smoke');
do $test$
declare before_count integer; approved_count integer;
        mutated_count integer; withdrawn_count integer;
begin
  select count(*) into before_count
    from public.estate_search_reviewed('Bfreviewgateonly',40);
  if before_count<>0 then raise exception 'unreviewed content public'; end if;
  insert into public.estate_search_publication
    (address,source,item_key,item_digest,publication_receipt,reviewed_at)
  select i.address,i.source,i.key,
    pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.row_to_json(i)::text,'UTF8')),
    pg_catalog.sha256('synthetic-review-receipt'::bytea),now()
  from public.estate_item i where i.address='bf-public-search-smoke-20261110';
  select count(*) into approved_count
    from public.estate_search_reviewed('Bfreviewgateonly',40);
  if approved_count<>1 then raise exception 'reviewed content invisible'; end if;
  update public.estate_item set title='Bfreviewgateonly altered'
    where address='bf-public-search-smoke-20261110';
  select count(*) into mutated_count
    from public.estate_search_reviewed('Bfreviewgateonly',40);
  if mutated_count<>0 then raise exception 'unreviewed edit public'; end if;
  update public.estate_search_publication set withdrawn_at=now()
    where address='bf-public-search-smoke-20261110';
  select count(*) into withdrawn_count
    from public.estate_search_reviewed('Bfreviewgateonly',40);
  if withdrawn_count<>0 then raise exception 'withdrawn content public'; end if;
end $test$;
rollback;
