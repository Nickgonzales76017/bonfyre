-- Aurekai estate search projection. Not a source of semantic authority.
-- Run only after inspecting an existing public.estate_item or estate_search.
-- Reconciled on the existing 69,551-row database, NOT a bootstrap schema.
-- Applied as Supabase migration 20261109201046.
revoke all privileges on table
  public.bonfyre_power_user_provider_receipts,
  public.estate_item,
  public.oauth_authorization_transaction,
  public.placement_generation,
  public.placement_receipt,
  public.placement_service
from anon, authenticated;
-- A narrow, intentional public query; no raw table exposure or effects.
revoke all on function public.placement_consume_oauth_transaction(text)
from public, anon, authenticated;
grant execute on function public.placement_consume_oauth_transaction(text) to service_role;
grant execute on function public.estate_search(text,integer) to anon, authenticated;
-- The existing estate_search signature and 9-column result are preserved.
-- An archive insert does not set public_search_approved. Promotion requires
-- separate native review of exact public source identity and distribution rights.
-- The current schema has no public_search_approved column. No automatic
-- promotion is implemented by this permissions migration.
