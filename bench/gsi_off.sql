-- Disable every GSI without dropping the postings tables.
-- The FDW decides gsi_usable at plan time by looking up public.gsi_registry by
-- (foreigntable_oid, column). Emptying the registry is therefore the smallest
-- change that makes the planner behave as if no index existed, while leaving
-- every postings row on disk. Documented in README under "How the A/B works".
BEGIN;
INSERT INTO public.gsi_registry_disabled SELECT * FROM public.gsi_registry
    ON CONFLICT (index_name) DO NOTHING;
ALTER TABLE public.gsi_index_file_state DROP CONSTRAINT IF EXISTS gsi_index_file_state_index_name_fkey;
DELETE FROM public.gsi_registry;
COMMIT;
