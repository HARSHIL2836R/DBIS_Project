-- Restore the registry disabled by gsi_off.sql.
BEGIN;
INSERT INTO public.gsi_registry SELECT * FROM public.gsi_registry_disabled
    ON CONFLICT (index_name) DO NOTHING;
DELETE FROM public.gsi_registry_disabled;
COMMIT;
