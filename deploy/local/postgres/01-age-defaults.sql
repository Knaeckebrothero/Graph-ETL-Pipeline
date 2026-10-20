-- Make Apache AGE usable in every session without per-session setup.
-- The image already preloads the library (shared_preload_libraries=age) and
-- creates the extension in 00-create-extension-age.sql. Putting ag_catalog
-- last on the search path keeps cypher() and agtype resolvable while plain
-- CREATE TABLE statements still land in public.
CREATE EXTENSION IF NOT EXISTS age;

DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET search_path = "$user", public, ag_catalog', current_database());
END
$$;
