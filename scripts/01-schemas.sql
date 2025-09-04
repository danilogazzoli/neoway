-- PASSO 1: Criação dos Schemas
--
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS context;
CREATE SCHEMA IF NOT EXISTS app;

DO $$
BEGIN
  RAISE NOTICE 'Schemas raw, context e app criados (se não existiam).';
END
$$;


