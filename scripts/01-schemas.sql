-- PASSO 1: Criação dos Schemas
--

--esta extensão fornece a função unaccent(), que remove acentos de caracteres, útil para normalização de textos.
CREATE EXTENSION IF NOT EXISTS unaccent;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS context;
CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS rst;

DO $$
BEGIN
  RAISE NOTICE 'Schemas raw, context, app e rst criados';
END
$$;


