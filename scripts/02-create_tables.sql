--=============================================================================
-- CAMADA RAW: Ingestão de Dados Brutos
--=============================================================================

CREATE TABLE IF NOT EXISTS raw.faturas (
    emitente TEXT,
    documento TEXT,
    contrato TEXT,
    categoria TEXT,
    qtdNota TEXT,
    fatura TEXT,
    valor TEXT,
    data_compra TEXT,
    data_pagamento TEXT
);

COMMENT ON TABLE raw.faturas IS 'Tabela de ingestão bruta (landing zone) dos dados de faturas.';

-- Limpa a tabela para garantir que a ingestão seja idempotente.
TRUNCATE TABLE raw.faturas;

DO $$
BEGIN
  RAISE NOTICE 'Tabela raw.faturas limpa.';
END
$$;




