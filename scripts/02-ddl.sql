--=============================================================================
-- CAMADA RAW: Ingestão de Dados Brutos
--=============================================================================

-- Garante que a tabela seja recriada do zero se o script for executado novamente.
DROP TABLE IF EXISTS raw.faturas CASCADE;

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

DO $$
BEGIN
  RAISE NOTICE 'Tabela raw.faturas criada com sucesso.';
END
$$;

CREATE TABLE IF NOT EXISTS context.transacoes_limpas (
    id_transacao SERIAL PRIMARY KEY,
    emitente_original TEXT,
    documento_original TEXT,
    documento_tratado TEXT,
    qualidade_documento VARCHAR(10), -- VALIDO, NULO, INVALIDO
    contrato TEXT,
    categoria TEXT,
    qtd_nota INTEGER,
    fatura TEXT,
    valor NUMERIC(15, 2),
    data_compra DATE,
    data_pagamento DATE
);

-- Limpa a tabela de destino antes de uma nova inserção para garantir idempotência.
-- A tabela RAW não deve ser truncada aqui para permitir reprocessamentos.
TRUNCATE TABLE context.transacoes_limpas;

DO $$
BEGIN
  RAISE NOTICE 'Tabela context.transacoes_limpas limpa e pronta para receber dados transformados.';
END
$$;
