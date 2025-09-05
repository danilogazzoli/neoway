--=============================================================================
-- SCRIPT DE PIPELINE DE DADOS: BRONZE -> SILVER -> GOLD
-- Objetivo: Implementar a arquitetura Medallion usando schemas no PostgreSQL.
--=============================================================================

--=============================================================================
-- CAMADA CONTEXT
--=============================================================================

INSERT INTO context.transacoes_limpas (
    emitente_original,
    documento_original,
    documento_tratado,
    qualidade_documento,
    contrato,
    categoria,
    qtd_nota,
    fatura,
    valor,
    data_compra,
    data_pagamento
)
SELECT
    emitente,

    documento,
    CASE
        WHEN documento ~ '^[a-f0-9]{64}$' THEN documento -- Regex para validar hash SHA-256
        ELSE NULL
    END AS documento_tratado,
    CASE
        WHEN documento IS NULL OR trim(documento) IN ('', '-', '---', 'SD') THEN 'NULO'
        WHEN documento ~ '^[a-f0-9]{64}$' THEN 'VALIDO'
        ELSE 'INVALIDO'
    END AS qualidade_documento,
    contrato,
 
    categoria,
 
    -- Tratamento para converter 'qtdNota' para inteiro de forma segura
    CASE
        WHEN qtdNota ~ '^[0-9]+$' THEN CAST(qtdNota AS INTEGER)
        ELSE NULL
    END AS qtd_nota,
    fatura,
    CAST(REPLACE(REPLACE(valor, '.', ''), ',', '.') AS NUMERIC(15, 2)),
    TO_DATE(NULLIF(TRIM(REPLACE(data_compra, '-', '/')), ''), 'DD/MM/YYYY'),
    TO_DATE(NULLIF(TRIM(REPLACE(data_pagamento, '-', '/')), ''), 'DD/MM/YYYY')
FROM
    raw.faturas;

DO $$
BEGIN
  RAISE NOTICE 'Dados transformados e inseridos em context.transacoes_limpas.';
END
$$;
