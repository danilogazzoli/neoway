--=============================================================================
-- SCRIPT DE PIPELINE DE DADOS: BRONZE -> SILVER -> GOLD
-- Objetivo: Implementar a arquitetura Medallion usando schemas no PostgreSQL.
--=============================================================================

--=============================================================================
-- CAMADA CONTEXT
--=============================================================================

INSERT INTO context.transacoes (
    emitente,
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
    UPPER(UNACCENT(TRIM(emitente))) as emitente,

    documento,
    CASE
        -- ~ é o operador de correspondência de expressão regular (case-sensitive, ou seja, diferencia maiúsculas de minúsculas
        -- ~* : Correspondência de expressão regular, sem diferenciar maiúsculas de minúsculas.
        -- !~ : Não corresponde à expressão regular (case-sensitive).
        -- !~* : Não corresponde à expressão regular (case-insensitive).
        -- '^[a-f0-9]{64}$' : Regex para validar hash SHA-256 - começa (^) e termina ($) com exatamente 64 caracteres que são dígitos de 0 a 9 ou letras de a a f.
        WHEN documento ~ '^[a-f0-9]{64}$' THEN documento -- Regex para validar hash SHA-256
        ELSE NULL
    END AS documento_tratado,
    CASE
        WHEN documento IS NULL OR trim(documento) IN ('', '-', '---', 'SD') THEN 'NULO'
        WHEN documento ~ '^[a-f0-9]{64}$' THEN 'VALIDO'
        ELSE 'INVALIDO'
    END AS qualidade_documento,
    contrato,
    UPPER(UNACCENT(TRIM(categoria))) as categoria,
 
    -- Tratamento para converter 'qtdNota' para inteiro de forma segura
    CASE
        --A string deve, do início (^) ao fim ($), conter um ou mais (+) caracteres que sejam exclusivamente dígitos de 0 a 9 ([0-9])
        WHEN qtdNota ~ '^[0-9]+$' THEN CAST(qtdNota AS INTEGER)
        ELSE NULL
    END AS qtd_nota,
    fatura,
    CAST(REPLACE(REPLACE(valor, '.', ''), ',', '.') AS NUMERIC(15, 2)),
    TO_DATE(NULLIF(TRIM(REPLACE(data_compra, '-', '/')), ''), 'DD/MM/YYYY'),
    TO_DATE(NULLIF(TRIM(REPLACE(data_pagamento, '-', '/')), ''), 'DD/MM/YYYY')
FROM
    raw.transacao;

DO $$
BEGIN
  RAISE NOTICE 'Dados transformados e inseridos em context.transacoes.';
END
$$;

--=============================================================================
-- CAMADA APP : CARGA DAS DIMENSÕES E DA TABELA FATO
--=============================================================================

-- Limpa as tabelas em ordem (fato primeiro) e reinicia as sequências dos IDs.
TRUNCATE TABLE app.transacao, app.documento, app.emitente, app.categoria RESTART IDENTITY CASCADE;
DO $$ BEGIN RAISE NOTICE 'Tabelas da camada APP (Gold) limpas.'; END $$;


-- Carga da Dimensão Categoria
INSERT INTO app.categoria (categoria)
SELECT DISTINCT categoria FROM context.transacoes WHERE categoria IS NOT NULL;

-- Carga da Dimensão Emitente
INSERT INTO app.emitente (emitente)
SELECT DISTINCT emitente FROM context.transacoes WHERE emitente IS NOT NULL;

-- Carga da Dimensão Documento
INSERT INTO app.documento (documento_original, documento_tratado, qualidade_documento)
SELECT DISTINCT documento_original, documento_tratado, qualidade_documento FROM context.transacoes;

-- Carga da Tabela Fato Transacao
INSERT INTO app.transacao (
    id_transacao,
    id_emitente,
    id_documento,
    id_categoria,
    contrato,
    fatura,
    qtd_nota,
    valor,
    data_compra,
    data_pagamento
)
SELECT
    t.id_transacao,
    e.id_emitente,
    d.id_documento,
    c.id_categoria,
    t.contrato,
    t.fatura,
    t.qtd_nota,
    t.valor,
    t.data_compra,
    t.data_pagamento
FROM context.transacoes t
LEFT JOIN app.emitente e ON t.emitente = e.emitente
LEFT JOIN app.documento d ON t.documento_original = d.documento_original AND t.documento_tratado = d.documento_tratado AND t.qualidade_documento = d.qualidade_documento
LEFT JOIN app.categoria c ON t.categoria = c.categoria;

DO $$
BEGIN
  RAISE NOTICE 'Camada APP (Gold) populada com sucesso.';
END
$$;