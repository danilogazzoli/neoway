--=============================================================================
-- CAMADA RAW: Ingestão de Dados Brutos
--=============================================================================

-- Garante que a tabela seja recriada do zero se o script for executado novamente.
DROP TABLE IF EXISTS raw.transacao CASCADE;

CREATE TABLE IF NOT EXISTS raw.transacao (
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

COMMENT ON TABLE raw.transacao IS 'Tabela de ingestão bruta (landing zone) dos dados de transações.';

DO $$
BEGIN
  RAISE NOTICE 'Tabela raw.transacao criada com sucesso.';
END
$$;

--=============================================================================
-- CAMADA CONTEXT: Limpeza dos dados e validação dos documentos
--=============================================================================
DROP TABLE IF EXISTS context.transacoes CASCADE;

CREATE TABLE IF NOT EXISTS context.transacoes (
    id_transacao SERIAL PRIMARY KEY,
    emitente VARCHAR(200),
    documento_original VARCHAR(64),
    documento_tratado VARCHAR(64),
    qualidade_documento VARCHAR(10), -- VALIDO, NULO, INVALIDO
    contrato VARCHAR(100),
    categoria VARCHAR(200),
    qtd_nota INTEGER,
    fatura VARCHAR(10),
    valor NUMERIC(15, 2),
    data_compra DATE,
    data_pagamento DATE
);

DO $$
BEGIN
  RAISE NOTICE 'Tabela context.transacoes limpa e pronta para receber dados transformados.';
END
$$;

--=============================================================================
-- CAMADA APP: Transformação dos dados e modelagem Dimensional
--=============================================================================
DROP TABLE IF EXISTS app.transacao CASCADE;
DROP TABLE IF EXISTS app.emitente CASCADE;
DROP TABLE IF EXISTS app.documento CASCADE;
DROP TABLE IF EXISTS app.categoria CASCADE;

-- Dimensão Categoria
CREATE TABLE IF NOT EXISTS app.categoria (
    id_categoria SERIAL PRIMARY KEY,
    categoria VARCHAR(200) UNIQUE NOT NULL
);

-- Dimensão Emitente
CREATE TABLE IF NOT EXISTS app.emitente (
    id_emitente SERIAL PRIMARY KEY,
    emitente VARCHAR(200) UNIQUE NOT NULL
);

-- Dimensão Documento
CREATE TABLE IF NOT EXISTS app.documento (
    id_documento SERIAL PRIMARY KEY,
    documento_original VARCHAR(64),
    documento_tratado VARCHAR(64),
    qualidade_documento VARCHAR(10),
    UNIQUE (documento_original, documento_tratado, qualidade_documento)
);


-- Tabela Fato Transacao
CREATE TABLE IF NOT EXISTS app.transacao (
    id_transacao INTEGER PRIMARY KEY, 
    id_emitente INTEGER REFERENCES app.emitente(id_emitente),
    id_documento INTEGER REFERENCES app.documento(id_documento),
    id_categoria INTEGER REFERENCES app.categoria(id_categoria),
    contrato VARCHAR(100),
    fatura VARCHAR(10),
    qtd_nota INTEGER,
    valor NUMERIC(15, 2),
    data_compra DATE,
    data_pagamento DATE
);

DO $$
BEGIN
  RAISE NOTICE 'Tabelas da camada APP criadas com sucesso.';
END
$$;
