CREATE TABLE IF NOT EXISTS bronze_contratacoes (
    numeroControlePNCP VARCHAR(50) PRIMARY KEY,
    anoContratacao INTEGER NOT NULL,
    dataPublicacaoPNCP DATE NOT NULL,
    modalidadeId INTEGER NOT NULL,
    valorTotalEstimado DECIMAL(18, 4),
    orgao_cnpj VARCHAR(14) NOT NULL,
    orgao_razao_social VARCHAR(255) NOT NULL,
    orgao_poder_id VARCHAR(10),
    orgao_esfera_id VARCHAR(10),
    unidade_codigo VARCHAR(20),
    unidade_nome VARCHAR(255),
    unidade_uf_sigla VARCHAR(2),
    unidade_municipio_nome VARCHAR(100),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    month_partition VARCHAR(7) GENERATED ALWAYS AS (strftime('%Y-%m', dataPublicacaoPNCP)) STORED
);

CREATE INDEX IF NOT EXISTS idx_contratacoes_month ON bronze_contratacoes(month_partition);
CREATE INDEX IF NOT EXISTS idx_contratacoes_modalidade ON bronze_contratacoes(modalidadeId);
CREATE INDEX IF NOT EXISTS idx_contratacoes_orgao ON bronze_contratacoes(orgao_cnpj);
