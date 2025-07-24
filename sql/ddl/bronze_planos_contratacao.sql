CREATE TABLE IF NOT EXISTS bronze_planos_contratacao (
    numeroControlePNCP VARCHAR(50) PRIMARY KEY,
    anoPCA INTEGER NOT NULL,
    valorTotal DECIMAL(18, 4),
    orgao_cnpj VARCHAR(14) NOT NULL,
    orgao_razao_social VARCHAR(255) NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_planos_contratacao_orgao ON bronze_planos_contratacao(orgao_cnpj);
CREATE INDEX IF NOT EXISTS idx_planos_contratacao_ano ON bronze_planos_contratacao(anoPCA);
