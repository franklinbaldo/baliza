CREATE TABLE IF NOT EXISTS bronze_contratos (
    numeroControlePNCP VARCHAR(50) PRIMARY KEY,
    dataAssinatura DATE NOT NULL,
    valorGlobal DECIMAL(18, 4),
    orgao_cnpj VARCHAR(14) NOT NULL,
    orgao_razao_social VARCHAR(255) NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contratos_orgao ON bronze_contratos(orgao_cnpj);
CREATE INDEX IF NOT EXISTS idx_contratos_data ON bronze_contratos(dataAssinatura);
