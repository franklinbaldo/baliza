CREATE TABLE IF NOT EXISTS bronze_atas (
    numeroControlePNCPAta VARCHAR(50) PRIMARY KEY,
    dataAssinatura DATE NOT NULL,
    vigenciaInicio DATE NOT NULL,
    vigenciaFim DATE NOT NULL,
    orgao_cnpj VARCHAR(14) NOT NULL,
    orgao_razao_social VARCHAR(255) NOT NULL,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_atas_orgao ON bronze_atas(orgao_cnpj);
CREATE INDEX IF NOT EXISTS idx_atas_vigencia ON bronze_atas(vigenciaInicio, vigenciaFim);
