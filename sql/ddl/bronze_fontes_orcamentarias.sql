CREATE TABLE IF NOT EXISTS bronze_fontes_orcamentarias (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    contratacao_pncp VARCHAR(50) NOT NULL,
    codigo INTEGER NOT NULL,
    nome VARCHAR(255) NOT NULL,
    descricao TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(contratacao_pncp, codigo)
);

CREATE INDEX IF NOT EXISTS idx_fontes_orcamentarias_contratacao ON bronze_fontes_orcamentarias(contratacao_pncp);
