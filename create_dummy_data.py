import pandas as pd
import numpy as np

# Create dummy pncp_content data
pncp_content_data = {
    "id": range(10),
    "reference_count": np.random.randint(1, 10, size=10),
    "content_size_bytes": np.random.randint(100, 1000, size=10),
    "numeroControlePNCP": [f"pncp_{i}" for i in range(10)],
    "anoContratacao": [2023] * 10,
    "dataPublicacaoPNCP": pd.to_datetime(["2023-01-01"] * 10),
    "dataAtualizacaoPNCP": pd.to_datetime(["2023-01-01"] * 10),
    "horaAtualizacaoPNCP": ["12:00:00"] * 10,
    "sequencialOrgao": [f"orgao_{i}" for i in range(10)],
    "cnpjOrgao": [f"cnpj_{i}" for i in range(10)],
    "siglaOrgao": [f"sigla_{i}" for i in range(10)],
    "nomeOrgao": [f"nome_orgao_{i}" for i in range(10)],
    "sequencialUnidade": [f"unidade_{i}" for i in range(10)],
    "codigoUnidade": [f"codigo_unidade_{i}" for i in range(10)],
    "siglaUnidade": [f"sigla_unidade_{i}" for i in range(10)],
    "nomeUnidade": [f"nome_unidade_{i}" for i in range(10)],
    "codigoEsfera": ["1"] * 10,
    "nomeEsfera": ["Federal"] * 10,
    "codigoPoder": ["1"] * 10,
    "nomePoder": ["Executivo"] * 10,
    "codigoMunicipio": ["1"] * 10,
    "nomeMunicipio": ["Brasilia"] * 10,
    "uf": ["DF"] * 10,
    "amparoLegalId": [1] * 10,
    "amparoLegalNome": ["Lei 8.666"] * 10,
    "modalidadeId": [1] * 10,
    "modalidadeNome": ["Pregao"] * 10,
    "numeroContratacao": [f"contratacao_{i}" for i in range(10)],
    "processo": [f"processo_{i}" for i in range(10)],
    "objetoContratacao": [f"objeto_{i}" for i in range(10)],
    "codigoSituacaoContratacao": [1] * 10,
    "nomeSituacaoContratacao": ["Aberta"] * 10,
    "valorTotalEstimado": np.random.uniform(1000, 10000, size=10),
    "informacaoComplementar": [""] * 10,
    "dataAssinatura": pd.to_datetime(["2023-01-01"] * 10),
    "dataVigenciaInicio": pd.to_datetime(["2023-01-01"] * 10),
    "dataVigenciaFim": pd.to_datetime(["2023-01-31"] * 10),
    "numeroControlePNCPContrato": [f"contrato_{i}" for i in range(10)],
    "justificativa": [""] * 10,
    "fundamentacaoLegal": [""] * 10,
    "endpoint_category": ["contratacoes"] * 10,
    "response_json": ["{}"] * 10,
}
pncp_content_df = pd.DataFrame(pncp_content_data)
pncp_content_df.to_parquet("data/01_raw/pncp_content.parquet")

# Create dummy pncp_requests data
pncp_requests_data = {
    "content_id": range(10),
    "endpoint_name": ["contratacoes"] * 10,
    "data_date": pd.to_datetime(["2023-01-01"] * 10),
    "url_path": ["/contratacoes"] * 10,
    "response_time": np.random.uniform(0.1, 1, size=10),
    "extracted_at": pd.to_datetime(["2023-01-01"] * 10),
}
pncp_requests_df = pd.DataFrame(pncp_requests_data)
pncp_requests_df.to_parquet("data/01_raw/pncp_requests.parquet")
