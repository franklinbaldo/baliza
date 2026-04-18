// Typed mirror of the parquet schemas declared in
// src/baliza/daily_exporter.py. When the Python schema changes, update these
// interfaces too — they are the source of truth for the archive fallback layer.

export interface ArchivedContrato {
  // Identity
  numero_controle_pncp: string;
  numero_controle_pncp_compra: string | null;
  ano_contrato: number | null;
  sequencial_contrato: number | null;
  ano_compra: number | null;
  sequencial_compra: number | null;
  numero_contrato_empenho: string | null;
  numero_retificacao: number | null;
  processo: string | null;

  // Órgão contratante
  cnpj_orgao: string;
  razao_social_orgao: string | null;
  poder_id: string | null;
  esfera_id: string | null;
  codigo_unidade: string | null;
  nome_unidade: string | null;
  uf_sigla: string | null;
  uf_nome: string | null;
  municipio_nome: string | null;
  codigo_ibge: string | null;

  // Órgão sub-rogado
  cnpj_orgao_subrogado: string | null;
  razao_social_orgao_subrogado: string | null;
  codigo_unidade_subrogada: string | null;
  nome_unidade_subrogada: string | null;
  uf_sigla_subrogada: string | null;

  // Fornecedor
  ni_fornecedor: string | null;
  tipo_pessoa: string | null;
  nome_razao_social_fornecedor: string | null;
  codigo_pais_fornecedor: string | null;
  ni_fornecedor_subcontratado: string | null;
  nome_fornecedor_subcontratado: string | null;
  tipo_pessoa_subcontratada: string | null;

  // Classification
  modalidade_id: number | null;
  modalidade_nome: string | null;
  tipo_contrato_id: number | null;
  tipo_contrato_nome: string | null;
  categoria_processo_id: number | null;
  categoria_processo_nome: string | null;
  receita: boolean | null;

  // Financial
  valor_inicial: number | null;
  valor_parcela: number | null;
  valor_global: number | null;
  valor_acumulado: number | null;
  numero_parcelas: number | null;

  // Dates (ISO strings as read back from DuckDB)
  data_publicacao: string | null;
  data_publicacao_pncp: string | null;
  data_assinatura: string | null;
  data_vigencia_inicio: string | null;
  data_vigencia_fim: string | null;
  data_inclusao: string | null;
  data_atualizacao: string | null;
  data_atualizacao_global: string | null;

  // Text / links
  objeto_contrato: string | null;
  informacao_complementar: string | null;
  link_sistema_origem: string | null;
  identificador_cipi: string | null;
  url_cipi: string | null;

  // Audit + pipeline metadata
  usuario_nome: string | null;
  data_particao: string;
}

export interface ArchivedOrgao {
  cnpj: string;
  razao_social: string | null;
  poder_id: string | null;
  esfera_id: string | null;
  contratos_no_dia: number | null;
  valor_total_no_dia: number | null;
}

export interface ArchivedUnidade {
  codigo_unidade: string;
  cnpj_orgao: string;
  nome_unidade: string | null;
  uf_sigla: string | null;
  municipio_nome: string | null;
  codigo_ibge: string | null;
  contratos_no_dia: number | null;
}

export interface ArchivedFornecedor {
  ni_fornecedor: string;
  tipo_pessoa: string | null;
  nome_razao_social: string | null;
  codigo_pais: string | null;
  contratos_no_dia: number | null;
  valor_total_no_dia: number | null;
}

// Closed set of tables served by the archive layer. New tables require a
// schema interface above AND an entry here — we never accept caller-supplied
// table names through a regex match.
export const ARCHIVED_TABLES = [
  'contratos',
  'orgaos',
  'unidades',
  'fornecedores',
] as const;

export type ArchivedTable = (typeof ARCHIVED_TABLES)[number];

export interface ArchivedTableRowMap {
  contratos: ArchivedContrato;
  orgaos: ArchivedOrgao;
  unidades: ArchivedUnidade;
  fornecedores: ArchivedFornecedor;
}
