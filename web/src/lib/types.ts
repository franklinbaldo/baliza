/**
 * Baliza Core Types
 */

export interface PNCPContractItem {
  sequencialItem: number;
  descricao: string;
  quantidade?: number;
  unidadeMedida?: string;
  valorUnitarioEstimado?: number;
}

export interface PNCPContract {
  numeroControlePNCP: string;
  dataPublicacaoPncp: string;
  dataAberturaProposta?: string;
  dataEncerramentoProposta?: string;
  objetoContratacao: string;
  valorTotalEstimado: number;
  valorTotalHomologado?: number;
  orgaoEntidade: {
    razaoSocial: string;
    cnpj: string;
  };
  unidadeOrgao: {
    nomeUnidade: string;
    municipioNome?: string;
    ufSigla?: string;
    codigoMunicipioIbge?: string;
  };
  municipio?: {
    nomeMunicipio: string;
  };
  modalidadeNome?: string;
  modoDisputaNome?: string;
  situacaoNome?: string;
  anoCompra?: number;
  sequencialCompra?: number;
  linkSistemaOrigem?: string;
  usuarioNome?: string;
  itens?: PNCPContractItem[];
}

export interface PNCPAgency {
  razao_social: string;
  cnpj: string;
}

export interface IBGEResult {
  id: number;
  nome: string;
  microrregiao: {
    mesorregiao: {
      UF: {
        nome: string;
      };
    };
  };
}

export interface SearchResult {
  id: string;
  label: string;
  type: 'agency' | 'city' | 'contract';
}
