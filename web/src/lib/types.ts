/**
 * Baliza Core Types
 */

export interface PNCPContract {
  numeroControlePNCP: string;
  dataPublicacaoPncp: string;
  objetoContratacao: string;
  valorTotalEstimado: number;
  orgaoEntidade: {
    razaoSocial: string;
    cnpj: string;
  };
  unidadeOrgao: {
    nomeUnidade: string;
  };
  municipio?: {
    nomeMunicipio: string;
  };
  modalidadeNome?: string;
  situacaoNome?: string;
  anoCompra?: number;
  sequencialCompra?: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  itens?: any[]; 
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
