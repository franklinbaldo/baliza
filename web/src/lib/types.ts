/**
 * Baliza Core Types
 */

export type { PNCPContract, PNCPContractItem } from './pncp';

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
