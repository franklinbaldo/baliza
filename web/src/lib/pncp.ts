// Zod schemas for the PNCP Consulta API boundary. Detail views validate every
// live response here; a parse failure is logged with the `PNCP_INVALID`
// telemetry tag and re-thrown so the caller falls through to the archive
// fallback instead of rendering partial or type-unsafe data.

import { z } from 'zod';
import type { ArchivedContrato } from './archive/schema';

export const PNCP_INVALID = 'pncp_invalid';

// Single contratação as it appears either on its own (detail endpoint) or
// inside the paginated publicação list. Every non-essential field is lenient
// so that unrelated schema drift on ignored columns does not break the UI.
const PNCPContractItemSchema = z
  .object({
    sequencialItem: z.number(),
    descricao: z.string(),
    quantidade: z.number().optional(),
    unidadeMedida: z.string().optional(),
    valorUnitarioEstimado: z.number().optional(),
  })
  .passthrough();

export const PNCPContractSchema = z
  .object({
    numeroControlePNCP: z.string().min(1),
    dataPublicacaoPncp: z.string().min(1),
    objetoContratacao: z.string(),
    valorTotalEstimado: z.number().nullable().optional(),
    valorTotalHomologado: z.number().nullable().optional(),
    dataAberturaProposta: z.string().nullable().optional(),
    dataEncerramentoProposta: z.string().nullable().optional(),
    orgaoEntidade: z
      .object({
        razaoSocial: z.string(),
        cnpj: z.string(),
      })
      .passthrough(),
    unidadeOrgao: z
      .object({
        nomeUnidade: z.string(),
        municipioNome: z.string().nullable().optional(),
        ufSigla: z.string().nullable().optional(),
        codigoMunicipioIbge: z.string().nullable().optional(),
      })
      .passthrough(),
    municipio: z
      .object({ nomeMunicipio: z.string().optional() })
      .passthrough()
      .nullable()
      .optional(),
    modalidadeNome: z.string().nullable().optional(),
    modoDisputaNome: z.string().nullable().optional(),
    situacaoNome: z.string().nullable().optional(),
    linkSistemaOrigem: z.string().nullable().optional(),
    usuarioNome: z.string().nullable().optional(),
    itens: z.array(PNCPContractItemSchema).optional(),
  })
  .passthrough();

const PNCPConsultaContractSchema = z
  .object({
    numeroControlePNCP: z.string().min(1),
    dataPublicacaoPncp: z.string().min(1),
    objetoCompra: z.string(),
    valorTotalEstimado: z.number().nullable().optional(),
    valorTotalHomologado: z.number().nullable().optional(),
    dataAberturaProposta: z.string().nullable().optional(),
    dataEncerramentoProposta: z.string().nullable().optional(),
    orgaoEntidade: z
      .object({
        razaoSocial: z.string(),
        cnpj: z.string(),
      })
      .passthrough(),
    unidadeOrgao: z
      .object({
        nomeUnidade: z.string(),
        municipioNome: z.string().nullable().optional(),
        ufSigla: z.string().nullable().optional(),
        codigoIbge: z.string().nullable().optional(),
      })
      .passthrough(),
    municipio: z
      .object({ nomeMunicipio: z.string().optional() })
      .passthrough()
      .nullable()
      .optional(),
    modalidadeNome: z.string().nullable().optional(),
    modoDisputaNome: z.string().nullable().optional(),
    situacaoCompraNome: z.string().nullable().optional(),
    linkSistemaOrigem: z.string().nullable().optional(),
    usuarioNome: z.string().nullable().optional(),
    itens: z.array(PNCPContractItemSchema).optional(),
  })
  .passthrough();

export const PNCPPublicacaoListSchema = z
  .object({
    data: z.array(PNCPConsultaContractSchema),
  })
  .passthrough();

export type PNCPContract = z.infer<typeof PNCPContractSchema>;

function toInternalContract(
  item: z.infer<typeof PNCPConsultaContractSchema>,
): PNCPContract {
  return PNCPContractSchema.parse({
    ...item,
    objetoContratacao: item.objetoCompra,
    situacaoNome: item.situacaoCompraNome,
    unidadeOrgao: {
      ...item.unidadeOrgao,
      codigoMunicipioIbge: item.unidadeOrgao.codigoIbge,
    },
  });
}

export function archivedContratoToInternalContract(
  row: ArchivedContrato,
  fallbackId = '',
): PNCPContract {
  return PNCPContractSchema.parse({
    numeroControlePNCP: row.numero_controle_pncp ?? fallbackId,
    dataPublicacaoPncp: row.data_publicacao_pncp ?? '',
    objetoContratacao: row.objeto_contrato ?? '',
    valorTotalEstimado: row.valor_global ?? row.valor_inicial ?? null,
    modalidadeNome: row.modalidade_nome ?? undefined,
    linkSistemaOrigem: row.link_sistema_origem ?? undefined,
    usuarioNome: row.usuario_nome ?? undefined,
    orgaoEntidade: {
      razaoSocial: row.razao_social_orgao ?? 'Órgão Arquivado',
      cnpj: row.cnpj_orgao ?? '',
    },
    unidadeOrgao: {
      nomeUnidade: row.nome_unidade ?? '',
      municipioNome: row.municipio_nome ?? '',
      ufSigla: row.uf_sigla ?? '',
      codigoMunicipioIbge: row.codigo_ibge ?? '',
    },
  });
}

export function parsePncpContract(raw: unknown): PNCPContract {
  try {
    return toInternalContract(PNCPConsultaContractSchema.parse(raw));
  } catch (err) {
    console.info('[pncp] parse failed', { reason: PNCP_INVALID });
    throw err;
  }
}

export function parsePncpPublicacaoList(raw: unknown): PNCPContract[] {
  try {
    return PNCPPublicacaoListSchema.parse(raw).data.map(toInternalContract);
  } catch (err) {
    console.info('[pncp] parse failed', { reason: PNCP_INVALID });
    throw err;
  }
}
