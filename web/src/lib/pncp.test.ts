import { describe, it, expect, vi, afterEach } from 'vitest';
import {
  PNCP_INVALID,
  archivedContratoToInternalContract,
  parsePncpContract,
  parsePncpPublicacaoList,
} from './pncp';

const validContract = {
  numeroControlePNCP: '12345678000195-1-000001/2024',
  dataPublicacaoPncp: '2024-05-01',
  objetoCompra: 'Aquisição de materiais',
  valorTotalEstimado: 1000,
  orgaoEntidade: { razaoSocial: 'X', cnpj: '12345678000195' },
  unidadeOrgao: { nomeUnidade: 'U', codigoIbge: '1100205' },
};

describe('pncp parsers', () => {
  afterEach(() => vi.restoreAllMocks());

  it('parses a valid contratação', () => {
    const out = parsePncpContract(validContract);
    expect(out.numeroControlePNCP).toBe('12345678000195-1-000001/2024');
    expect(out.objetoContratacao).toBe('Aquisição de materiais');
    expect(out.unidadeOrgao.codigoMunicipioIbge).toBe('1100205');
  });

  it('preserves null valorTotalEstimado instead of coercing to zero', () => {
    const out = parsePncpContract({ ...validContract, valorTotalEstimado: null });
    expect(out.valorTotalEstimado).toBeNull();
  });

  it('preserves missing valorTotalEstimado as undefined', () => {
    const { valorTotalEstimado: _unused, ...withoutValue } = validContract;
    void _unused;
    const out = parsePncpContract(withoutValue);
    expect(out.valorTotalEstimado).toBeUndefined();
  });

  it('normalizes publicação list aliases before validating', () => {
    const out = parsePncpPublicacaoList({
      data: [
        {
          numeroControlePNCP: '12345678000195-1-000002/2024',
          dataPublicacaoPncp: '2024-05-02T10:00:00',
          objetoCompra: 'Aquisição publicada no endpoint de publicação',
          orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '12345678000195' },
          unidadeOrgao: {
            nomeUnidade: 'Unidade',
            codigoIbge: '1100205',
          },
          situacaoCompraNome: 'Divulgada no PNCP',
        },
      ],
    });

    expect(out[0].objetoContratacao).toBe('Aquisição publicada no endpoint de publicação');
    expect(out[0].unidadeOrgao.codigoMunicipioIbge).toBe('1100205');
    expect(out[0].situacaoNome).toBe('Divulgada no PNCP');
  });

  it('normalizes archived Parquet rows into the same internal contract schema', () => {
    const out = archivedContratoToInternalContract({
      numero_controle_pncp: '12345678000195-1-000003/2024',
      data_publicacao_pncp: '2024-05-03T10:00:00',
      objeto_contrato: 'Aquisição arquivada no Parquet',
      valor_global: 2500,
      cnpj_orgao: '12345678000195',
      razao_social_orgao: 'Órgão Arquivado',
      nome_unidade: 'Unidade Arquivada',
      municipio_nome: 'Porto Velho',
      uf_sigla: 'RO',
      codigo_ibge: '1100205',
    } as Parameters<typeof archivedContratoToInternalContract>[0]);

    expect(out.objetoContratacao).toBe('Aquisição arquivada no Parquet');
    expect(out.unidadeOrgao.codigoMunicipioIbge).toBe('1100205');
    expect(out.orgaoEntidade.razaoSocial).toBe('Órgão Arquivado');
  });

  it('keeps null data_publicacao_pncp instead of failing internal validation', () => {
    const out = archivedContratoToInternalContract({
      numero_controle_pncp: '12345678000195-1-000099/2024',
      data_publicacao_pncp: null,
      objeto_contrato: 'Linha arquivada sem data de publicação',
      cnpj_orgao: '12345678000195',
      razao_social_orgao: 'Órgão Arquivado',
      nome_unidade: 'Unidade',
      municipio_nome: 'Brasília',
      uf_sigla: 'DF',
      codigo_ibge: '5300108',
    } as Parameters<typeof archivedContratoToInternalContract>[0]);

    expect(out.dataPublicacaoPncp).toBeNull();
    expect(out.objetoContratacao).toBe('Linha arquivada sem data de publicação');
  });

  it('keeps internal aliases when storage aliases are absent', () => {
    const out = parsePncpPublicacaoList({
      data: [
        {
          numeroControlePNCP: '12345678000195-1-000004/2024',
          dataPublicacaoPncp: '2024-05-04',
          objetoCompra: 'Aquisição com aliases internos preservados',
          orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '12345678000195' },
          unidadeOrgao: {
            nomeUnidade: 'Unidade',
            // No `codigoIbge`; only the internal alias is provided.
            codigoMunicipioIbge: '1100205',
          },
          // No `situacaoCompraNome`; only the internal alias is provided.
          situacaoNome: 'Divulgada no PNCP',
        },
      ],
    });

    expect(out[0].unidadeOrgao.codigoMunicipioIbge).toBe('1100205');
    expect(out[0].situacaoNome).toBe('Divulgada no PNCP');
  });

  it('logs PNCP_INVALID telemetry tag and rethrows on malformed contract', () => {
    const info = vi.spyOn(console, 'info').mockImplementation(() => {});
    expect(() => parsePncpContract({ junk: true })).toThrow();
    expect(info).toHaveBeenCalledWith('[pncp] parse failed', {
      reason: PNCP_INVALID,
    });
  });

  it('logs PNCP_INVALID telemetry tag and rethrows on malformed publicação list', () => {
    const info = vi.spyOn(console, 'info').mockImplementation(() => {});
    expect(() => parsePncpPublicacaoList({ data: [{ junk: true }] })).toThrow();
    expect(info).toHaveBeenCalledWith('[pncp] parse failed', {
      reason: PNCP_INVALID,
    });
  });
});
