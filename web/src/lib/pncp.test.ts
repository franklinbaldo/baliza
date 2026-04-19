import { describe, it, expect, vi, afterEach } from 'vitest';
import {
  PNCP_INVALID,
  parsePncpContract,
  parsePncpPublicacaoList,
} from './pncp';

const validContract = {
  numeroControlePNCP: '12345678000195-1-000001/2024',
  dataPublicacaoPncp: '2024-05-01',
  objetoContratacao: 'Aquisição de materiais',
  valorTotalEstimado: 1000,
  orgaoEntidade: { razaoSocial: 'X', cnpj: '12345678000195' },
  unidadeOrgao: { nomeUnidade: 'U' },
};

describe('pncp parsers', () => {
  afterEach(() => vi.restoreAllMocks());

  it('parses a valid contratação', () => {
    const out = parsePncpContract(validContract);
    expect(out.numeroControlePNCP).toBe('12345678000195-1-000001/2024');
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
