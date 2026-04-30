import { afterEach, describe, expect, it, vi } from 'vitest';
import { fetchPublicacaoList } from './pncpPublicacao';

describe('fetchPublicacaoList', () => {
  afterEach(() => vi.restoreAllMocks());

  it('can query only the current month for a single modality', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          data: [
            {
              numeroControlePNCP: '12345678000195-1-000002/2026',
              dataPublicacaoPncp: '2026-04-02T10:00:00',
              objetoCompra: 'Aquisição publicada no mês corrente',
              orgaoEntidade: { razaoSocial: 'Órgão', cnpj: '12345678000195' },
              unidadeOrgao: { nomeUnidade: 'Unidade', codigoIbge: '1100205' },
            },
          ],
        }),
        { status: 200 },
      ),
    );
    global.fetch = fetchMock;

    const result = await fetchPublicacaoList(
      { codigoMunicipioIbge: '1100205' },
      {
        dateWindow: 'current-month',
        modalidades: [6],
        tamanhoPagina: 40,
        now: new Date('2026-04-24T12:00:00Z'),
      },
    );

    expect(fetchMock).toHaveBeenCalledOnce();
    const url = new URL(fetchMock.mock.calls[0][0] as string);
    expect(url.searchParams.get('dataInicial')).toBe('20260401');
    expect(url.searchParams.get('dataFinal')).toBe('20260424');
    expect(url.searchParams.get('codigoModalidadeContratacao')).toBe('6');
    expect(url.searchParams.get('codigoMunicipioIbge')).toBe('1100205');
    expect(result[0].objetoContratacao).toBe('Aquisição publicada no mês corrente');
  });
});
