import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import { queryParquetFallback } from '../../lib/parquetFallback';
import AgencyDetailViewRaw from '../AgencyDetailView.svelte';
import CityDetailViewRaw from '../CityDetailView.svelte';
import ContractDetailViewRaw from '../ContractDetailView.svelte';

const AgencyDetailView = AgencyDetailViewRaw as unknown as Parameters<typeof render>[0];
const CityDetailView = CityDetailViewRaw as unknown as Parameters<typeof render>[0];
const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/detail-view-fallback.feature');

const fallbackMock = vi.mocked(queryParquetFallback);

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

const ARCHIVED_AGENCY_ROW = {
  numero_controle_pncp: '00000000000191-1-000001/2024',
  objeto_contratacao: 'Aquisição arquivada – órgão',
  data_publicacao_pncp: '2024-01-15T00:00:00',
  valor_total_estimado: 9999,
  orgao_cnpj: '00000000000191',
  orgao_razao_social: 'Órgão Arquivado',
};

const ARCHIVED_CITY_ROW = {
  numero_controle_pncp: '00000000000191-1-000002/2024',
  objeto_contratacao: 'Aquisição arquivada – município',
  data_publicacao_pncp: '2024-02-20T00:00:00',
  valor_total_estimado: 5555,
  municipio_nome: 'São Paulo',
  uf_sigla: 'SP',
  codigo_municipio_ibge: '3550308',
};

const ARCHIVED_CONTRACT_ROW = {
  numero_controle_pncp: '00000000000191-1-000001-1',
  objeto_contratacao: 'Aquisição arquivada – contratação',
  data_publicacao_pncp: '2024-03-01T00:00:00',
  valor_total_estimado: 7777,
  modalidade_nome: 'Pregão Eletrônico',
  situacao_nome: 'Publicada',
  orgao_cnpj: '00000000000191',
  orgao_razao_social: 'Órgão Arquivado',
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    fallbackMock.mockReset();
    fallbackMock.mockResolvedValue(null);
    setUrlQuery('');
  });

  Scenario('Agency view falls back to Parquet when PNCP fails', ({ Given, And, When, Then }) => {
    Given('the PNCP API is unavailable for cnpj "00000000000191"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns one contract row for cnpj "00000000000191"', () => {
      fallbackMock.mockResolvedValue({
        rows: [ARCHIVED_AGENCY_ROW],
        dataParticao: '2024-12-01',
      });
    });

    When('the agency detail view mounts for cnpj "00000000000191"', async () => {
      setUrlQuery('cnpj=00000000000191');
      render(AgencyDetailView);
      await tick();
    });

    Then('I should see an info banner about PNCP indisponível', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/PNCP indisponível/i);
        },
        { timeout: 2000 },
      );
    });

    And('I should see the archived contract from the Parquet snapshot', () => {
      expect(screen.getByText(/Aquisição arquivada – órgão/)).toBeTruthy();
    });
  });

  Scenario('Agency view shows combined error when both fail', ({ Given, And, When, Then }) => {
    Given('the PNCP API is unavailable for cnpj "00000000000191"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns no rows for cnpj "00000000000191"', () => {
      fallbackMock.mockResolvedValue(null);
    });

    When('the agency detail view mounts for cnpj "00000000000191"', async () => {
      setUrlQuery('cnpj=00000000000191');
      render(AgencyDetailView);
      await tick();
    });

    Then('I should see an error banner about arquivo histórico', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/arquivo histórico/i);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Agency view does not query Parquet when PNCP succeeds', ({ Given, When, Then }) => {
    Given('the PNCP API returns one contract for cnpj "00000000000191"', () => {
      global.fetch = vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            data: [
              {
                numeroControlePNCP: '00000000000191-1-000001/2024',
                dataPublicacaoPncp: '2024-06-01T00:00:00',
                objetoContratacao: 'Contrato PNCP ao vivo',
                valorTotalEstimado: 10000,
                orgaoEntidade: { razaoSocial: 'Órgão Vivo', cnpj: '00000000000191' },
                unidadeOrgao: { nomeUnidade: 'Unidade' },
              },
            ],
          }),
          { status: 200 },
        ),
      );
    });

    When('the agency detail view mounts for cnpj "00000000000191"', async () => {
      setUrlQuery('cnpj=00000000000191');
      render(AgencyDetailView);
      await tick();
    });

    Then('the Parquet fallback should not be called', async () => {
      await waitFor(
        () => expect(screen.getByText(/Contrato PNCP ao vivo/)).toBeTruthy(),
        { timeout: 2000 },
      );
      expect(fallbackMock).not.toHaveBeenCalled();
    });
  });

  Scenario('City view falls back to Parquet when PNCP fails', ({ Given, And, When, Then }) => {
    Given('the PNCP API is unavailable for ibge "3550308"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns one contract row for ibge "3550308"', () => {
      fallbackMock.mockResolvedValue({
        rows: [ARCHIVED_CITY_ROW],
        dataParticao: '2024-12-01',
      });
    });

    When('the city detail view mounts for ibge "3550308"', async () => {
      setUrlQuery('ibge=3550308');
      render(CityDetailView);
      await tick();
    });

    Then('I should see an info banner about PNCP indisponível', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/PNCP indisponível/i);
        },
        { timeout: 2000 },
      );
    });

    And('I should see the archived contract from the Parquet snapshot', () => {
      expect(screen.getByText(/Aquisição arquivada – município/)).toBeTruthy();
    });
  });

  Scenario('City view shows combined error when both fail', ({ Given, And, When, Then }) => {
    Given('the PNCP API is unavailable for ibge "3550308"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns no rows for ibge "3550308"', () => {
      fallbackMock.mockResolvedValue(null);
    });

    When('the city detail view mounts for ibge "3550308"', async () => {
      setUrlQuery('ibge=3550308');
      render(CityDetailView);
      await tick();
    });

    Then('I should see an error banner about arquivo histórico', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/arquivo histórico/i);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Contract view falls back to Parquet when PNCP fails', ({ Given, And, When, Then }) => {
    Given('the PNCP API is unavailable for id "00000000000191-1-000001-1"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns one row for id "00000000000191-1-000001-1"', () => {
      fallbackMock.mockResolvedValue({
        rows: [ARCHIVED_CONTRACT_ROW],
        dataParticao: '2024-12-01',
      });
    });

    When('the contract detail view mounts for id "00000000000191-1-000001-1"', async () => {
      setUrlQuery('id=00000000000191-1-000001-1');
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an info banner about PNCP indisponível', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/PNCP indisponível/i);
        },
        { timeout: 2000 },
      );
    });

    And('I should see the archived contract objeto', () => {
      const matches = screen.getAllByText(/Aquisição arquivada – contratação/);
      expect(matches.length).toBeGreaterThan(0);
    });
  });

  Scenario('Contract view shows combined error when both fail', ({ Given, And, When, Then }) => {
    Given('the PNCP API is unavailable for id "00000000000191-1-000001-1"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns no rows for id "00000000000191-1-000001-1"', () => {
      fallbackMock.mockResolvedValue(null);
    });

    When('the contract detail view mounts for id "00000000000191-1-000001-1"', async () => {
      setUrlQuery('id=00000000000191-1-000001-1');
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an error banner about arquivo histórico', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/arquivo histórico/i);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Contract view accepts slash-separated PNCP IDs and reaches the fallback', ({ Given, And, When, Then }) => {
    const slashId = '00000000000191-1-000001/2024';
    const slashRow = { ...ARCHIVED_CONTRACT_ROW, numero_controle_pncp: slashId };

    Given('the PNCP API is unavailable for id "00000000000191-1-000001/2024"', () => {
      global.fetch = vi.fn().mockResolvedValue(new Response('boom', { status: 503 }));
    });

    And('the Parquet fallback returns one row for id "00000000000191-1-000001/2024"', () => {
      fallbackMock.mockResolvedValue({
        rows: [slashRow],
        dataParticao: '2024-12-01',
      });
    });

    When('the contract detail view mounts for id "00000000000191-1-000001/2024"', async () => {
      setUrlQuery(`id=${encodeURIComponent(slashId)}`);
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an info banner about PNCP indisponível', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/PNCP indisponível/i);
        },
        { timeout: 2000 },
      );
    });

    And('I should see the archived contract objeto', () => {
      const matches = screen.getAllByText(/Aquisição arquivada – contratação/);
      expect(matches.length).toBeGreaterThan(0);
    });
  });
});
