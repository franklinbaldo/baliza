import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import ContractDetailViewRaw from '../ContractDetailView.svelte';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/contract-detail.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

const RICH_PAYLOAD = {
  numeroControlePNCP: '00000000000191-1-000001/2024',
  dataPublicacaoPncp: '2025-01-15T00:00:00',
  dataAberturaProposta: '2025-01-20T09:00:00',
  dataEncerramentoProposta: '2025-02-05T17:00:00',
  objetoContratacao: 'Aquisição de materiais de expediente',
  valorTotalEstimado: 1500,
  valorTotalHomologado: 1450,
  modalidadeNome: 'Pregão Eletrônico',
  modoDisputaNome: 'Aberto',
  situacaoNome: 'Publicada',
  orgaoEntidade: {
    razaoSocial: 'Prefeitura Municipal',
    cnpj: '00000000000191',
  },
  unidadeOrgao: {
    nomeUnidade: 'Departamento de Compras',
    municipioNome: 'São Paulo',
    ufSigla: 'SP',
    codigoMunicipioIbge: '3550308',
  },
  linkSistemaOrigem: 'https://origem.exemplo.gov.br/compras/1',
  usuarioNome: 'Sistema PNCP',
  itens: [
    {
      sequencialItem: 1,
      descricao: 'Caneta esferográfica azul',
      quantidade: 100,
      unidadeMedida: 'UN',
      valorUnitarioEstimado: 2.5,
    },
    {
      sequencialItem: 2,
      descricao: 'Papel A4 75g',
      quantidade: 50,
      unidadeMedida: 'RS',
      valorUnitarioEstimado: 25,
    },
  ],
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
  });

  Scenario('Missing ID shows EntityNotFound', ({ Given, When, Then }) => {
    Given('the URL has no id parameter', () => {
      setUrlQuery('');
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see "CONTRATAÇÃO não encontrada"', async () => {
      await waitFor(() =>
        expect(screen.getByText('CONTRATAÇÃO não encontrada')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch pending shows skeleton', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001/2024"', () => {
      setUrlQuery('id=00000000000191-1-000001/2024');
    });

    And('the fetch is pending', () => {
      global.fetch = vi.fn().mockReturnValue(new Promise(() => {}));
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an aria-busy element', async () => {
      await waitFor(() =>
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy(),
      );
    });
  });

  Scenario('Fetch error shows AlertBanner', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001/2024"', () => {
      setUrlQuery('id=00000000000191-1-000001/2024');
    });

    And('the fetch throws "Contratação não localizada"', () => {
      global.fetch = vi
        .fn()
        .mockResolvedValue(new Response('nope', { status: 404 }));
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see an alert banner with the error message', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/PNCP indisponível/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Successful fetch renders all detail blocks', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001/2024"', () => {
      setUrlQuery('id=00000000000191-1-000001/2024');
    });

    And('the PNCP API returns a valid contract payload', () => {
      global.fetch = vi.fn().mockImplementation(async () =>
        new Response(JSON.stringify(RICH_PAYLOAD), { status: 200 }),
      );
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see the "Detalhes da Contratação" block', async () => {
      await waitFor(
        () => expect(screen.getByText('Detalhes da Contratação')).toBeTruthy(),
        { timeout: 2000 },
      );
    });

    And('I should see the "Órgão Responsável" block', () => {
      expect(screen.getByText('Órgão Responsável')).toBeTruthy();
    });

    And('I should see the "Valores" block', () => {
      expect(screen.getByText('Valores')).toBeTruthy();
    });

    And('I should see the "Itens" block', () => {
      expect(screen.getByText('Itens')).toBeTruthy();
    });

    And('I should see the "Fontes e Metadados" block', () => {
      expect(screen.getByText('Fontes e Metadados')).toBeTruthy();
    });
  });

  Scenario('Successful fetch renders formatted currency and links', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001/2024"', () => {
      setUrlQuery('id=00000000000191-1-000001/2024');
    });

    And('the PNCP API returns a valid contract payload', () => {
      global.fetch = vi.fn().mockImplementation(async () =>
        new Response(JSON.stringify(RICH_PAYLOAD), { status: 200 }),
      );
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('I should see the BRL-formatted valor "R$ 1.500,00"', async () => {
      await waitFor(
        () => {
          const matches = Array.from(document.querySelectorAll('dd')).map(
            (el) => el.textContent?.replace(/\s+/g, ' ').trim() ?? '',
          );
          expect(matches.some((t) => /R\$\s*1\.500,00/.test(t))).toBe(true);
        },
        { timeout: 2000 },
      );
    });

    And('I should see a link to the agency page with cnpj "00000000000191"', () => {
      const link = document.querySelector(
        'a[href="/baliza/orgao?cnpj=00000000000191"]',
      );
      expect(link).toBeTruthy();
    });

    And('I should see a link to the municipality page with ibge "3550308"', () => {
      const link = document.querySelector(
        'a[href="/baliza/municipio?ibge=3550308"]',
      );
      expect(link).toBeTruthy();
    });

    And('I should see an external link to the origin system', () => {
      const link = document.querySelector(
        'a[href="https://origem.exemplo.gov.br/compras/1"]',
      ) as HTMLAnchorElement | null;
      expect(link).toBeTruthy();
      expect(link?.getAttribute('rel')).toMatch(/noopener/);
    });
  });

  Scenario('Slash-form id uses the year segment when calling PNCP', ({ Given, And, When, Then }) => {
    Given('the URL has id "00000000000191-1-000001/2024"', () => {
      setUrlQuery('id=00000000000191-1-000001%2F2024');
    });

    And('the PNCP API returns a valid contract payload', () => {
      global.fetch = vi.fn().mockImplementation(async () =>
        new Response(JSON.stringify(RICH_PAYLOAD), { status: 200 }),
      );
    });

    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });

    Then('the PNCP consulta URL should contain "/compras/2024/000001"', async () => {
      await waitFor(
        () => {
          const fetchMock = global.fetch as unknown as ReturnType<typeof vi.fn>;
          const calls = fetchMock.mock.calls.map((args: unknown[]) => String(args[0]));
          expect(calls.some((u: string) => u.includes('/compras/2024/000001'))).toBe(true);
        },
        { timeout: 2000 },
      );
    });
  });
});
