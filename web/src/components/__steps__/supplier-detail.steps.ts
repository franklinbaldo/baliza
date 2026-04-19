import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import * as parquetFallback from '../../lib/parquetFallback';
import SupplierDetailViewRaw from '../SupplierDetailView.svelte';

const SupplierDetailView = SupplierDetailViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/supplier-detail.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

function baseRow(overrides: Partial<Record<string, unknown>> = {}) {
  return {
    numero_controle_pncp: '00000000000191-1-000001/2024',
    data_publicacao_pncp: '2025-01-15',
    objeto_contrato: 'Prestação de serviços',
    valor_global: 1000,
    valor_inicial: 1000,
    cnpj_orgao: '00000000000191',
    razao_social_orgao: 'Prefeitura Exemplo',
    nome_unidade: 'Secretaria de Compras',
    ni_fornecedor: '12345678000195',
    nome_razao_social_fornecedor: 'Fornecedora Exemplo Ltda',
    ...overrides,
  };
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
  });

  Scenario('Missing CNPJ shows EntityNotFound', ({ Given, When, Then }) => {
    Given('the URL has no cnpj parameter', () => {
      setUrlQuery('');
    });

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see "FORNECEDOR não encontrada"', async () => {
      await waitFor(() =>
        expect(screen.getByText('FORNECEDOR não encontrada')).toBeTruthy(),
      );
    });
  });

  Scenario('Archive pending shows skeleton', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "12345678000195"', () => {
      setUrlQuery('cnpj=12345678000195');
    });

    And('the archive lookup is pending', () => {
      vi.spyOn(parquetFallback, 'queryParquetFallback').mockReturnValue(
        new Promise(() => {}),
      );
    });

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see an aria-busy element', async () => {
      await waitFor(() =>
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy(),
      );
    });
  });

  Scenario('Archive failure shows AlertBanner', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "12345678000195"', () => {
      setUrlQuery('cnpj=12345678000195');
    });

    And('the archive lookup fails', () => {
      vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
        ok: false,
        reason: 'no_manifest',
      });
    });

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see an alert banner with the error message', async () => {
      await waitFor(
        () => {
          const alert = screen.getByRole('alert');
          expect(alert.textContent).toMatch(/arquivo histórico/i);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Valid CNPJ without contracts shows EmptyState', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "12345678000195"', () => {
      setUrlQuery('cnpj=12345678000195');
    });

    And('the archive lookup returns empty', () => {
      vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
        ok: false,
        reason: 'empty',
      });
    });

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see "Nenhum contrato arquivado"', async () => {
      await waitFor(
        () => expect(screen.getByText('Nenhum contrato arquivado')).toBeTruthy(),
        { timeout: 2000 },
      );
    });
  });

  Scenario('Contracts render as links', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "12345678000195"', () => {
      setUrlQuery('cnpj=12345678000195');
    });

    And(
      'the archive lookup returns a contract with id "00000000000191-1-000001/2024"',
      () => {
        vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
          ok: true,
          rows: [baseRow()],
          dataParticao: '2025-01-31',
        });
      },
    );

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see a link to that contract', async () => {
      await waitFor(
        () => {
          const link = document.querySelector(
            'a[href*="00000000000191-1-000001/2024"]',
          );
          expect(link).toBeTruthy();
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Average ticket renders from archive data', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "12345678000195"', () => {
      setUrlQuery('cnpj=12345678000195');
    });

    And('the archive lookup returns contracts with values 1000 and 2000', () => {
      vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
        ok: true,
        rows: [
          baseRow({
            numero_controle_pncp: '00000000000191-1-000001/2024',
            valor_global: 1000,
            valor_inicial: 1000,
          }),
          baseRow({
            numero_controle_pncp: '00000000000191-1-000002/2024',
            valor_global: 2000,
            valor_inicial: 2000,
          }),
        ],
        dataParticao: '2025-01-31',
      });
    });

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see the avg-ticket card showing "R$ 1.500,00"', async () => {
      await waitFor(
        () => {
          const card = screen.getByTestId('avg-ticket');
          expect(card.textContent).toMatch(/R\$\s*1\.500,00/);
        },
        { timeout: 2000 },
      );
    });
  });

  Scenario('Competing suppliers card is always rendered', ({ Given, And, When, Then }) => {
    Given('the URL has cnpj "12345678000195"', () => {
      setUrlQuery('cnpj=12345678000195');
    });

    And(
      'the archive lookup returns a contract with id "00000000000191-1-000001/2024"',
      () => {
        vi.spyOn(parquetFallback, 'queryParquetFallback').mockResolvedValue({
          ok: true,
          rows: [baseRow()],
          dataParticao: '2025-01-31',
        });
      },
    );

    When('the supplier detail view mounts', async () => {
      render(SupplierDetailView);
      await tick();
    });

    Then('I should see the competing-suppliers card', async () => {
      await waitFor(
        () => expect(screen.getByTestId('competing-suppliers')).toBeTruthy(),
        { timeout: 2000 },
      );
    });
  });
});
