import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import * as parquetFallback from '../../lib/parquetFallback';
import type { ArchivedContrato } from '../../lib/archive/schema';
import AtasViewRaw from '../AtasView.svelte';

const AtasView = AtasViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/atas.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

function futureDate(): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() + 1);
  return d.toISOString().slice(0, 10);
}

function vigentRow(overrides: Partial<Record<string, unknown>> = {}): ArchivedContrato {
  return {
    numero_controle_pncp: '00000000000191-1-000001/2024',
    data_publicacao_pncp: '2025-01-15',
    data_vigencia_inicio: '2025-01-15',
    data_vigencia_fim: futureDate(),
    objeto_contrato: 'Registro de preços para papel A4 sulfite',
    valor_global: 120000,
    valor_inicial: 120000,
    cnpj_orgao: '00000000000191',
    razao_social_orgao: 'Prefeitura Exemplo',
    nome_unidade: 'Secretaria de Compras',
    ...overrides,
  } as unknown as ArchivedContrato;
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
  });

  Scenario('Empty objeto shows search prompt', ({ Given, When, Then }) => {
    Given('the URL has no objeto parameter', () => {
      setUrlQuery('');
    });

    When('the atas view mounts', async () => {
      render(AtasView);
      await tick();
    });

    Then('I should see "Digite o objeto a pesquisar"', async () => {
      await waitFor(() =>
        expect(screen.getByText('Digite o objeto a pesquisar')).toBeTruthy(),
      );
    });
  });

  Scenario('Short objeto shows search prompt', ({ Given, When, Then }) => {
    Given('the URL has objeto "ab"', () => {
      setUrlQuery('objeto=ab');
    });

    When('the atas view mounts', async () => {
      render(AtasView);
      await tick();
    });

    Then('I should see "Digite o objeto a pesquisar"', async () => {
      await waitFor(() =>
        expect(screen.getByText('Digite o objeto a pesquisar')).toBeTruthy(),
      );
    });
  });

  Scenario('Archive pending shows skeleton', ({ Given, And, When, Then }) => {
    Given('the URL has objeto "papel A4"', () => {
      setUrlQuery('objeto=papel%20A4');
    });

    And('the archive lookup is pending', () => {
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockReturnValue(
        new Promise(() => {}),
      );
    });

    When('the atas view mounts', async () => {
      render(AtasView);
      await tick();
    });

    Then('I should see a loading skeleton', async () => {
      await waitFor(() =>
        expect(document.querySelector('[aria-busy="true"]')).toBeTruthy(),
      );
    });
  });

  Scenario('Archive failure shows AlertBanner', ({ Given, And, When, Then }) => {
    Given('the URL has objeto "papel A4"', () => {
      setUrlQuery('objeto=papel%20A4');
    });

    And('the archive lookup fails', () => {
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockResolvedValue({
        ok: false,
        reason: 'no_manifest',
      });
    });

    When('the atas view mounts', async () => {
      render(AtasView);
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

  Scenario('Valid objeto without matches shows EmptyState', ({ Given, And, When, Then }) => {
    Given('the URL has objeto "papel A4"', () => {
      setUrlQuery('objeto=papel%20A4');
    });

    And('the archive lookup returns empty', () => {
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockResolvedValue({
        ok: false,
        reason: 'empty',
      });
    });

    When('the atas view mounts', async () => {
      render(AtasView);
      await tick();
    });

    Then('I should see "Nenhuma ata vigente encontrada"', async () => {
      await waitFor(
        () => expect(screen.getByText('Nenhuma ata vigente encontrada')).toBeTruthy(),
        { timeout: 2000 },
      );
    });
  });

  Scenario('Matching contracts render as atas-list', ({ Given, And, When, Then }) => {
    Given('the URL has objeto "papel A4"', () => {
      setUrlQuery('objeto=papel%20A4');
    });

    And('the archive lookup returns a vigent contract', () => {
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockResolvedValue({
        ok: true,
        rows: [vigentRow()],
        dataParticao: '2025-01-31',
      });
    });

    When('the atas view mounts', async () => {
      render(AtasView);
      await tick();
    });

    Then('I should see the atas-list', async () => {
      await waitFor(
        () => expect(screen.getByTestId('atas-list')).toBeTruthy(),
        { timeout: 2000 },
      );
    });
  });

  Scenario('Query always includes the today-date gte filter', ({ Given, When, Then }) => {
    const spy = vi.fn().mockResolvedValue({
      ok: true,
      rows: [vigentRow()],
      dataParticao: '2025-01-31',
    });

    Given('the URL has objeto "papel A4"', () => {
      setUrlQuery('objeto=papel%20A4');
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockImplementation(spy);
    });

    When('the atas view mounts', async () => {
      render(AtasView);
      await tick();
    });

    Then('the archive query includes a data_vigencia_fim gte filter', async () => {
      await waitFor(() => expect(spy).toHaveBeenCalled(), { timeout: 2000 });
      const [, filters] = spy.mock.calls[0] ?? [];
      expect(Array.isArray(filters)).toBe(true);
      const gteFilter = (filters as Array<{ column: string; op: string }>).find(
        (f) => f.column === 'data_vigencia_fim' && f.op === 'gte',
      );
      expect(gteFilter).toBeTruthy();
    });
  });
});
