import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor, fireEvent } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import * as iaManifest from '../../lib/ia-manifest';
import * as duckdbLib from '../../lib/duckdb';
import { __setCatmatEntriesForTest } from '../../lib/catmat';
import PcaHubViewRaw from '../PcaHubView.svelte';
import CatmatSearchRaw from '../CatmatSearch.svelte';

const PcaHubView = PcaHubViewRaw as unknown as Parameters<typeof render>[0];
const CatmatSearch = CatmatSearchRaw as unknown as Parameters<typeof render>[0];

const feature = await loadFeature('features/pca-demand.feature');

function makeBatchResult(cols: number[][]): object {
  const numRows = cols[0]?.length ?? 0;
  return {
    batches: numRows === 0 ? [] : [{
      numRows,
      getChildAt: (colIdx: number) => ({ get: (rowIdx: number) => cols[colIdx][rowIdx] }),
    }],
  };
}

describeFeature(feature, ({ Rule, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Rule('PCA is presented as planned demand, never as contracted fact', ({ RuleScenario }) => {
    RuleScenario(
      'A PCA detail page warns the reader that the data is intent',
      ({ Given, Then, And }) => {
        Given('the user opens a PCA item detail page', async () => {
          render(PcaHubView);
          await tick();
        });

        Then('the page carries a banner warning', async () => {
          await waitFor(() => {
            const alert = document.querySelector('[role="alert"]');
            expect(alert).toBeTruthy();
            expect(alert!.textContent).toMatch(/intenção\/planejamento/);
          }, { timeout: 2000 });
        });

        And('every numeric value on the page is labelled "estimado"', async () => {
          // Rendered with noData=true so no table visible, but the static schema
          // section labels valor_total as "Valor total planejado (estimado, R$)".
          await waitFor(() => {
            expect(document.body.textContent).toMatch(/estimado/i);
          }, { timeout: 2000 });
        });
      },
    );
  });

  // These two rules test backend (Python) behavior, not frontend rendering.
  // Stubs satisfy vitest-cucumber's rule-completeness check; actual coverage
  // lives in Python:
  //   - Partitioning: tests/unit/test_mirror_cmd_current_partition.py
  //       test_current_partition_start_annual_resource
  //       test_annual_partition_differs_from_monthly_floor
  //   - Composite PK:  tests/unit/test_resource_atlas.py
  //       test_pca_canonical_table_uses_composite_pk
  Rule('PCA partitions are annual, not monthly', ({ RuleScenario }) => {
    RuleScenario(
      'The archive layer returns the right yearly partition',
      ({ Given, When, Then, And }) => {
        Given('the registered `pca` resource declares annual partitioning', () => {});
        When('the explorer asks for the latest PCA partition', () => {});
        Then('the answer is a year (e.g. "2026"), not a month', () => {});
        And('the manifest carries `data_particao = "2026"` for that row', () => {});
      },
    );
  });

  Rule('PCA item identity is composite', ({ RuleScenario }) => {
    RuleScenario(
      'Re-ingesting a PCA item with the same composite key replaces',
      ({ Given, When, Then, And }) => {
        Given(
          'an item with `(id_pca_pncp, numero_item) = (X, 1)` was ingested',
          () => {},
        );
        When('the same `(X, 1)` is ingested again with updated `valorTotal`', () => {});
        Then(
          'there is exactly one row for `(X, 1)` in `pca_itens_canonical`',
          () => {},
        );
        And('the new `valorTotal` wins', () => {});
      },
    );
  });

  Rule('A user can pivot from a CATMAT code to its planned demand', ({ RuleScenario }) => {
    RuleScenario(
      'The CATMAT search results page links to the PCA listing',
      ({ Given, When, Then, And }) => {
        Given('the user typed a CATMAT/CATSER code into the search box', async () => {
          __setCatmatEntriesForTest([
            { code: '7510', description: 'PAPEL PARA IMPRESSÃO', type: 'CATMAT' },
          ]);
          render(CatmatSearch);
          await tick();
          const input = screen.getByLabelText('Descrição do item para busca CATMAT');
          await fireEvent.input(input, { target: { value: '7510' } });
        });

        When('the search resolves to a known code', async () => {
          await waitFor(
            () => expect(screen.getByTestId('catmat-results')).toBeTruthy(),
            { timeout: 2000 },
          );
        });

        Then('the result card includes a link "Ver demanda planejada (PCA)"', async () => {
          await waitFor(() => {
            const link = screen.getByTestId('pca-demand-link');
            expect(link.textContent).toMatch(/Ver demanda planejada \(PCA\)/);
          }, { timeout: 2000 });
        });

        And('clicking it lands on `/pca?codigo=<code>`', async () => {
          const link = screen.getByTestId('pca-demand-link') as HTMLAnchorElement;
          expect(link.href).toMatch(/\/pca\?codigo=7510/);
        });
      },
    );

    RuleScenario(
      'The PCA listing groups planned items by year',
      ({ Given, When, Then, And }) => {
        Given('the route `/pca?codigo=7510` is opened', async () => {
          vi.spyOn(iaManifest, 'getLatestParquetInfo').mockResolvedValue({
            url: 'https://archive.org/download/baliza-pncp-pca-2026/pca-2026.parquet',
            dataParticao: '2026',
            identifier: 'baliza-pncp-pca-2026',
          } as Awaited<ReturnType<typeof iaManifest.getLatestParquetInfo>>);

          vi.spyOn(duckdbLib, 'getDuckDB').mockResolvedValue({
            db: null as unknown as Awaited<ReturnType<typeof duckdbLib.getDuckDB>>['db'],
            conn: {
              query: vi.fn().mockResolvedValue(
                // ano_pca=2026, n=12, valor_total=500000
                makeBatchResult([[2026], [12], [500000]]),
              ),
            } as unknown as Awaited<ReturnType<typeof duckdbLib.getDuckDB>>['conn'],
          });

          render(PcaHubView, { props: { codigo: '7510' } });
          await tick();
        });

        When('the data is loaded', async () => {
          await waitFor(
            () => expect(screen.queryByRole('table')).toBeTruthy(),
            { timeout: 3000 },
          );
        });

        Then('items are grouped by `anoPca`', async () => {
          await waitFor(() => {
            const headers = screen.getAllByRole('columnheader');
            const headerText = headers.map((h) => h.textContent ?? '').join(' ');
            expect(headerText).toMatch(/Ano PCA/i);
          }, { timeout: 2000 });
        });

        And('each year shows the total planned `valorTotal` and item count', async () => {
          await waitFor(() => {
            const text = document.body.textContent ?? '';
            expect(text).toMatch(/2026/);
            expect(text).toMatch(/12/);
            // formatBRL(500000) → "R$ 500.000,00" (pt-BR locale)
            expect(text).toMatch(/500[.,]000/);
          }, { timeout: 2000 });
        });

        And('the source PCA URL on the PNCP portal is linked per item', async () => {
          await waitFor(() => {
            const link = screen.getByTestId('pncp-portal-link') as HTMLAnchorElement;
            expect(link.href).toMatch(/pncp\.gov\.br.*anoPca=2026/);
          }, { timeout: 2000 });
        });
      },
    );
  });
});
