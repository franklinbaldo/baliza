import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor, fireEvent } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './_shared';
import ContractDetailViewRaw from '../../ContractDetailView.svelte';
import AtasViewRaw from '../../AtasView.svelte';
import CatmatSearchRaw from '../../CatmatSearch.svelte';
import DispensasViewRaw from '../../DispensasView.svelte';
import CompararViewRaw from '../../CompararView.svelte';
import MercadoViewRaw from '../../MercadoView.svelte';
import * as pncpPublicacao from '../../../lib/pncpPublicacao';
import type { PNCPContract } from '../../../lib/pncp';
import * as parquetFallback from '../../../lib/parquetFallback';
import type { ArchivedContrato } from '../../../lib/archive/schema';
import { __setCatmatEntriesForTest } from '../../../lib/catmat';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const AtasView = AtasViewRaw as unknown as Parameters<typeof render>[0];
const CatmatSearch = CatmatSearchRaw as unknown as Parameters<typeof render>[0];
const DispensasView = DispensasViewRaw as unknown as Parameters<typeof render>[0];
const CompararView = CompararViewRaw as unknown as Parameters<typeof render>[0];
const MercadoView = MercadoViewRaw as unknown as Parameters<typeof render>[0];

function futurePlus(years: number): string {
  const d = new Date();
  d.setFullYear(d.getFullYear() + years);
  return d.toISOString().slice(0, 10);
}

const ATAS_ROW = {
  numero_controle_pncp: '00000000000191-1-000001/2024',
  data_publicacao_pncp: '2025-01-15',
  data_vigencia_inicio: '2025-01-15',
  data_vigencia_fim: futurePlus(1),
  objeto_contrato: 'Registro de preços para papel A4 sulfite',
  valor_global: 120000,
  valor_inicial: 120000,
  cnpj_orgao: '00000000000191',
  razao_social_orgao: 'Prefeitura Exemplo',
  nome_unidade: 'Secretaria de Compras',
} as unknown as ArchivedContrato;

const feature = await loadFeature('features/journeys/02_public_buyer.feature');

const PAYLOAD = {
  numeroControlePNCP: '00000000000191-1-000001/2024',
  dataPublicacaoPncp: '2025-01-15T00:00:00',
  objetoCompra: 'Aquisição de merenda escolar',
  valorTotalEstimado: 1500,
  modalidadeNome: 'Pregão Eletrônico',
  orgaoEntidade: {
    razaoSocial: 'Prefeitura Municipal',
    cnpj: '00000000000191',
  },
  unidadeOrgao: {
    nomeUnidade: 'Secretaria de Compras',
    codigoIbge: '3550308',
  },
  linkSistemaOrigem: 'https://origem.exemplo.gov.br/compras/1',
  itens: [],
};

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
  });

  Scenario('Browse vigent registered-price frameworks for an object', ({ Given, Then }) => {
    Given('the user opens "/atas?objeto=papel%20A4"', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/?objeto=papel%20A4');
      vi.spyOn(parquetFallback, 'queryArchivedTableWhere').mockResolvedValue({
        ok: true,
        rows: [ATAS_ROW],
        dataParticao: '2025-01-31',
      });
      render(AtasView);
      await tick();
    });
    Then(
      'the user sees a list of vigent atas with start date, end date and contracting agency',
      async () => {
        await waitFor(
          () => expect(screen.getByTestId('atas-list')).toBeTruthy(),
          { timeout: 2000 },
        );
      },
    );
  });

  Scenario(
    'Generate a price reference and export it as a citable PDF',
    ({ Given, When, Then, And }) => {
      Given('the user opens a market page for "papel A4"', async () => {
        cleanup();
        vi.restoreAllMocks();
        window.history.replaceState({}, '', '/mercado?objeto=papel%20A4');
        vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([
          {
            numeroControlePNCP: '00000000000191-1-000010/2024',
            dataPublicacaoPncp: '2025-01-10T00:00:00',
            objetoContratacao: 'Aquisição de papel A4',
            valorTotalEstimado: 1200,
            modalidadeNome: 'Pregão Eletrônico',
            orgaoEntidade: { razaoSocial: 'Prefeitura A', cnpj: '00000000000191' },
            unidadeOrgao: { nomeUnidade: 'Compras', ufSigla: 'SP' },
          },
          {
            numeroControlePNCP: '00000000000191-1-000011/2024',
            dataPublicacaoPncp: '2025-01-15T00:00:00',
            objetoContratacao: 'Papel A4 sulfite',
            valorTotalEstimado: 1800,
            modalidadeNome: 'Pregão Eletrônico',
            orgaoEntidade: { razaoSocial: 'Prefeitura B', cnpj: '00000000000192' },
            unidadeOrgao: { nomeUnidade: 'Administrativo', ufSigla: 'SP' },
          },
          {
            numeroControlePNCP: '00000000000191-1-000012/2024',
            dataPublicacaoPncp: '2025-01-20T00:00:00',
            objetoContratacao: 'Papel A4 para impressão',
            valorTotalEstimado: 1500,
            modalidadeNome: 'Pregão Eletrônico',
            orgaoEntidade: { razaoSocial: 'Prefeitura C', cnpj: '00000000000193' },
            unidadeOrgao: { nomeUnidade: 'Secretaria', ufSigla: 'RJ' },
          },
        ] as unknown as PNCPContract[]);
        render(MercadoView);
        await tick();
        await waitFor(
          () => expect(screen.getByTestId('mercado-gerar-pesquisa')).toBeTruthy(),
          { timeout: 3000 },
        );
      });
      When('the user clicks "Gerar pesquisa de preços"', async () => {
        await fireEvent.click(screen.getByTestId('mercado-gerar-pesquisa'));
        await tick();
      });
      Then('a PDF is produced containing min, average, median, max and standard deviation of unit price', async () => {
        await waitFor(
          () => {
            const section = screen.getByTestId('mercado-price-ref');
            expect(section).toBeTruthy();
            // min=1200, max=1800, avg=1500, median=1500, stddev=~244.95
            expect(screen.getByTestId('price-ref-min').textContent).toMatch(/1\.200/);
            expect(screen.getByTestId('price-ref-avg').textContent).toMatch(/1\.500/);
            expect(screen.getByTestId('price-ref-median').textContent).toMatch(/1\.500/);
            expect(screen.getByTestId('price-ref-max').textContent).toMatch(/1\.800/);
            expect(screen.getByTestId('price-ref-stddev')).toBeTruthy();
          },
          { timeout: 3000 },
        );
      });
      And('the PDF includes the source contract IDs and snapshot date', async () => {
        await waitFor(
          () => {
            const ids = screen.getByTestId('price-ref-ids');
            expect(ids.textContent).toContain('00000000000191-1-000010/2024');
            const dateEl = screen.getByTestId('price-ref-date');
            expect(dateEl.textContent).toMatch(/^\d{4}-\d{2}-\d{2}$/);
          },
          { timeout: 3000 },
        );
      });
    },
  );

  Scenario(
    'Compare procurement practice across municipalities of similar size',
    ({ Given, Then, And }) => {
      Given('the user opens "/comparar?ibge=3550308&objeto=merenda"', async () => {
        cleanup();
        vi.restoreAllMocks();
        window.history.replaceState({}, '', '/comparar?ibge=3550308&objeto=merenda');
        vi.spyOn(pncpPublicacao, 'fetchPublicacaoList').mockResolvedValue([
          {
            numeroControlePNCP: '00000000000191-1-000001/2024',
            dataPublicacaoPncp: '2025-01-10T00:00:00',
            objetoContratacao: 'Aquisição de merenda escolar',
            valorTotalEstimado: 50000,
            modalidadeNome: 'Pregão Eletrônico',
            orgaoEntidade: { razaoSocial: 'Prefeitura A', cnpj: '00000000000191' },
            unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'SP' },
          },
        ] as unknown as PNCPContract[]);
        render(CompararView);
        await tick();
      });
      Then('the user sees three peer municipalities of similar population', async () => {
        await waitFor(() => {
          expect(screen.getByTestId('comparar-peers')).toBeTruthy();
          const items = screen.getAllByTestId('comparar-peer-item');
          expect(items.length).toBe(3);
        }, { timeout: 3000 });
      });
      And('the user sees the per-capita spend for the same object', async () => {
        await waitFor(() => {
          const spendText = screen.getByText(/habitante/i);
          expect(spendText).toBeTruthy();
        }, { timeout: 3000 });
      });
    },
  );

  Scenario('Resolve a CATMAT or CATSER code from a free-text description', ({ Given, Then }) => {
    Given('the user types "papel sulfite branco A4 75g" into a catalog input', async () => {
      cleanup();
      // Inject a representative slice so the search runs without hitting the
      // /data/catmat.json endpoint (CatmatSearch fetches it on first use).
      __setCatmatEntriesForTest([
        { code: '19746', description: 'PAPEL PARA IMPRESSÃO FORMATADO', type: 'CATMAT' },
        { code: '10383', description: 'PAPEL HIGIÊNICO', type: 'CATMAT' },
        { code: '26824', description: 'OUTSOURCING DE IMPRESSAO PAGINAS A4 MONOCROMATICA COM PAPEL', type: 'CATSER' },
        { code: '37', description: 'AGENDA', type: 'CATMAT' },
      ]);
      render(CatmatSearch);
      await tick();
      const input = screen.getByLabelText('Descrição do item para busca CATMAT');
      await fireEvent.input(input, { target: { value: 'papel sulfite branco A4 75g' } });
      await tick();
    });

    Then('the user sees the most likely CATMAT codes ranked by match confidence', async () => {
      await waitFor(
        () => expect(screen.getByTestId('catmat-results')).toBeTruthy(),
        { timeout: 2000 },
      );
      const items = screen.getAllByTestId('catmat-result-item');
      expect(items.length).toBeGreaterThan(0);
      // PDM-level catalog: query "papel ... A4" maps to entries describing
      // paper-related catalog categories. Top result must mention paper or A4.
      expect(items[0].textContent?.toLowerCase()).toMatch(/papel|a4/);
    });
  });

  Scenario('Inspect the legal basis cited by peers in similar exemptions', ({ Given, Then }) => {
    Given('the user opens "/dispensas?objeto=papel%20A4"', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/dispensas?objeto=papel A4');
      const dispensas: PNCPContract[] = [
        {
          numeroControlePNCP: '00000000000191-8-000001/2024',
          dataPublicacaoPncp: '2025-01-10T00:00:00',
          objetoContratacao: 'Aquisição de papel A4 sulfite',
          fundamentacaoLegal: 'Art. 75, inciso II, da Lei nº 14.133/2021',
          modalidadeNome: 'Dispensa de Licitação',
          orgaoEntidade: { razaoSocial: 'Prefeitura A', cnpj: '00000000000191' },
          unidadeOrgao: { nomeUnidade: 'Compras', codigoMunicipioIbge: '3550308' },
        } as unknown as PNCPContract,
        {
          numeroControlePNCP: '00000000000192-8-000002/2024',
          dataPublicacaoPncp: '2025-01-15T00:00:00',
          objetoContratacao: 'Compra de papel A4 75g',
          fundamentacaoLegal: 'Art. 75, inciso II, da Lei nº 14.133/2021',
          modalidadeNome: 'Dispensa de Licitação',
          orgaoEntidade: { razaoSocial: 'Prefeitura B', cnpj: '00000000000192' },
          unidadeOrgao: { nomeUnidade: 'Administrativo', codigoMunicipioIbge: '3550309' },
        } as unknown as PNCPContract,
        {
          numeroControlePNCP: '00000000000193-8-000003/2024',
          dataPublicacaoPncp: '2025-01-20T00:00:00',
          objetoContratacao: 'Papel A4 branco para impressão',
          fundamentacaoLegal: 'Art. 75, inciso VIII, da Lei nº 14.133/2021',
          modalidadeNome: 'Dispensa de Licitação',
          orgaoEntidade: { razaoSocial: 'Prefeitura C', cnpj: '00000000000193' },
          unidadeOrgao: { nomeUnidade: 'Secretaria', codigoMunicipioIbge: '3550310' },
        } as unknown as PNCPContract,
      ];
      vi.spyOn(pncpPublicacao, 'fetchDispensaPagesForObjeto').mockResolvedValue(dispensas);
      render(DispensasView);
      await tick();
    });
    Then('the user sees the most cited legal articles in similar dispensa contracts', async () => {
      await waitFor(
        () => expect(screen.getByTestId('dispensas-legal-list')).toBeTruthy(),
        { timeout: 3000 },
      );
      const items = screen.getAllByTestId('dispensas-legal-item');
      expect(items.length).toBeGreaterThan(0);
      // Art. 75 II appears twice → must rank first.
      expect(items[0].textContent).toMatch(/inciso II/);
      expect(items[0].textContent).toMatch(/14\.133/);
    });
  });

  Scenario('Look up a short CATMAT code by typing its 2-digit number', ({ Given, Then }) => {
    Given('the user types "37" into a catalog input', async () => {
      cleanup();
      __setCatmatEntriesForTest([
        { code: '37', description: 'AGENDA', type: 'CATMAT' },
        { code: '3700', description: 'PRODUTOS FARMACÊUTICOS', type: 'CATMAT' },
      ]);
      render(CatmatSearch);
      await tick();
      const input = screen.getByLabelText('Descrição do item para busca CATMAT');
      await fireEvent.input(input, { target: { value: '37' } });
      await tick();
    });

    Then('the user sees the CATMAT entry for code "37"', async () => {
      await waitFor(
        () => expect(screen.getByTestId('catmat-results')).toBeTruthy(),
        { timeout: 2000 },
      );
      const items = screen.getAllByTestId('catmat-result-item');
      expect(items.length).toBeGreaterThan(0);
      // The exact code "37" must be present — not merely a prefix like "3700".
      const texts = items.map((el) => el.textContent ?? '');
      expect(texts.some((t) => /\b37\b/.test(t))).toBe(true);
    });
  });

  Scenario(
    "Crossover with journey 3 — buyer audits a peer's contract before riding on it",
    ({ Given, Then, And }) => {
      Given('the user opens "/contratacao?id=00000000000191-1-000001/2024"', async () => {
        window.history.replaceState({}, '', '/?id=00000000000191-1-000001/2024');
        global.fetch = vi
          .fn()
          .mockImplementation(async () => new Response(JSON.stringify(PAYLOAD), { status: 200 }));
        render(ContractDetailView);
        await tick();
      });
      Then("the user sees the contract's value, modality and supplier", async () => {
        // ContractDetailView renders value (formatted BRL), modality and
        // orgaoEntidade.razaoSocial — the three pieces a buyer needs to
        // defend the decision against an audit.
        await waitFor(
          () => {
            const dds = Array.from(document.querySelectorAll('dd')).map(
              (el) => el.textContent?.replace(/\s+/g, ' ').trim() ?? '',
            );
            expect(dds.some((t) => /R\$\s*1\.500,00/.test(t))).toBe(true);
            expect(dds.some((t) => /Pregão Eletrônico/.test(t))).toBe(true);
            expect(dds.some((t) => /Prefeitura Municipal/.test(t))).toBe(true);
          },
          { timeout: 2000 },
        );
      });
      And('the user sees an outbound link to the original PNCP record', () => {
        const link = document.querySelector(
          'a[href="https://origem.exemplo.gov.br/compras/1"]',
        ) as HTMLAnchorElement | null;
        expect(link).toBeTruthy();
        expect(link?.getAttribute('target')).toBe('_blank');
      });
    },
  );
});
