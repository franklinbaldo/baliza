const fs = require('fs');
let file = fs.readFileSync('web/src/components/__steps__/journeys/01_supplier_prospecting.steps.ts', 'utf-8');

file = file.replace(
  `Scenario('Search for an object opens an aggregated market page', ({ Given, When, Then, And }) => {
    Given('the user opens the home page', noop);
    When('the user submits the free-text query "merenda escolar"', noop);
    Then('the user lands on a market page summarizing top buyers, top suppliers and price ranges', () =>
      plannedStep('market aggregation page (/mercado/{objeto})'),
    );
    And('the page shows the number of distinct contracts found', noop);
  });`,
  `Scenario('Search for an object opens an aggregated market page', ({ Given, When, Then, And }) => {
    Given('the user opens the home page', async () => {
      cleanup();
      vi.restoreAllMocks();
      window.history.replaceState({}, '', '/mercado?objeto=merenda%20escolar');
      vi.spyOn(pncpPublicacao, 'fetchPublicacaoPagesForObjeto').mockResolvedValue([
        {
          numeroControlePNCP: '00000000000191-1-000001/2024',
          dataPublicacaoPncp: '2025-01-10T00:00:00',
          objetoContratacao: 'Merenda escolar — banana',
          valorTotalEstimado: 1500,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura A', cnpj: '00000000000191' },
          unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'SP' },
          niFornecedor: '12345678000195',
          nomeRazaoSocialFornecedor: 'Fornecedora Exemplo Ltda',
        },
        {
          numeroControlePNCP: '00000000000191-1-000002/2024',
          dataPublicacaoPncp: '2025-01-12T00:00:00',
          objetoContratacao: 'Merenda escolar — pão',
          valorTotalEstimado: 2000,
          modalidadeNome: 'Pregão Eletrônico',
          orgaoEntidade: { razaoSocial: 'Prefeitura B', cnpj: '00000000000192' },
          unidadeOrgao: { nomeUnidade: 'Educação', ufSigla: 'SP' },
          niFornecedor: '12345678000196',
          nomeRazaoSocialFornecedor: 'Pão Gostoso Ltda',
        },
      ] as unknown as PNCPContract[]);
      render(MercadoView);
      await tick();
    });
    When('the user submits the free-text query "merenda escolar"', noop);
    Then('the user lands on a market page summarizing top buyers, top suppliers and price ranges', async () => {
      await waitFor(() => {
        expect(screen.getByTestId('mercado-top-buyers')).toBeTruthy();
        expect(screen.getByTestId('mercado-top-suppliers')).toBeTruthy();
        expect(screen.getByTestId('mercado-price-range')).toBeTruthy();
      }, { timeout: 3000 });
    });
    And('the page shows the number of distinct contracts found', async () => {
      await waitFor(() => {
        const count = screen.getByTestId('mercado-count');
        expect(count.textContent).toContain('2');
      }, { timeout: 3000 });
    });
  });`
);
fs.writeFileSync('web/src/components/__steps__/journeys/01_supplier_prospecting.steps.ts', file);
