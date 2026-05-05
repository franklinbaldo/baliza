const fs = require('fs');
let file = fs.readFileSync('web/src/components/__steps__/journeys/02_public_buyer.steps.ts', 'utf-8');

file = file.replace(
  `Scenario(
    'Compare procurement practice across municipalities of similar size',
    ({ Given, Then, And }) => {
      Given('the user opens "/comparar?ibge=3550308&objeto=merenda"', noop);
      Then('the user sees three peer municipalities of similar population', () =>
        plannedStep('peer-municipality comparison route'),
      );
      And('the user sees the per-capita spend for the same object', noop);
    },
  );`,
  `Scenario(
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
  );`
);
fs.writeFileSync('web/src/components/__steps__/journeys/02_public_buyer.steps.ts', file);
