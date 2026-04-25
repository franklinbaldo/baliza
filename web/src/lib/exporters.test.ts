import { describe, it, expect } from 'vitest';
import { toCsv, toMarkdown, toExportRows, EXPORT_HEADERS } from './exporters';
import type { PNCPContract } from './pncp';

const SAMPLE = [
  {
    numeroControlePNCP: '00000000000191-1-000001/2024',
    dataPublicacaoPncp: '2025-01-15T00:00:00',
    objetoContratacao: 'Aquisição de papel A4',
    valorTotalEstimado: 1500,
    modalidadeNome: 'Pregão Eletrônico',
    orgaoEntidade: { razaoSocial: 'Prefeitura X', cnpj: '00000000000191' },
    unidadeOrgao: { nomeUnidade: 'Compras' },
  },
  {
    numeroControlePNCP: '00000000000191-1-000002/2024',
    dataPublicacaoPncp: '2025-02-10T00:00:00',
    objetoContratacao: 'Serviço, com vírgula e "aspas"',
    valorTotalEstimado: null,
    modalidadeNome: 'Dispensa',
    orgaoEntidade: { razaoSocial: 'Prefeitura | Y', cnpj: '00000000000192' },
    unidadeOrgao: { nomeUnidade: 'Saúde' },
  },
] as unknown as PNCPContract[];

describe('toExportRows', () => {
  it('shapes contracts into the five export columns', () => {
    const rows = toExportRows(SAMPLE);
    expect(rows[0]).toEqual({
      agency: 'Prefeitura X',
      modality: 'Pregão Eletrônico',
      objeto: 'Aquisição de papel A4',
      date: '2025-01-15',
      valor: '1.500,00',
    });
    expect(rows[1].valor).toBe('');
  });
});

describe('toCsv', () => {
  it('emits the header even when there are no rows', () => {
    expect(toCsv([])).toBe(`${EXPORT_HEADERS.join(',')}\n`);
  });

  it('quotes fields containing comma, quote or newline and doubles inner quotes', () => {
    const csv = toCsv(SAMPLE);
    const lines = csv.trim().split('\n');
    expect(lines[0]).toBe('Órgão,Modalidade,Objeto,Data,Valor estimado (BRL)');
    expect(lines[1]).toBe(
      'Prefeitura X,Pregão Eletrônico,Aquisição de papel A4,2025-01-15,"1.500,00"',
    );
    // The objeto on row 2 contains both a comma and embedded double quotes,
    // so it must be wrapped and have its quotes doubled.
    expect(lines[2]).toContain('"Serviço, com vírgula e ""aspas"""');
    expect(lines[2]).toContain('Prefeitura | Y');
  });
});

describe('toMarkdown', () => {
  it('emits header + separator even when there are no rows', () => {
    const md = toMarkdown([]);
    const lines = md.trim().split('\n');
    expect(lines).toHaveLength(2);
    expect(lines[0]).toBe('| Órgão | Modalidade | Objeto | Data | Valor estimado (BRL) |');
    expect(lines[1]).toBe('| --- | --- | --- | --- | --- |');
  });

  it('escapes pipe characters and emits one row per contract', () => {
    const md = toMarkdown(SAMPLE);
    const lines = md.trim().split('\n');
    expect(lines).toHaveLength(4);
    // Pipes inside the agency name must be backslash-escaped so they do
    // not break the table layout.
    expect(lines[3]).toContain('Prefeitura \\| Y');
  });
});
