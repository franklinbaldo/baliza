// Pure helpers that turn a visible BuscaView result list into CSV or
// GitHub-flavored Markdown. Both formats expose the same five columns the
// user already sees in the table — keeping export and on-screen parity is
// the contract callers depend on. Side effects (download trigger,
// clipboard write) live in separate helpers so they can be spied on in
// tests without dragging the format logic with them.

import type { PNCPContract } from './pncp';

export interface ExportRow {
  agency: string;
  modality: string;
  objeto: string;
  date: string;
  valor: string;
}

export const EXPORT_HEADERS = [
  'Órgão',
  'Modalidade',
  'Objeto',
  'Data',
  'Valor estimado (BRL)',
] as const;

function formatBrlPlain(value: number | null | undefined): string {
  if (typeof value !== 'number') return '';
  return value.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function formatIsoDate(s: string | null | undefined): string {
  if (!s) return '';
  // PNCP timestamps come as 2025-01-15T00:00:00 — slice to just the date so
  // the export reads the same as the on-screen formatDate output.
  return s.slice(0, 10);
}

export function toExportRows(results: PNCPContract[]): ExportRow[] {
  return results.map((c) => ({
    agency: c.orgaoEntidade?.razaoSocial ?? '',
    modality: c.modalidadeNome ?? '',
    objeto: c.objetoContratacao ?? '',
    date: formatIsoDate(c.dataPublicacaoPncp),
    valor: formatBrlPlain(c.valorTotalEstimado ?? null),
  }));
}

// RFC 4180-style CSV: wrap a field in double-quotes when it contains a
// quote, comma, or newline; escape embedded quotes by doubling them.
function csvEscape(field: string): string {
  if (/["\n,]/.test(field)) {
    return `"${field.replace(/"/g, '""')}"`;
  }
  return field;
}

export function toCsv(results: PNCPContract[]): string {
  const rows = toExportRows(results);
  const header = EXPORT_HEADERS.map(csvEscape).join(',');
  const body = rows
    .map((r) => [r.agency, r.modality, r.objeto, r.date, r.valor].map(csvEscape).join(','))
    .join('\n');
  return body ? `${header}\n${body}\n` : `${header}\n`;
}

// GFM tables: pipe-separated cells with a separator row of dashes. Pipes
// inside cells must be escaped or they break the column layout; newlines
// within cells get collapsed to spaces because GFM does not support
// hard breaks inside table cells.
function mdEscape(field: string): string {
  return field.replace(/\|/g, '\\|').replace(/\r?\n/g, ' ');
}

export function toMarkdown(results: PNCPContract[]): string {
  const rows = toExportRows(results);
  const header = `| ${EXPORT_HEADERS.map(mdEscape).join(' | ')} |`;
  const sep = `| ${EXPORT_HEADERS.map(() => '---').join(' | ')} |`;
  if (rows.length === 0) return `${header}\n${sep}\n`;
  const body = rows
    .map((r) => `| ${[r.agency, r.modality, r.objeto, r.date, r.valor].map(mdEscape).join(' | ')} |`)
    .join('\n');
  return `${header}\n${sep}\n${body}\n`;
}

// Trigger a browser download of arbitrary text content. Wraps the
// Blob + temporary anchor + revokeObjectURL dance so callers (and tests)
// have one symbol to spy on.
export function downloadTextFile(filename: string, contents: string, mime: string): void {
  if (typeof window === 'undefined') return;
  const blob = new Blob([contents], { type: `${mime};charset=utf-8` });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
