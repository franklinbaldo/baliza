// Pure helpers that turn a visible BuscaView result list into CSV or
// GitHub-flavored Markdown. Both formats expose the same five columns the
// user already sees in the table — keeping export and on-screen parity is
// the contract callers depend on. Side effects (download trigger,
// clipboard write) live in separate helpers so they can be spied on in
// tests without dragging the format logic with them.

import type { PNCPContract } from './pncp';
import { formatDate } from './format';

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

// The "matching the visible table" requirement on Journey 01's CSV
// scenario means export rows must read the same as the BuscaView result
// cells, so we delegate to the same formatDate the UI uses. formatDate
// returns the em-dash for missing/invalid input — turn that into an
// empty string so downstream tools see a blank cell instead of a glyph.
const EM_DASH = '—';
function formatExportDate(s: string | null | undefined): string {
  const out = formatDate(s);
  return out === EM_DASH ? '' : out;
}

export function toExportRows(results: PNCPContract[]): ExportRow[] {
  return results.map((c) => ({
    agency: c.orgaoEntidade?.razaoSocial ?? '',
    modality: c.modalidadeNome ?? '',
    objeto: c.objetoContratacao ?? '',
    date: formatExportDate(c.dataPublicacaoPncp),
    valor: formatBrlPlain(c.valorTotalEstimado ?? null),
  }));
}

// CSV-injection guard: Excel, Numbers and Google Sheets evaluate any cell
// whose first character is =, +, -, @, tab, CR, or LF as a formula on
// open. Some importers strip leading whitespace before evaluation, which
// is why \n needs the same treatment as \r and \t. Since contract fields
// come from untrusted PNCP data, prefix a leading apostrophe — the
// spreadsheet keeps the literal text and skips the formula path. OWASP
// "Formula Injection" recommends this exact escape.
function neutralizeFormula(field: string): string {
  if (field.length === 0) return field;
  const first = field.charCodeAt(0);
  // = + - @ tab LF CR
  if (
    first === 0x3d ||
    first === 0x2b ||
    first === 0x2d ||
    first === 0x40 ||
    first === 0x09 ||
    first === 0x0a ||
    first === 0x0d
  ) {
    return `'${field}`;
  }
  return field;
}

// RFC 4180-style CSV: wrap a field in double-quotes when it contains a
// quote, comma, or any line-ending byte (\n or bare \r — readers that
// treat CR as a record terminator would otherwise split the row);
// escape embedded quotes by doubling them. Formula-injection
// neutralization runs first so the leading apostrophe itself never
// becomes part of an evaluated formula.
function csvEscape(field: string): string {
  const safe = neutralizeFormula(field);
  if (/["\n\r,]/.test(safe)) {
    return `"${safe.replace(/"/g, '""')}"`;
  }
  return safe;
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
// inside cells must be escaped or they break the column layout; any
// line-ending bytes (\n, \r, or the pair) get collapsed to a single
// space because GFM does not support hard breaks inside table cells.
function mdEscape(field: string): string {
  return field.replace(/\|/g, '\\|').replace(/[\r\n]+/g, ' ');
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
