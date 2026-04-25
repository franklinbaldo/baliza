import { describe, expect, it } from 'vitest';
import { buildContractsPageUrl, buildRawArchiveStatus } from './ia-raw-status';

describe('buildRawArchiveStatus', () => {
  it('derives coverage and latest raw file from Internet Archive metadata', () => {
    const status = buildRawArchiveStatus({
      files_count: '5',
      item_size: '6000',
      item_last_updated: '1777070323',
      server: 'ia801400.us.archive.org',
      dir: '/30/items/baliza-pncp-raw',
      files: [
        { name: 'raw-2024-01.zip', size: '1000', mtime: '1777069486', source: 'original', format: 'ZIP' },
        { name: 'raw-2024-03.zip', size: '3000', mtime: '1777069510', source: 'original', format: 'ZIP' },
        { name: 'raw-2024-02.zip', size: '2000', mtime: '1777069498', source: 'original', format: 'ZIP' },
        { name: 'baliza-pncp-raw_meta.xml', size: '989', source: 'original', format: 'Metadata' },
        { name: 'baliza-pncp-raw_archive.torrent', size: '27269', source: 'metadata' },
      ],
    });

    expect(status.rawZipCount).toBe(3);
    expect(status.firstPeriod).toBe('2024-01');
    expect(status.latestPeriod).toBe('2024-03');
    expect(status.latestRawFile?.name).toBe('raw-2024-03.zip');
    expect(status.rawBytes).toBe(6000);
    expect(status.metadataFileCount).toBe(2);
    expect(status.rawArchives).toEqual([
      expect.objectContaining({
        period: '2024-01',
        fileName: 'raw-2024-01.zip',
        contractsPageUrl: 'https://ia801400.us.archive.org/view_archive.php?archive=%2F30%2Fitems%2Fbaliza-pncp-raw%2Fraw-2024-01.zip&file=contratos_p1.json',
        contractsCount: null,
      }),
      expect.objectContaining({ period: '2024-02' }),
      expect.objectContaining({ period: '2024-03' }),
    ]);
    expect(status.lastUpdatedIso).toBe('2026-04-24T22:38:43.000Z');
  });

  it('builds the IA extracted-file URL used for contract counts', () => {
    expect(
      buildContractsPageUrl(
        'raw-2023-01.zip',
        'ia601400.us.archive.org',
        '/30/items/baliza-pncp-raw',
      ),
    ).toBe(
      'https://ia601400.us.archive.org/view_archive.php?archive=%2F30%2Fitems%2Fbaliza-pncp-raw%2Fraw-2023-01.zip&file=contratos_p1.json',
    );
  });
});
