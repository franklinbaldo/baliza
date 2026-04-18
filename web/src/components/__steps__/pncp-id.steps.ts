import { loadFeature, describeFeature } from '@amiceli/vitest-cucumber';
import { screen, cleanup, waitFor } from '@testing-library/svelte/pure';
import { vi, expect } from 'vitest';
import { tick } from 'svelte';
import { render } from './shared';
import { parsePncpId, type PncpId } from '../../lib/pncpId';
import ContractDetailViewRaw from '../ContractDetailView.svelte';

const ContractDetailView = ContractDetailViewRaw as unknown as Parameters<typeof render>[0];
const feature = await loadFeature('features/pncp-id.feature');

function setUrlQuery(qs: string) {
  window.history.replaceState({}, '', qs ? `/?${qs}` : '/');
}

describeFeature(feature, ({ Scenario, BeforeEachScenario }) => {
  let raw = '';
  let parsed: PncpId | null = null;

  BeforeEachScenario(async () => {
    cleanup();
    vi.restoreAllMocks();
    setUrlQuery('');
    raw = '';
    parsed = null;
  });

  Scenario('Canonical ID parses into its four segments', ({ Given, When, Then, And }) => {
    Given('the PNCP ID "12345678000195-1-000001/2024"', () => {
      raw = '12345678000195-1-000001/2024';
    });
    When('it is parsed', () => {
      parsed = parsePncpId(raw);
    });
    Then('the result should have cnpj "12345678000195"', () => {
      expect(parsed?.cnpj).toBe('12345678000195');
    });
    And('the result should have tipo "1"', () => {
      expect(parsed?.tipo).toBe('1');
    });
    And('the result should have sequencial "000001"', () => {
      expect(parsed?.sequencial).toBe('000001');
    });
    And('the result should have ano "2024"', () => {
      expect(parsed?.ano).toBe('2024');
    });
  });

  Scenario('Hyphen-only ID is rejected', ({ Given, When, Then }) => {
    Given('the PNCP ID "12345678000195-1-000001-1"', () => {
      raw = '12345678000195-1-000001-1';
    });
    When('it is parsed', () => {
      parsed = parsePncpId(raw);
    });
    Then('the result should be null', () => {
      expect(parsed).toBeNull();
    });
  });

  Scenario('Missing year segment is rejected', ({ Given, When, Then }) => {
    Given('the PNCP ID "12345678000195-1-000001"', () => {
      raw = '12345678000195-1-000001';
    });
    When('it is parsed', () => {
      parsed = parsePncpId(raw);
    });
    Then('the result should be null', () => {
      expect(parsed).toBeNull();
    });
  });

  Scenario('Three-digit year is rejected', ({ Given, When, Then }) => {
    Given('the PNCP ID "12345678000195-1-000001/202"', () => {
      raw = '12345678000195-1-000001/202';
    });
    When('it is parsed', () => {
      parsed = parsePncpId(raw);
    });
    Then('the result should be null', () => {
      expect(parsed).toBeNull();
    });
  });

  Scenario('Lowercase letters are rejected', ({ Given, When, Then }) => {
    Given('the PNCP ID "abcdefghijklmn-1-000001/2024"', () => {
      raw = 'abcdefghijklmn-1-000001/2024';
    });
    When('it is parsed', () => {
      parsed = parsePncpId(raw);
    });
    Then('the result should be null', () => {
      expect(parsed).toBeNull();
    });
  });

  Scenario('ContractDetailView renders a targeted EntityNotFound for non-canonical IDs', ({ Given, When, Then, And }) => {
    Given('the URL has id "12345678000195-1-000001-1"', () => {
      setUrlQuery('id=12345678000195-1-000001-1');
    });
    When('the contract detail view mounts', async () => {
      render(ContractDetailView);
      await tick();
    });
    Then('I should see a non-canonical ID notice', async () => {
      await waitFor(() =>
        expect(screen.getByText(/fora do padrão PNCP/i)).toBeTruthy(),
      );
    });
    And('I should see the expected format hint', () => {
      expect(screen.getByText(/00000000000000-1-000001\/2024/)).toBeTruthy();
    });
  });
});
