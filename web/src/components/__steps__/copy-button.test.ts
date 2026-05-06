import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, cleanup, waitFor, fireEvent } from '@testing-library/svelte/pure';
import CopyButtonRaw from '../CopyButton.svelte';
import ShareButtonRaw from '../ShareButton.svelte';

const CopyButton = CopyButtonRaw as unknown as Parameters<typeof render>[0];
const ShareButton = ShareButtonRaw as unknown as Parameters<typeof render>[0];

function stubClipboard() {
  // Make sure navigator.clipboard exists and its writeText is spy-able.
  // jsdom doesn't ship navigator.clipboard, and re-defining it on the
  // navigator instance is ignored for non-configurable prototype slots —
  // so we install/replace the property on the navigator's prototype and
  // then spy on the writeText method directly.
  const proto = Object.getPrototypeOf(navigator);
  Object.defineProperty(proto, 'clipboard', {
    configurable: true,
    value: { writeText: () => Promise.resolve() },
  });
  return vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue();
}

describe('CopyButton', () => {
  beforeEach(() => {
    cleanup();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('writes text to clipboard on click', async () => {
    const writeText = stubClipboard();
    render(CopyButton, { props: { text: 'PNCP-123' } });
    await fireEvent.click(screen.getByRole('button'));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith('PNCP-123'));
  });

  it('disables when text is blank', () => {
    render(CopyButton, { props: { text: '   ' } });
    expect(screen.getByRole('button')).toBeDisabled();
  });

  it('renders inline label when variant=inline', () => {
    render(CopyButton, { props: { text: 'x', variant: 'inline', label: 'Copiar BibTeX' } });
    expect(screen.getByRole('button')).toHaveTextContent('Copiar BibTeX');
  });
});

describe('ShareButton', () => {
  beforeEach(() => {
    cleanup();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('uses navigator.share when available', async () => {
    const shareSpy = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, 'share', { configurable: true, value: shareSpy });
    render(ShareButton, { props: { url: 'https://example.test/x', title: 'X' } });
    await fireEvent.click(screen.getByRole('button'));
    await waitFor(() =>
      expect(shareSpy).toHaveBeenCalledWith(
        expect.objectContaining({ url: 'https://example.test/x', title: 'X' }),
      ),
    );
  });

  it('falls back to clipboard when share is undefined', async () => {
    const writeText = stubClipboard();
    Object.defineProperty(navigator, 'share', { configurable: true, value: undefined });
    render(ShareButton, { props: { url: 'https://example.test/y' } });
    await fireEvent.click(screen.getByRole('button'));
    await waitFor(() => expect(writeText).toHaveBeenCalledWith('https://example.test/y'));
  });
});
