const fs = require('fs');

let file = fs.readFileSync('web/src/components/MercadoView.svelte', 'utf-8');

file = file.replace(
  `  const topSuppliers = $derived.by(() => {
    const counts: Record<string, { name: string; count: number }> = {};
    for (const c of contracts) {
      if (!c.nomeRazaoSocialFornecedor && !c.niFornecedor) continue;
      const id = (c.niFornecedor || c.nomeRazaoSocialFornecedor || 'Unknown') as string;
      const name = c.nomeRazaoSocialFornecedor || c.niFornecedor || 'Unknown';
      if (!counts[id]) counts[id] = { name, count: 0 };
      counts[id].count++;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count).slice(0, 5);
  });`,
  `  const topSuppliers = $derived.by(() => {
    const counts: Record<string, { name: string; count: number }> = {};
    for (const c of contracts) {
      if (!c.nomeRazaoSocialFornecedor && !c.niFornecedor) continue;
      const id = (c.niFornecedor || c.nomeRazaoSocialFornecedor || 'Unknown') as string;
      const name = (c.nomeRazaoSocialFornecedor || c.niFornecedor || 'Unknown') as string;
      if (!counts[id]) counts[id] = { name, count: 0 };
      counts[id].count++;
    }
    return Object.values(counts).sort((a, b) => b.count - a.count).slice(0, 5);
  });`
);

fs.writeFileSync('web/src/components/MercadoView.svelte', file);
