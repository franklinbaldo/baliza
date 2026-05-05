const fs = require('fs');

function patch(file) {
    let content = fs.readFileSync(file, 'utf8');
    content = content.replace(/const params = new URLSearchParams/g, '// eslint-disable-next-line svelte/prefer-svelte-reactivity\n    const params = new URLSearchParams');
    fs.writeFileSync(file, content);
}

patch('web/src/components/CompararView.svelte');
patch('web/src/components/MercadoView.svelte');
