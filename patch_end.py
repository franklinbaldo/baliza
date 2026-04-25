import re

with open('web/src/components/ContractDetailView.svelte', 'r') as f:
    content = f.read()

# Fix the stray '    </div>\n</EntityDetailLayout>' error
content = content.replace("  {/if}\n</div>\n</EntityDetailLayout>", "</EntityDetailLayout>")
content = content.replace("  {/if}\n</div>", "")

with open('web/src/components/ContractDetailView.svelte', 'w') as f:
    f.write(content)
