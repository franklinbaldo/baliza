with open("web/src/components/ContractDetailView.svelte") as f:
    content = f.read()

# I need to close the `{#if data}` block that I started when I injected:
# `{#if data}\n    <div style="display:flex; ...`
# The `{#if data}` block should wrap both the summary and `<div class="grid-details">...</div>`.
# I will add `  {/if}\n` before `</EntityDetailLayout>`

content = content.replace(
    "    </div>\n</EntityDetailLayout>", "    </div>\n  {/if}\n</EntityDetailLayout>"
)

with open("web/src/components/ContractDetailView.svelte", "w") as f:
    f.write(content)
