<div>
  <section data-testid="dev-example-python">
    <h2>Python (pandas + pyarrow)</h2>
    <pre><code>{`import pandas as pd

manifest = pd.read_csv("https://archive.org/cors/baliza-pncp-manifest/manifest.csv")
latest = (
    manifest[manifest.table_name == "contratos"]
    .sort_values("data_particao")
    .iloc[-1]
)
contratos = pd.read_parquet(latest.parquet_url)
print(contratos.shape, latest.data_particao)`}</code></pre>
  </section>

  <section data-testid="dev-example-r">
    <h2>R (arrow)</h2>
    <pre><code>{`library(arrow)
library(readr)

manifest <- read_csv("https://archive.org/cors/baliza-pncp-manifest/manifest.csv")
contratos_only <- manifest[manifest$table_name == "contratos", ]
contratos_only <- contratos_only[order(contratos_only$data_particao), ]
latest <- tail(contratos_only, 1)
contratos <- read_parquet(latest$parquet_url)
nrow(contratos)`}</code></pre>
  </section>

  <section data-testid="dev-example-js">
    <h2>JavaScript (DuckDB WASM)</h2>
    <pre><code>{`import * as duckdb from '@duckdb/duckdb-wasm';

const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
const worker = new Worker(bundle.mainWorker);
const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
await db.instantiate(bundle.mainModule);
const conn = await db.connect();

const url = 'https://archive.org/download/baliza-pncp-YYYYMM/contratos-YYYY-MM-DD.parquet';
const res = await conn.query(\`SELECT COUNT(*) FROM read_parquet('\${url}')\`);
console.log(res.toArray());`}</code></pre>
  </section>
</div>

