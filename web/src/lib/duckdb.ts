import type { AsyncDuckDB, AsyncDuckDBConnection } from '@duckdb/duckdb-wasm';

export let dbInstance: AsyncDuckDB | null = null;
export let connInstance: AsyncDuckDBConnection | null = null;
let initializationPromise: Promise<{ db: AsyncDuckDB; conn: AsyncDuckDBConnection }> | null = null;

export async function getDuckDB(): Promise<{ db: AsyncDuckDB; conn: AsyncDuckDBConnection }> {
  if (dbInstance && connInstance) return { db: dbInstance, conn: connInstance };
  if (initializationPromise) return initializationPromise;

  initializationPromise = (async () => {
    try {
      const duckdb = await import('@duckdb/duckdb-wasm');
      const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
      const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);

      if (!bundle.mainWorker) throw new Error('DuckDB bundle selection failed');

      let worker: Worker;
      try {
        worker = new Worker(bundle.mainWorker);
      } catch {
        const resp = await fetch(bundle.mainWorker);
        const blob = new Blob([await resp.text()], { type: 'application/javascript' });
        worker = new Worker(URL.createObjectURL(blob));
      }

      const logger = new duckdb.ConsoleLogger();
      dbInstance = new duckdb.AsyncDuckDB(logger, worker);
      await dbInstance.instantiate(bundle.mainModule, bundle.pthreadWorker);
      connInstance = await dbInstance.connect();

      try {
        await connInstance.query('INSTALL httpfs; LOAD httpfs;');
        await connInstance.query('SET enable_http_metadata_cache=true;');
      } catch {
        // httpfs extension unavailable — HTTP queries won't work but local SQL still functions
      }

      return { db: dbInstance, conn: connInstance };
    } catch (err) {
      initializationPromise = null;
      throw err;
    }
  })();

  return initializationPromise;
}
