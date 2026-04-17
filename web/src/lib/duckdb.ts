import type {
  AsyncDuckDB,
  AsyncDuckDBConnection,
  DuckDBBundles,
} from '@duckdb/duckdb-wasm';

import duckdb_mvp_wasm from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import duckdb_mvp_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import duckdb_eh_wasm from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import duckdb_eh_worker from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

export let dbInstance: AsyncDuckDB | null = null;
export let connInstance: AsyncDuckDBConnection | null = null;
let initializationPromise: Promise<{ db: AsyncDuckDB; conn: AsyncDuckDBConnection }> | null = null;

function getBundledBundles(): DuckDBBundles {
  return {
    mvp: { mainModule: duckdb_mvp_wasm, mainWorker: duckdb_mvp_worker },
    eh: { mainModule: duckdb_eh_wasm, mainWorker: duckdb_eh_worker },
  };
}

export async function getDuckDB(): Promise<{ db: AsyncDuckDB; conn: AsyncDuckDBConnection }> {
  if (dbInstance && connInstance) return { db: dbInstance, conn: connInstance };
  if (initializationPromise) return initializationPromise;

  initializationPromise = (async () => {
    try {
      const duckdb = await import('@duckdb/duckdb-wasm');

      let bundle: Awaited<ReturnType<typeof duckdb.selectBundle>>;
      try {
        bundle = await duckdb.selectBundle(getBundledBundles());
      } catch {
        bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
      }

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
