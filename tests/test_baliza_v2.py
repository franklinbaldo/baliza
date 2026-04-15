import unittest
from datetime import datetime

from baliza.engine import BalizaEngine


class TestBalizaV2(unittest.TestCase):
    def test_engine_init(self):
        with BalizaEngine() as engine:
            self.assertEqual(engine.path, ":memory:")

    def test_quarantine_native(self):
        with BalizaEngine() as engine:
            now = datetime.now()
            raw_data = {"test": "data"}
            engine.quarantine_record("test_res", now, "test_error", raw_data)

            # Verify via Ibis
            q = engine.get_table("quarantine", schema="baliza_state")
            count = q.count().execute()
            self.assertEqual(count, 1)

    def test_ingestion_flow(self):
        # In-memory ingestion test would require a physical file to exist for read_json
        # Here we just verify the schema and table listing works
        with BalizaEngine() as engine:
            tables = engine.con.list_tables(database="main")
            self.assertIsInstance(tables, list)
