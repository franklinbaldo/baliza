import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import ibis

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

    def test_upsert_flow(self):
        """Verify that upsert_rows handles duplicates correctly (idempotency)."""
        with BalizaEngine() as engine:
            mock_data = {
                "numeroControlePNCP": "123456",
                "identificador": "TEST-001",
                "valor": 1000.50,
            }
            
            # First ingestion
            engine.upsert_rows([mock_data], "test_table")
            
            # Second ingestion with same PK but updated data
            updated_data = mock_data.copy()
            updated_data["valor"] = 1500.00
            engine.upsert_rows([updated_data], "test_table")
            
            # Verify: should still have 1 row, but with updated value
            t = engine.get_table("test_table")
            res = t.execute()
            self.assertEqual(len(res), 1)
            self.assertEqual(float(res.iloc[0]["valor"]), 1500.00)

    def test_ingestion_memory_direct(self):
        """Verify direct memory ingestion from list of dicts."""
        with BalizaEngine() as engine:
            rows = [{"numeroControlePNCP": f"ID-{i}", "val": i} for i in range(10)]
            engine.upsert_rows(rows, "batch_table")
            
            t = engine.get_table("batch_table")
            self.assertEqual(t.count().execute(), 10)

    def test_list_tables_schema_aware(self):
        with BalizaEngine() as engine:
            # Create a table in main schema using memtable
            engine.con.create_table("dummy", obj=ibis.memtable([{"a": 1}]), database="main")
            tables = engine.con.list_tables(database="main")
            self.assertIn("dummy", tables)
