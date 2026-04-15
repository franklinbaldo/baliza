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

    def test_ingestion_flow_real(self):
        """Verify that ingest_json actually loads data into a table."""
        with BalizaEngine() as engine:
            # Create a mock JSON payload
            mock_data = {
                "id": 123,
                "identificador": "TEST-001",
                "valor": 1000.50,
                "data": "2024-01-01"
            }
            
            with tempfile.TemporaryDirectory() as tmp_dir:
                json_path = Path(tmp_dir) / "test_data.json"
                with open(json_path, "w") as f:
                    json.dump([mock_data], f)
                
                # Ingest into a test table
                engine.ingest_json(str(json_path), "test_table", schema="main")
                
                # Verify
                t = engine.get_table("test_table", schema="main")
                res = t.execute()
                self.assertEqual(len(res), 1)
                self.assertEqual(res.iloc[0]["identificador"], "TEST-001")
                self.assertEqual(float(res.iloc[0]["valor"]), 1000.50)

    def test_list_tables_schema_aware(self):
        with BalizaEngine() as engine:
            # Create a table in main schema using memtable
            engine.con.create_table("dummy", obj=ibis.memtable([{"a": 1}]), database="main")
            tables = engine.con.list_tables(database="main")
            self.assertIn("dummy", tables)
