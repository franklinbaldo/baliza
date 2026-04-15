import unittest
from datetime import datetime
from pathlib import Path
from baliza.engine import BalizaEngine
from baliza.ia_uploader import IAUploader
import ibis

class TestBalizaV2(unittest.TestCase):
    def setUp(self):
        # Use in-memory DuckDB for fast tests
        self.engine = BalizaEngine(":memory:")

    def test_engine_init(self):
        """Verify that schemas and quarantine tables are created on init."""
        # Check schemas exist
        schemas = self.engine.con.list_databases()
        self.assertIn("baliza_state", schemas)
        
        # Check quarantine table exists (schema-aware)
        tables = self.engine.con.list_tables(database="baliza_state")
        self.assertIn("quarantine", tables)

    def test_quarantine_record(self):
        """Verify that record quarantine works without private Ibis access errors."""
        now = datetime.now()
        error_msg = "Validation Test Error"
        raw_data = {"id": 123, "context": "test"}
        
        self.engine.quarantine_record("test_resource", now, error_msg, raw_data)
        
        # Query it back
        q_table = self.engine.get_table("quarantine", schema="baliza_state")
        res = q_table.execute()
        self.assertEqual(len(res), 1)
        self.assertEqual(res.iloc[0]["error"], error_msg)

    def test_engine_connection_leak(self):
        """Verify that connection disconnects cleanly."""
        engine = BalizaEngine(":memory:")
        # Should not raise
        engine.__exit__(None, None, None)

if __name__ == "__main__":
    unittest.main()
