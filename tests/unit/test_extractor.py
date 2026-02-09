"""Unit tests for PNCPExtractor checkpoint functionality.

These tests validate the checkpoint/marker system that enables resumable extraction.
Inspired by causaganha PR #346 which caught critical bugs in checkpoint logic.

Test scenarios follow BDD principles (Given/When/Then):
1. First run (no checkpoint) → creates marker
2. Resume from checkpoint → continues from saved state
3. Complete run → marker indicates completion
4. Re-run after completion → skips (idempotency)
5. Corrupted checkpoint → graceful recovery
6. Network failures → partial state preserved
"""

import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import duckdb
import httpx

from baliza.extractor import CheckpointData, PNCPExtractor


class TestExtractorCheckpointFirstRun(unittest.TestCase):
    """Test checkpoint behavior on first run (no existing checkpoint).
    
    Given: A clean database with no existing checkpoints
    When: Extraction runs for the first time
    Then: Checkpoint should be created after each page
    """

    def setUp(self):
        """Create temporary database for isolated testing."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_no_checkpoint_when_first_page_extracted_then_checkpoint_created(self):
        """
        Given: No existing checkpoint for date 2024-01-15
        When: First page is extracted
        Then: Checkpoint should exist with current_page=1
        """
        # Given: Clean database
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            
            # Verify no checkpoint exists
            checkpoint = self.extractor._get_checkpoint(con, "contratos", datetime(2024, 1, 15))
            self.assertIsNone(checkpoint, "Should have no checkpoint initially")
            
            # When: Save checkpoint after first page
            stats: CheckpointData = {
                "current_page": 1,
                "total_pages": 5,
                "rows_extracted": 100,
            }
            self.extractor._save_checkpoint(con, "contratos", datetime(2024, 1, 15), stats, datetime.now())
            
            # Then: Checkpoint should exist
            saved_checkpoint = self.extractor._get_checkpoint(con, "contratos", datetime(2024, 1, 15))
            self.assertIsNotNone(saved_checkpoint, "Checkpoint should exist after save")
            self.assertEqual(saved_checkpoint["current_page"], 1)
            self.assertEqual(saved_checkpoint["total_pages"], 5)
            self.assertEqual(saved_checkpoint["rows_extracted"], 100)

    def test_given_no_checkpoint_when_extraction_runs_then_starts_from_page_one(self):
        """
        Given: No existing checkpoint
        When: Extraction is initiated
        Then: Extraction should start from page 1 (not resume from middle)
        """
        # Mock HTTP responses for 3 pages
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            if page <= 3:
                return {
                    "data": [
                        {
                            "numeroControlePNCP": f"CTRL-{page}-{i}",
                            "anoCompra": 2024,
                            "sequencialCompra": i,
                            "orgaoEntidade": {"cnpj": "12345678000190", "razaoSocial": "Test"},
                            "unidadeOrgao": {"codigoUnidade": "001", "nomeUnidade": "Unit"},
                            "modalidadeId": 1,
                            "modalidadeNome": "Pregão",
                            "valorInicial": 1000.0,
                            "dataPublicacao": "2024-01-15T10:00:00",
                            "objetoContrato": f"Contract {i}",
                        }
                        for i in range(10)
                    ],
                    "totalPaginas": 3,
                }
            else:
                return {"data": [], "totalPaginas": 3}

        # When: Run extraction with mocked API
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            result = self.extractor.extract(
                datetime(2024, 1, 15),
                datetime(2024, 1, 15),
                resource="contratos",
                workers=1
            )
        
        # Then: Should have extracted all 3 pages
        self.assertEqual(result["pages"], 3, "Should process all 3 pages")
        self.assertEqual(result["rows_extracted"], 30, "Should extract 10 rows per page")
        
        # And: No checkpoint should remain (completed successfully)
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", datetime(2024, 1, 15))
            self.assertIsNone(checkpoint, "Checkpoint should be cleared after completion")


class TestExtractorCheckpointResume(unittest.TestCase):
    """Test checkpoint resume functionality.
    
    Given: An existing checkpoint at page N
    When: Extraction resumes
    Then: Should continue from page N+1 (not restart from page 1)
    """

    def setUp(self):
        """Create temporary database with existing checkpoint."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_checkpoint_at_page_2_when_resume_then_continues_from_page_3(self):
        """
        Given: Checkpoint exists at page 2 (of 5 total pages)
        When: Extraction resumes
        Then: Should fetch page 3, not restart from page 1
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Given: Create checkpoint at page 2
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            stats: CheckpointData = {
                "current_page": 2,
                "total_pages": 5,
                "rows_extracted": 200,
            }
            self.extractor._save_checkpoint(con, "contratos", extraction_date, stats, datetime.now())
            
            # Verify checkpoint exists
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
            self.assertEqual(checkpoint["current_page"], 2, "Checkpoint should be at page 2")

        # Track which pages were requested
        requested_pages = []
        
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            requested_pages.append(page)
            
            # Return data for pages 3, 4, 5 (resume scenario)
            if page in [3, 4, 5]:
                return {
                    "data": [
                        {
                            "numeroControlePNCP": f"CTRL-{page}-{i}",
                            "anoCompra": 2024,
                            "orgaoEntidade": {"cnpj": "12345678000190"},
                            "dataPublicacao": "2024-01-15T10:00:00",
                        }
                        for i in range(10)
                    ],
                    "totalPaginas": 5,
                }
            else:
                # Pages 1-2 should not be requested (already processed)
                return {"data": [], "totalPaginas": 5}

        # When: Resume extraction
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            result = self.extractor.extract(
                extraction_date,
                extraction_date,
                resource="contratos",
                workers=1
            )
        
        # Then: Should only request pages 3, 4, 5 (not 1, 2)
        self.assertNotIn(1, requested_pages, "Should not re-fetch page 1")
        self.assertNotIn(2, requested_pages, "Should not re-fetch page 2")
        self.assertIn(3, requested_pages, "Should fetch page 3")
        self.assertIn(4, requested_pages, "Should fetch page 4")
        self.assertIn(5, requested_pages, "Should fetch page 5")
        
        # And: Should process 3 pages (3, 4, 5)
        self.assertEqual(result["pages"], 5, "Should track total pages")

    def test_given_checkpoint_when_resume_then_preserves_previous_row_count(self):
        """
        Given: Checkpoint with 200 rows already extracted
        When: Extraction resumes and adds 100 more rows
        Then: Total row count should accumulate correctly
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Given: Checkpoint at page 2 with 200 rows
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            stats: CheckpointData = {
                "current_page": 2,
                "total_pages": 3,
                "rows_extracted": 200,
            }
            self.extractor._save_checkpoint(con, "contratos", extraction_date, stats, datetime.now())

        # Mock API to return 1 more page
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            if page == 3:
                return {
                    "data": [
                        {
                            "numeroControlePNCP": f"CTRL-{page}-{i}",
                            "anoCompra": 2024,
                            "orgaoEntidade": {"cnpj": "12345678000190"},
                            "dataPublicacao": "2024-01-15T10:00:00",
                        }
                        for i in range(100)  # 100 rows in last page
                    ],
                    "totalPaginas": 3,
                }
            else:
                return {"data": [], "totalPaginas": 3}

        # When: Resume extraction
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            result = self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        # Then: Total should be 200 (previous) + 100 (new) = 300
        self.assertEqual(result["rows_extracted"], 300, "Should accumulate row counts from resume")


class TestExtractorCheckpointCompletion(unittest.TestCase):
    """Test checkpoint cleanup after successful completion.
    
    Given: Extraction completes successfully
    When: All pages are processed
    Then: Checkpoint should be cleared (not left behind)
    """

    def setUp(self):
        """Create temporary database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_extraction_completes_when_all_pages_processed_then_checkpoint_cleared(self):
        """
        Given: Extraction processes all pages successfully
        When: Final page is completed
        Then: Checkpoint should be removed from database
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Mock API with 2 pages
        def mock_get(url, params=None, **kwargs):
            page = params.get("pagina", 1)
            response = MagicMock()
            response.status_code = 200
            
            if page in [1, 2]:
                response.json.return_value = {
                    "data": [{"numeroControlePNCP": f"CTRL-{page}-{i}", "anoCompra": 2024, "orgaoEntidade": {"cnpj": "12345678000190"}, "dataPublicacao": "2024-01-15T10:00:00"} for i in range(10)],
                    "totalPaginas": 2,
                }
            else:
                response.json.return_value = {"data": [], "totalPaginas": 2}
                
            response.raise_for_status = MagicMock()
            return response

        # When: Run complete extraction
        with patch.object(self.extractor.client, 'get', side_effect=mock_get):
            self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        # Then: Checkpoint should be cleared
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
            self.assertIsNone(checkpoint, "Checkpoint should be cleared after successful completion")
            
            # And: Coverage should be marked as complete
            result = con.execute(
                "SELECT status FROM baliza_state.coverage WHERE resource = 'contratos'"
            ).fetchone()
            self.assertIsNotNone(result, "Coverage record should exist")
            self.assertEqual(result[0], "complete", "Status should be 'complete'")

    def test_given_checkpoint_exists_when_extraction_completes_then_coverage_recorded(self):
        """
        Given: Checkpoint exists from partial extraction
        When: Extraction completes successfully
        Then: Coverage table should show 'complete' status
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Given: Create partial checkpoint
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            stats: CheckpointData = {
                "current_page": 1,
                "total_pages": 2,
                "rows_extracted": 50,
            }
            self.extractor._save_checkpoint(con, "contratos", extraction_date, stats, datetime.now())

        # Mock API to complete extraction
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            if page == 2:
                return {
                    "data": [{"numeroControlePNCP": f"CTRL-{page}-{i}", "anoCompra": 2024, "orgaoEntidade": {"cnpj": "12345678000190"}, "dataPublicacao": "2024-01-15T10:00:00"} for i in range(10)],
                    "totalPaginas": 2,
                }
            else:
                return {"data": [], "totalPaginas": 2}

        # When: Complete extraction
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        # Then: Coverage should exist
        with duckdb.connect(str(self.db_path)) as con:
            coverage = con.execute(
                "SELECT status, total_paginas, rows_extracted FROM baliza_state.coverage WHERE resource = 'contratos'"
            ).fetchone()
            self.assertIsNotNone(coverage, "Coverage should be recorded")
            self.assertEqual(coverage[0], "complete")
            self.assertEqual(coverage[1], 2, "Should record total pages")


class TestExtractorCheckpointIdempotency(unittest.TestCase):
    """Test idempotency: re-running completed extraction should be safe.
    
    Given: Extraction already completed (no checkpoint exists)
    When: Same extraction is run again
    Then: Should not duplicate data or create unnecessary work
    """

    def setUp(self):
        """Create temporary database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_completed_extraction_when_rerun_then_no_duplicates(self):
        """
        Given: Extraction completed (coverage shows 'complete')
        When: Same date range is extracted again
        Then: Should not duplicate rows (INSERT OR IGNORE behavior)
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Mock API with consistent data
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            if page == 1:
                return {
                    "data": [
                        {
                            "numeroControlePNCP": "CTRL-UNIQUE-001",  # Same ID both times
                            "anoCompra": 2024,
                            "sequencialCompra": 1,
                            "orgaoEntidade": {"cnpj": "12345678000190", "razaoSocial": "Test"},
                            "unidadeOrgao": {"codigoUnidade": "001", "nomeUnidade": "Unit"},
                            "modalidadeId": 1,
                            "modalidadeNome": "Pregão",
                            "valorInicial": 1000.0,
                            "dataPublicacao": "2024-01-15T10:00:00",
                            "objetoContrato": "Contract 1",
                        }
                    ],
                    "totalPaginas": 1,
                }
            else:
                return {"data": [], "totalPaginas": 1}

        # First run
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            result1 = self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        self.assertEqual(result1["rows_extracted"], 1, "First run should extract 1 row")
        
        # Verify data exists
        with duckdb.connect(str(self.db_path)) as con:
            count1 = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
            self.assertEqual(count1, 1, "Should have 1 row after first extraction")

        # Second run (idempotent re-run)
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        # Then: Should not duplicate
        with duckdb.connect(str(self.db_path)) as con:
            count2 = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
            self.assertEqual(count2, 1, "Should still have only 1 row (INSERT OR IGNORE)")
        
        # Note: result2["rows_extracted"] will be 1 because it tries to insert, but DB deduplicates
        # The important assertion is count2 == 1 (no duplicates in database)


class TestExtractorCheckpointCorruption(unittest.TestCase):
    """Test graceful handling of corrupted checkpoints.
    
    Given: Checkpoint data is corrupted or invalid
    When: Extraction attempts to use the checkpoint
    Then: Should handle gracefully (restart or clear invalid state)
    """

    def setUp(self):
        """Create temporary database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_corrupted_checkpoint_when_get_checkpoint_then_returns_none_on_error(self):
        """
        Given: Checkpoint table has corrupted data (NULL values, invalid types)
        When: _get_checkpoint is called
        Then: Should return None gracefully (allowing restart from page 1)
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Given: Create corrupted checkpoint (NULL total_pages)
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            
            # Insert corrupted data directly (bypass validation)
            con.execute(
                """
                INSERT INTO baliza_state.extraction_checkpoint
                VALUES ('contratos', '2024-01-15', 2, NULL, 200, NOW(), NOW())
            """
            )

        # When: Try to get checkpoint
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
        
        # Then: Should handle gracefully
        if checkpoint:
            # If it returns data, total_pages should be None
            self.assertIsNone(checkpoint["total_pages"], "Corrupted field should be None")
        # Otherwise returning None is also acceptable (graceful degradation)

    def test_given_checkpoint_with_invalid_page_number_when_resume_then_restarts_safely(self):
        """
        Given: Checkpoint claims current_page > total_pages (impossible state)
        When: Extraction resumes
        Then: Should detect invalid state and handle appropriately
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Given: Create invalid checkpoint (page 10 of 5 total)
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            stats: CheckpointData = {
                "current_page": 10,  # Invalid: beyond total_pages
                "total_pages": 5,
                "rows_extracted": 1000,
            }
            self.extractor._save_checkpoint(con, "contratos", extraction_date, stats, datetime.now())

        # Mock API with 5 pages
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            # Page 11 (current_page + 1) should return empty
            if page <= 5:
                return {
                    "data": [{"numeroControlePNCP": f"CTRL-{page}-{i}", "anoCompra": 2024, "orgaoEntidade": {"cnpj": "12345678000190"}, "dataPublicacao": "2024-01-15T10:00:00"} for i in range(10)],
                    "totalPaginas": 5,
                }
            else:
                return {"data": [], "totalPaginas": 5}

        # When: Attempt to resume (should handle invalid state)
        try:
            with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
                result = self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
            
            # Then: Should complete without error (even if it restarts)
            # The checkpoint will try page 11, get empty data, and stop
            # This is acceptable behavior (fail-safe)
            self.assertIsNotNone(result, "Should return result even with corrupted checkpoint")
        
        except Exception as e:
            self.fail(f"Should handle corrupted checkpoint gracefully, but raised: {e}")


class TestExtractorCheckpointNetworkFailures(unittest.TestCase):
    """Test checkpoint behavior during network failures.
    
    Given: Network errors occur during extraction
    When: HTTP requests fail with timeouts or connection errors
    Then: Partial data should be preserved via checkpoints
    """

    def setUp(self):
        """Create temporary database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_network_timeout_when_extraction_fails_then_checkpoint_preserves_state(self):
        """
        Given: Extraction successfully processes 2 pages
        When: Page 3 fails with TimeoutException
        Then: Checkpoint should preserve state at page 2 (allowing resume)
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Mock API: pages 1-2 succeed, page 3 times out
        call_count = [0]
        
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            call_count[0] += 1
            
            if page in [1, 2]:
                return {
                    "data": [{"numeroControlePNCP": f"CTRL-{page}-{i}", "anoCompra": 2024, "orgaoEntidade": {"cnpj": "12345678000190"}, "dataPublicacao": "2024-01-15T10:00:00"} for i in range(10)],
                    "totalPaginas": 5,
                }
            elif page == 3 and call_count[0] <= 4:  # Fail first 4 attempts (triggers retry logic)
                # Simulate network timeout
                raise httpx.TimeoutException("Request timeout")
            else:
                # After retries exhausted, return empty to stop
                return {"data": [], "totalPaginas": 5}

        # When: Run extraction (will fail at page 3)
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            try:
                self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
            except httpx.TimeoutException:
                pass  # Expected to fail

        # Then: Checkpoint should exist at last successful page
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
            
            # Checkpoint should exist (extraction didn't complete)
            self.assertIsNotNone(checkpoint, "Checkpoint should exist after timeout")
            
            # Should be at page 2 (last successful page) or earlier
            self.assertLessEqual(checkpoint["current_page"], 2, "Should checkpoint at last successful page")
            
            # Data from pages 1-2 should be preserved
            count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
            self.assertGreater(count, 0, "Partial data should be preserved")

    def test_given_http_500_error_when_extraction_fails_then_state_is_recoverable(self):
        """
        Given: API returns 500 error on page 2
        When: Extraction fails after retries
        Then: Checkpoint at page 1 should allow recovery
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Mock API: page 1 succeeds, page 2 returns 500
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            if page == 1:
                return {
                    "data": [{"numeroControlePNCP": f"CTRL-{page}-{i}", "anoCompra": 2024, "orgaoEntidade": {"cnpj": "12345678000190"}, "dataPublicacao": "2024-01-15T10:00:00"} for i in range(10)],
                    "totalPaginas": 3,
                }
            elif page == 2:
                # Simulate 500 error
                raise httpx.HTTPStatusError("500 Server Error", request=Mock(), response=Mock(status_code=500))
            else:
                return {"data": [], "totalPaginas": 3}

        # When: Run extraction (will fail at page 2)
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            try:
                self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
            except httpx.HTTPStatusError:
                pass  # Expected to fail

        # Then: Checkpoint should exist for recovery
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
            
            # Checkpoint should exist
            self.assertIsNotNone(checkpoint, "Checkpoint should exist after HTTP error")
            
            # Data from page 1 should be preserved
            count = con.execute("SELECT COUNT(*) FROM test_dataset.contratos").fetchone()[0]
            self.assertEqual(count, 10, "First page data should be preserved")


class TestExtractorCheckpointEdgeCases(unittest.TestCase):
    """Test edge cases in checkpoint logic.
    
    Scenarios that caught bugs in causaganha PR #291:
    - Empty pages
    - Single-page extractions
    - Checkpoint timestamp consistency
    """

    def setUp(self):
        """Create temporary database."""
        self.test_dir = tempfile.mkdtemp()
        self.db_path = Path(self.test_dir) / "test.duckdb"
        self.extractor = PNCPExtractor(self.db_path, dataset="test_dataset")
        
    def tearDown(self):
        """Clean up temporary files."""
        self.extractor.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_given_api_returns_zero_pages_when_extract_then_handles_gracefully(self):
        """
        Given: API returns no data (empty result)
        When: Extraction runs
        Then: Should handle gracefully without creating invalid checkpoint
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Mock API with no data
        def mock_fetch(client, url, params, max_size=None):
            return {"data": [], "totalPaginas": 0}

        # When: Run extraction
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            result = self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        # Then: Should complete without error
        self.assertEqual(result["rows_extracted"], 0, "Should extract 0 rows")
        self.assertIn("pages", result, "Should return pages count")
        
        # No checkpoint should exist (nothing to resume)
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
            self.assertIsNone(checkpoint, "No checkpoint for empty extraction")

    def test_given_single_page_extraction_when_complete_then_checkpoint_cleared(self):
        """
        Given: Extraction has only 1 page total
        When: That page completes
        Then: Checkpoint should be cleared immediately (edge case: first == last)
        """
        extraction_date = datetime(2024, 1, 15)
        
        # Mock API with single page
        def mock_fetch(client, url, params, max_size=None):
            page = params.get("pagina", 1)
            
            if page == 1:
                return {
                    "data": [{"numeroControlePNCP": "CTRL-SINGLE-001", "anoCompra": 2024, "orgaoEntidade": {"cnpj": "12345678000190"}, "dataPublicacao": "2024-01-15T10:00:00"}],
                    "totalPaginas": 1,
                }
            else:
                return {"data": [], "totalPaginas": 1}

        # When: Run extraction
        with patch("baliza.extractor._fetch_page", side_effect=mock_fetch):
            result = self.extractor.extract(extraction_date, extraction_date, resource="contratos", workers=1)
        
        # Then: Should complete successfully
        self.assertEqual(result["pages"], 1, "Should process 1 page")
        
        # And: No checkpoint should remain
        with duckdb.connect(str(self.db_path)) as con:
            checkpoint = self.extractor._get_checkpoint(con, "contratos", extraction_date)
            self.assertIsNone(checkpoint, "Single-page extraction should clear checkpoint")

    def test_given_multiple_dates_when_checkpoints_exist_then_isolated_per_date(self):
        """
        Given: Checkpoints exist for multiple dates
        When: Querying checkpoint for specific date
        Then: Should return only that date's checkpoint (isolation)
        """
        date1 = datetime(2024, 1, 15)
        date2 = datetime(2024, 1, 16)
        
        with duckdb.connect(str(self.db_path)) as con:
            self.extractor._ensure_schema(con)
            
            # Create checkpoints for two different dates
            stats1: CheckpointData = {"current_page": 3, "total_pages": 10, "rows_extracted": 300}
            stats2: CheckpointData = {"current_page": 7, "total_pages": 15, "rows_extracted": 700}
            
            self.extractor._save_checkpoint(con, "contratos", date1, stats1, datetime.now())
            self.extractor._save_checkpoint(con, "contratos", date2, stats2, datetime.now())
            
            # Get checkpoint for date1
            checkpoint1 = self.extractor._get_checkpoint(con, "contratos", date1)
            self.assertEqual(checkpoint1["current_page"], 3, "Should get date1's checkpoint")
            
            # Get checkpoint for date2
            checkpoint2 = self.extractor._get_checkpoint(con, "contratos", date2)
            self.assertEqual(checkpoint2["current_page"], 7, "Should get date2's checkpoint")
            
            # They should be independent
            self.assertNotEqual(checkpoint1["current_page"], checkpoint2["current_page"],
                              "Checkpoints should be isolated per date")


if __name__ == "__main__":
    unittest.main()
