"""
PNCP Data Extraction Module

This module contains all components needed for extracting data from PNCP API
and exporting to Parquet files using DLT (Data Load Tool).

Key components:
- config.py: API configuration and REST client setup
- pipeline.py: Main extraction pipelines and sources
- gap_detector.py: Smart incremental loading with gap detection
"""

from .pipeline import (
    pncp_source,
    run_priority_extraction,
    run_modalidade_extraction,
    create_default_pipeline
)

from .gap_detector import (
    find_extraction_gaps,
    DataGap
)

__all__ = [
    # Main pipeline functions
    "pncp_source",
    "run_priority_extraction", 
    "run_modalidade_extraction",
    "create_default_pipeline",
    
    # Gap detection
    "find_extraction_gaps",
    "DataGap"
]