"""
Completion tracking utilities for PNCP data extraction.
Extracted from pipeline.py to break circular dependencies.
"""

from pathlib import Path
from typing import Dict, List
from datetime import datetime

# Note: Using pathlib throughout for modern Python practices



def is_extraction_completed(output_dir: str, endpoint: str, month: str) -> bool:
    """
    Check if extraction is completed for endpoint/month combination.
    
    Args:
        output_dir: Base output directory
        endpoint: Endpoint name
        month: Month in YYYY-MM format
        
    Returns:
        True if extraction is completed, False otherwise
    """
    year, month_num = month.split("-")
    marker_path = Path(output_dir) / endpoint / year / month_num / ".completed"
    return marker_path.exists()


def get_completed_extractions(output_dir: str) -> Dict[str, List[str]]:
    """
    Get completed extractions by scanning .completed marker files.
    
    Args:
        output_dir: Base output directory to scan
    
    Returns:
        Dict mapping endpoint names to lists of completed months (YYYY-MM format)
    """
    completed = {}
    output_path = Path(output_dir)
    
    if not output_path.exists():
        return completed
    
    for endpoint_dir in output_path.iterdir():
        if not endpoint_dir.is_dir():
            continue
            
        completed[endpoint_dir.name] = []
        
        for year_dir in endpoint_dir.iterdir():
            if not year_dir.is_dir():
                continue
                
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                    
                # Check for .completed marker
                marker_path = month_dir / ".completed"
                if marker_path.exists():
                    month_key = f"{year_dir.name}-{month_dir.name}"
                    completed[endpoint_dir.name].append(month_key)
    
    return completed


def _get_months_in_range(start_date: str, end_date: str) -> List[str]:
    """
    Get list of months (YYYY-MM format) between start and end dates.
    
    Note: This is a helper function used by gap detection and completion tracking.
    Currently kept private as it's specific to the completion tracking logic.
    
    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
    
    Returns:
        List of month strings in YYYY-MM format
    """
    start_dt = datetime.strptime(start_date, "%Y%m%d").date()
    end_dt = datetime.strptime(end_date, "%Y%m%d").date()
    
    months = []
    current = start_dt.replace(day=1)  # Start of month
    
    while current <= end_dt:
        months.append(current.strftime("%Y-%m"))
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return months


def mark_extraction_completed(output_dir: str, start_date: str, end_date: str, endpoints: List[str]):
    """
    Mark extractions as completed by creating .completed marker files.
    
    Args:
        output_dir: Base output directory
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format  
        endpoints: List of endpoints that were extracted
    """
    months = _get_months_in_range(start_date, end_date)
    
    for endpoint in endpoints:
        for month in months:
            year, month_num = month.split("-")
            marker_dir = Path(output_dir) / endpoint / year / month_num
            marker_dir.mkdir(parents=True, exist_ok=True)
            
            marker_path = marker_dir / ".completed"
            with marker_path.open("w") as f:
                # Marker file contains basic completion metadata
                # Extended metrics are tracked in the DLT pipeline state
                f.write(f"Completed at: {datetime.now().isoformat()}\n")
                f.write(f"Date range: {start_date} to {end_date}\n")
