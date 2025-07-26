"""
Completion tracking utilities for PNCP data extraction.
Extracted from pipeline.py to break circular dependencies.
"""

import os
from typing import Dict, List, Set
from datetime import datetime, date

# TODO: Consider adding `is_extraction_completed` function to this module
#       for completeness, as it is a logical counterpart to `mark_extraction_completed`.
#       Also, evaluate consistently using `pathlib` instead of `os.path` for
#       all file system operations within this module for improved readability
#       and modern Python practices.



def get_completed_extractions(output_dir: str) -> Dict[str, List[str]]:
    """
    Get completed extractions by scanning .completed marker files.
    
    Args:
        output_dir: Base output directory to scan
    
    Returns:
        Dict mapping endpoint names to lists of completed months (YYYY-MM format)
    """
    completed = {}
    
    if not os.path.exists(output_dir):
        return completed
    
    for endpoint_dir in os.listdir(output_dir):
        endpoint_path = os.path.join(output_dir, endpoint_dir)
        if not os.path.isdir(endpoint_path):
            continue
            
        completed[endpoint_dir] = []
        
        for year_dir in os.listdir(endpoint_path):
            year_path = os.path.join(endpoint_path, year_dir)
            if not os.path.isdir(year_path):
                continue
                
            for month_dir in os.listdir(year_path):
                month_path = os.path.join(year_path, month_dir)
                if not os.path.isdir(month_path):
                    continue
                    
                # Check for .completed marker
                marker_path = os.path.join(month_path, ".completed")
                if os.path.exists(marker_path):
                    month_key = f"{year_dir}-{month_dir}"
                    completed[endpoint_dir].append(month_key)
    
    return completed


def _get_months_in_range(start_date: str, end_date: str) -> List[str]:
    """
    Get list of months (YYYY-MM format) between start and end dates.
    
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
            marker_dir = os.path.join(output_dir, endpoint, year, month_num)
            os.makedirs(marker_dir, exist_ok=True)
            
            marker_path = os.path.join(marker_dir, ".completed")
            with open(marker_path, "w") as f:
                f.write(f"Completed at: {datetime.now().isoformat()}\n")
                f.write(f"Date range: {start_date} to {end_date}\n")
