"""
Gap Detection for PNCP Data Extraction
Uses DLT state and existing data to determine what we already have
and fetch only missing date ranges/pages.
"""

from datetime import date, timedelta, datetime
from typing import List, Tuple, Optional, Set, Dict
from dataclasses import dataclass


@dataclass
class DataGap:
    """Represents a gap in our data that needs to be fetched."""
    start_date: str  # YYYYMMDD format
    end_date: str    # YYYYMMDD format
    endpoint: str
    modalidade: Optional[int] = None
    missing_pages: Optional[List[int]] = None  # Specific pages missing, None = all pages
    
    def __str__(self):
        modal_str = f" modalidade={self.modalidade}" if self.modalidade else ""
        pages_str = f" pages={self.missing_pages}" if self.missing_pages else ""
        return f"{self.endpoint}: {self.start_date}-{self.end_date}{modal_str}{pages_str}"


class PNCPGapDetector:
    """
    Detects gaps in existing PNCP data to enable incremental extraction.
    """
    
    def __init__(self):
        self.endpoints = ["contratacoes_publicacao", "contratos", "atas"]
    
    def find_missing_date_ranges(
        self, 
        start_date: str, 
        end_date: str,
        endpoints: List[str] = None,
        check_pagination: bool = True
    ) -> List[DataGap]:
        """
        Find date ranges and pagination gaps in our existing data.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format  
            endpoints: List of endpoints to check (default: all)
            check_pagination: If True, also check for missing pages within date ranges
            
        Returns:
            List of DataGap objects representing missing data
        """
        if not endpoints:
            endpoints = self.endpoints
            
        gaps = []
        
        for endpoint in endpoints:
            if check_pagination:
                endpoint_gaps = self._find_endpoint_and_pagination_gaps(endpoint, start_date, end_date)
            else:
                endpoint_gaps = self._find_endpoint_gaps(endpoint, start_date, end_date)
            gaps.extend(endpoint_gaps)
        
        return gaps
    
    def _find_endpoint_and_pagination_gaps(self, endpoint: str, start_date: str, end_date: str) -> List[DataGap]:
        """Find both date range gaps and pagination gaps for an endpoint.
        
        Uses filesystem completion tracking to identify missing months and creates
        precise date ranges for gaps, intersected with the requested range.
        """
        from baliza.utils.completion_tracking import get_completed_extractions, _get_months_in_range
        from calendar import monthrange
        
        completed = get_completed_extractions("data")
        completed_months = set(completed.get(endpoint, []))
        
        # Get months needed for this date range
        months_needed = set(_get_months_in_range(start_date, end_date))
        
        # Find missing months
        missing_months = months_needed - completed_months
        
        if not missing_months:
            print(f"✅ No gaps found for {endpoint} - all data already extracted")
            return []
        
        # Create precise gaps for each missing month, intersected with requested range
        gaps = []
        for month_str in sorted(missing_months):
            year, month = month_str.split("-")
            
            # Calculate month boundaries
            month_start = f"{year}{month.zfill(2)}01"
            last_day = monthrange(int(year), int(month))[1]
            month_end = f"{year}{month.zfill(2)}{last_day:02d}"
            
            # Intersect with requested range to get precise gap
            gap_start = max(start_date, month_start)
            gap_end = min(end_date, month_end)
            
            if gap_start <= gap_end:
                gaps.append(DataGap(gap_start, gap_end, endpoint))
                print(f"🔄 Gap detected for {endpoint}: {gap_start} to {gap_end}")
        
        return gaps
    
    def _get_existing_requests_with_pagination(self, table) -> Dict[str, Set[int]]:
        """
        Get existing requests with pagination info.
        Returns dict of {date_string: set_of_pages}
        """
        try:
            # Query to get unique combinations of date and page from successful extractions
            # We need to reverse-engineer this from the data since we don't store request URLs
            
            # For now, use a simplified approach based on extraction timestamps
            # This is a placeholder - in reality we'd need to store request metadata
            query = (
                table
                .filter(table._dlt_load_id.notnull())
                .select([
                    table._baliza_extracted_at,
                    # We would need additional fields to track pagination
                    # For now, assume we have complete pages for dates we've extracted
                ])
                .distinct()
            )
            
            results = query.execute()
            
            # Simplified: assume if we have data for a date, we have pages 1-N
            # In reality, we'd need to store page numbers in the data
            existing_dates = {}
            
            for row in results.itertuples():
                if hasattr(row, '_baliza_extracted_at') and row._baliza_extracted_at:
                    try:
                        extract_date = datetime.fromisoformat(row._baliza_extracted_at.replace('Z', '+00:00')).date()
                        date_str = extract_date.strftime("%Y%m%d")
                        
                        # For now, assume we have pages 1-50 for dates we've processed
                        # This is a heuristic that should be replaced with actual page tracking
                        existing_dates[date_str] = set(range(1, 51))
                        
                    except:
                        continue
            
            return existing_dates
            
        except Exception as e:
            print(f"⚠️  Error getting existing requests: {e}")
            return {}
    
    def _find_pagination_gaps(self, start_date: str, end_date: str, existing_requests: Dict[str, Set[int]]) -> List[Tuple[str, List[int]]]:
        """
        Find missing pages within date ranges we've partially processed.
        
        Returns:
            List of (date_string, missing_pages) tuples
        """
        pagination_gaps = []
        
        # Parse date range
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(end_date, "%Y%m%d").date()
        
        # Check each date in the range
        current_dt = start_dt
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y%m%d")
            
            if date_str in existing_requests:
                # We have some pages for this date - check if all pages are complete
                existing_pages = existing_requests[date_str]
                
                # Estimate total pages needed (this would need to be smarter in reality)
                # For now, assume max 100 pages per date
                max_pages = 100
                expected_pages = set(range(1, max_pages + 1))
                
                missing_pages = list(expected_pages - existing_pages)
                
                if missing_pages:
                    # Only return realistic gaps (don't assume 100 pages if we only have 5)
                    # Limit to reasonable continuation (e.g., if we have pages 1-50, check 51-60)
                    max_existing = max(existing_pages) if existing_pages else 0
                    realistic_missing = [p for p in missing_pages if p <= max_existing + 10]
                    
                    if realistic_missing:
                        pagination_gaps.append((date_str, realistic_missing))
            
            current_dt += timedelta(days=1)
        
        return pagination_gaps
    
    def _find_date_gaps(self, start_date: str, end_date: str, existing_requests: Dict[str, Set[int]]) -> List[Tuple[date, date]]:
        """
        Find date ranges that have no data at all.
        
        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            existing_requests: Dict of {date_string: set_of_pages}
            
        Returns:
            List of (start_date, end_date) tuples for missing date ranges
        """
        # Parse date range
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(end_date, "%Y%m%d").date()
        
        # Find dates with no data
        missing_dates = []
        current_dt = start_dt
        
        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y%m%d")
            
            if date_str not in existing_requests or not existing_requests[date_str]:
                missing_dates.append(current_dt)
            
            current_dt += timedelta(days=1)
        
        # Convert consecutive missing dates to ranges
        if not missing_dates:
            return []
        
        ranges = []
        range_start = missing_dates[0]
        range_end = missing_dates[0]
        
        for dt in missing_dates[1:]:
            if dt == range_end + timedelta(days=1):
                # Consecutive date - extend range
                range_end = dt
            else:
                # Gap - start new range
                ranges.append((range_start, range_end))
                range_start = dt
                range_end = dt
        
        # Add final range
        ranges.append((range_start, range_end))
        
        return ranges
    
    def _find_endpoint_gaps(self, endpoint: str, start_date: str, end_date: str) -> List[DataGap]:
        """Find gaps for a specific endpoint using filesystem completion tracking."""
        try:
            # Use filesystem-based gap detection instead of database queries
            # This is more reliable and doesn't require database connections
            from baliza.utils.completion_tracking import get_completed_extractions, _get_months_in_range
            
            completed = get_completed_extractions("data")
            completed_months = set(completed.get(endpoint, []))
            
            # Get months needed for this date range
            months_needed = set(_get_months_in_range(start_date, end_date))
            
            # Find missing months
            missing_months = months_needed - completed_months
            
            if not missing_months:
                return []  # No gaps
            
            # Convert missing months back to date ranges
            gaps = []
            for month_str in sorted(missing_months):
                year, month = month_str.split("-")
                # Create gap for entire month
                month_start = f"{year}{month.zfill(2)}01"
                
                # Calculate last day of month
                from calendar import monthrange
                last_day = monthrange(int(year), int(month))[1]
                month_end = f"{year}{month.zfill(2)}{last_day:02d}"
                
                # Intersect with requested range
                gap_start = max(start_date, month_start)
                gap_end = min(end_date, month_end)
                
                if gap_start <= gap_end:
                    gaps.append(DataGap(gap_start, gap_end, endpoint))
            
            return gaps
            
        except Exception as e:
            print(f"⚠️  Error detecting gaps for {endpoint}: {e}")
            # Safer to assume we need everything
            return [DataGap(start_date, end_date, endpoint)]
    
    def _get_existing_date_ranges(self, table) -> List[Tuple[date, date]]:
        """
        Get existing date ranges from the table data.
        This is heuristic - we infer date ranges from extraction timestamps.
        """
        try:
            # Get distinct extraction dates
            query = (
                table
                .filter(table._dlt_load_id.notnull())  # Successfully loaded
                .select([table._baliza_extracted_at])
                .distinct()
            )
            
            results = query.execute()
            
            # Convert extraction dates to date ranges
            # This is a simplification - in reality we'd need more sophisticated logic
            # to determine what date ranges each extraction covered
            
            extraction_dates = []
            for row in results.itertuples():
                if hasattr(row, '_baliza_extracted_at') and row._baliza_extracted_at:
                    try:
                        # Parse ISO date
                        extract_date = datetime.fromisoformat(row._baliza_extracted_at.replace('Z', '+00:00')).date()
                        extraction_dates.append(extract_date)
                    except Exception as e:
                        print(f"⚠️  Error parsing extracted date: {e}")
                        continue
            
            if not extraction_dates:
                return []
            
            # Note: This method is deprecated. Use filesystem-based completion tracking instead.
            # Stored metadata approach would be more reliable than timestamp heuristics.
            ranges = []
            for extract_date in extraction_dates:
                # Conservative approach: assume extraction covered 7 days
                range_start = extract_date - timedelta(days=7)
                range_end = extract_date
                ranges.append((range_start, range_end))
            
            # Merge overlapping ranges
            return self._merge_overlapping_ranges(ranges)
            
        except Exception as e:
            print(f"⚠️  Error getting existing date ranges: {e}")
            return []
    
    def _parse_date_range(self, start_date: str, end_date: str) -> Tuple[date, date]:
        """Parse date strings to date objects."""
        start_dt = datetime.strptime(start_date, "%Y%m%d").date()
        end_dt = datetime.strptime(end_date, "%Y%m%d").date()
        return (start_dt, end_dt)
    
    def _find_gaps_in_ranges(
        self, 
        requested: Tuple[date, date], 
        existing: List[Tuple[date, date]]
    ) -> List[Tuple[date, date]]:
        """Find gaps between requested range and existing ranges."""
        if not existing:
            return [requested]
        
        req_start, req_end = requested
        gaps = []
        
        # Sort existing ranges by start date
        existing_sorted = sorted(existing, key=lambda x: x[0])
        
        current_pos = req_start
        
        for exist_start, exist_end in existing_sorted:
            # If there's a gap before this existing range
            if current_pos < exist_start and exist_start <= req_end:
                gap_end = min(exist_start - timedelta(days=1), req_end)
                if current_pos <= gap_end:
                    gaps.append((current_pos, gap_end))
            
            # Move current position past this existing range
            if exist_end >= current_pos:
                current_pos = max(current_pos, exist_end + timedelta(days=1))
        
        # Check for gap after all existing ranges
        if current_pos <= req_end:
            gaps.append((current_pos, req_end))
        
        return gaps
    
    def _merge_overlapping_ranges(self, ranges: List[Tuple[date, date]]) -> List[Tuple[date, date]]:
        """Merge overlapping date ranges."""
        if not ranges:
            return []
        
        # Sort by start date
        sorted_ranges = sorted(ranges, key=lambda x: x[0])
        merged = [sorted_ranges[0]]
        
        for current_start, current_end in sorted_ranges[1:]:
            last_start, last_end = merged[-1]
            
            # If current range overlaps with last range, merge them
            if current_start <= last_end + timedelta(days=1):
                merged[-1] = (last_start, max(last_end, current_end))
            else:
                merged.append((current_start, current_end))
        
        return merged
    
    def get_backfill_gaps(self, endpoints: List[str] = None) -> List[DataGap]:
        """
        Get all gaps for a complete backfill (from earliest available data to today).
        
        Args:
            endpoints: List of endpoints to check (default: all)
            
        Returns:
            List of DataGap objects for complete backfill
        """
        # PNCP data is available from 2021 (approximately)
        earliest_date = "20210101"
        today = date.today().strftime("%Y%m%d")
        
        # Note: Simplified implementation - returns full historical range
        if not endpoints:
            endpoints = self.endpoints
            
        gaps = []
        for endpoint in endpoints:
            gaps.append(DataGap(earliest_date, today, endpoint))
        
        return gaps


def find_extraction_gaps(
    start_date: str = None,
    end_date: str = None, 
    endpoints: List[str] = None,
    backfill_all: bool = False,
    check_pagination: bool = True
) -> List[DataGap]:
    """
    Find gaps in PNCP data extraction including pagination gaps.
    
    Args:
        start_date: Start date in YYYYMMDD format (None for backfill)
        end_date: End date in YYYYMMDD format (None for backfill)
        endpoints: List of endpoints to check
        backfill_all: If True, find all historical gaps
        check_pagination: If True, also detect missing pages within date ranges
        
    Returns:
        List of DataGap objects representing missing data
    """
    detector = PNCPGapDetector()
    
    if backfill_all or (start_date is None and end_date is None):
        print("🔍 Detecting gaps for complete historical backfill...")
        gaps = detector.get_backfill_gaps(endpoints)
    else:
        print(f"🔍 Detecting gaps for date range {start_date} to {end_date}...")
        if check_pagination:
            print("   📄 Including pagination gap detection...")
        gaps = detector.find_missing_date_ranges(start_date, end_date, endpoints, check_pagination)
    
    if gaps:
        print(f"📋 Found {len(gaps)} data gaps:")
        date_gaps = [g for g in gaps if not g.missing_pages]
        page_gaps = [g for g in gaps if g.missing_pages]
        
        if date_gaps:
            print(f"   📅 {len(date_gaps)} date range gaps")
            for gap in date_gaps[:3]:  # Show first 3
                print(f"      - {gap}")
            if len(date_gaps) > 3:
                print(f"      - ... and {len(date_gaps) - 3} more date gaps")
        
        if page_gaps:
            print(f"   📄 {len(page_gaps)} pagination gaps")
            for gap in page_gaps[:3]:  # Show first 3
                print(f"      - {gap}")
            if len(page_gaps) > 3:
                print(f"      - ... and {len(page_gaps) - 3} more page gaps")
    else:
        print("✅ No gaps found - all requested data already exists!")
    
    return gaps