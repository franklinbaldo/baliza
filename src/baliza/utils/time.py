"""
Utilitários de tempo para o pipeline Baliza.
"""
from datetime import datetime, timedelta
from typing import Generator, Tuple


def date_range_slicer(
    start_date: datetime, 
    end_date: datetime, 
    chunk_size_days: int
) -> Generator[Tuple[str, str], None, None]:
    """
    Fatia um período de tempo em chunks menores para processamento paralelo.
    
    Args:
        start_date: Data inicial
        end_date: Data final  
        chunk_size_days: Tamanho do chunk em dias
        
    Yields:
        Tuplas (start_str, end_str) no formato 'YYYYMMDD'
    """
    current = start_date
    
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_size_days - 1), end_date)
        
        yield (
            current.strftime("%Y%m%d"),
            chunk_end.strftime("%Y%m%d")
        )
        
        current = chunk_end + timedelta(days=1)