from datetime import date
from pydantic import BaseModel

class ParquetSchema(BaseModel):
    intimation_id: int | None
    process_number: str | None
    tribunal: str | None
    decision_date: date | None
    download_url: str | None
    needs_download: bool | None
    ia_url: str | None
