"""
Endpoint utilities for PNCP API integration
"""

from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

from ..config import ENDPOINT_CONFIG, MODALIDADE_CONTRATACAO, settings


class URLBuilder:
    """Builds PNCP API endpoint URLs with proper parameters"""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or settings.PNCP_API_BASE_URL

    def build_url(self, endpoint_name: str, **params) -> str:
        """Build complete URL for endpoint with parameters"""
        config = ENDPOINT_CONFIG.get(endpoint_name)
        if not config:
            raise ValueError(f"Unknown endpoint: {endpoint_name}")

        # Validate required parameters
        missing_params = []
        for required_param in config["required_params"]:
            if required_param not in params:
                missing_params.append(required_param)

        if missing_params:
            raise ValueError(
                f"Missing required parameters for {endpoint_name}: {missing_params}"
            )

        # Filter only valid parameters
        valid_params = config["required_params"] + config.get("optional_params", [])
        filtered_params = {k: v for k, v in params.items() if k in valid_params}

        # Build URL
        path = config["path"]
        url = f"{self.base_url}{path}"

        if filtered_params:
            url += f"?{urlencode(filtered_params)}"

        return url

    def get_endpoint_config(self, endpoint_name: str) -> Dict:
        """Get configuration for specific endpoint"""
        return ENDPOINT_CONFIG.get(endpoint_name, {})


class DateRangeHelper:
    """Helper for generating date ranges for API requests"""

    @staticmethod
    def format_date(date_obj: date) -> str:
        """Format date as YYYYMMDD for PNCP API"""
        return date_obj.strftime("%Y%m%d")

    @staticmethod
    def parse_date(date_str: str) -> date:
        """Parse YYYYMMDD string to date object"""
        return datetime.strptime(date_str, "%Y%m%d").date()

    @classmethod
    def get_last_n_days(cls, n_days: int) -> Tuple[str, str]:
        """Get date range for last N days"""
        end_date = date.today()
        start_date = end_date - timedelta(days=n_days)
        return cls.format_date(start_date), cls.format_date(end_date)

    @classmethod
    def get_current_month(cls) -> Tuple[str, str]:
        """Get date range for current month"""
        today = date.today()
        start_date = today.replace(day=1)
        return cls.format_date(start_date), cls.format_date(today)

    @classmethod
    def get_previous_month(cls) -> Tuple[str, str]:
        """Get date range for previous month"""
        today = date.today()
        last_month = today.replace(day=1) - timedelta(days=1)
        start_date = last_month.replace(day=1)
        end_date = last_month
        return cls.format_date(start_date), cls.format_date(end_date)

    @classmethod
    def get_month_range(cls, year: int, month: int) -> Tuple[str, str]:
        """Get date range for a specific month"""
        import calendar

        _, num_days = calendar.monthrange(year, month)
        start_date = date(year, month, 1)
        end_date = date(year, month, num_days)
        return cls.format_date(start_date), cls.format_date(end_date)

    @classmethod
    def chunk_date_range(
        cls, start_date: str, end_date: str, chunk_days: int
    ) -> List[Tuple[str, str]]:
        """Split date range into smaller chunks"""
        start = cls.parse_date(start_date)
        end = cls.parse_date(end_date)

        chunks = []
        current = start

        while current <= end:
            chunk_end = min(current + timedelta(days=chunk_days - 1), end)
            chunks.append((cls.format_date(current), cls.format_date(chunk_end)))
            current = chunk_end + timedelta(days=1)

        return chunks

    @staticmethod
    def parse_date_string(date_str: str) -> date:
        """Parse YYYY-MM-DD string to date object"""
        return datetime.strptime(date_str, "%Y-%m-%d").date()

    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> bool:
        """Validate date range parameters"""
        try:
            start = DateRangeHelper.parse_date_string(start_date)
            end = DateRangeHelper.parse_date_string(end_date)
            return start <= end
        except ValueError:
            return False


class ModalidadeHelper:
    """Helper for working with modalidades de contratação"""

    @staticmethod
    def get_modalidade_name(codigo: int) -> str:
        """Get modalidade name by code"""
        return MODALIDADE_CONTRATACAO.get(codigo, f"Modalidade {codigo}")

    @staticmethod
    def get_high_priority_modalidades() -> List[int]:
        """Get list of high priority modalidade codes"""
        return settings.HIGH_PRIORITY_MODALIDADES

    @staticmethod
    def get_all_modalidades() -> List[int]:
        """Get all available modalidade codes"""
        return list(MODALIDADE_CONTRATACAO.keys())

    @classmethod
    def get_modalidades_by_priority(cls, priority: str = "high") -> List[int]:
        """Get modalidades filtered by priority level"""
        if priority == "high":
            return cls.get_high_priority_modalidades()
        elif priority == "all":
            return cls.get_all_modalidades()
        else:
            # Medium and low priority
            high_priority = set(cls.get_high_priority_modalidades())
            all_modalidades = set(cls.get_all_modalidades())
            return list(all_modalidades - high_priority)


class PaginationHelper:
    """Helper for handling API pagination"""

    @staticmethod
    def get_page_size(endpoint_name: str, requested_size: Optional[int] = None) -> int:
        """Get appropriate page size for endpoint"""
        config = ENDPOINT_CONFIG.get(endpoint_name, {})
        limits = config.get("page_size_limits", {"min": 10, "max": 500})
        default_size = config.get("default_page_size", 500)

        if requested_size is None:
            return default_size

        # Clamp to limits
        return max(limits["min"], min(requested_size, limits["max"]))

    @staticmethod
    def calculate_total_pages(total_records: int, page_size: int) -> int:
        """Calculate total number of pages needed"""
        return (total_records + page_size - 1) // page_size

    @staticmethod
    def has_more_pages(current_page: int, total_pages: int) -> bool:
        """Check if there are more pages to fetch"""
        return current_page < total_pages

    @staticmethod
    def get_page_ranges(total_pages: int, batch_size: int) -> List[Tuple[int, int]]:
        """Get page ranges for parallel processing"""
        ranges = []
        for i in range(1, total_pages + 1, batch_size):
            start_page = i
            end_page = min(i + batch_size - 1, total_pages)
            ranges.append((start_page, end_page))
        return ranges

    @staticmethod
    def estimate_requests_needed(
        date_range_days: int, avg_records_per_day: int, records_per_page: int
    ) -> int:
        """Estimate requests needed for a given date range"""
        total_records = date_range_days * avg_records_per_day
        return (total_records + records_per_page - 1) // records_per_page


class EndpointValidator:
    """Validates endpoint requests and responses"""

    @staticmethod
    def validate_date_range(start_date: str, end_date: str) -> None:
        """Validate date range parameters"""
        try:
            start = DateRangeHelper.parse_date(start_date)
            end = DateRangeHelper.parse_date(end_date)
        except ValueError as e:
            raise ValueError(f"Invalid date format. Use YYYYMMDD: {e}")

        if start > end:
            raise ValueError("Start date must be before end date")

        # Check if range is too large
        max_days = settings.MAX_DATE_RANGE_DAYS
        if (end - start).days > max_days:
            raise ValueError(f"Date range too large. Maximum {max_days} days allowed")

    @staticmethod
    def validate_modalidade(modalidade: int) -> bool:
        """Validate modalidade code"""
        if modalidade is None:
            return False
        return modalidade in MODALIDADE_CONTRATACAO

    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """Validate date format"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def validate_pagination_params(page: int, page_size: int) -> bool:
        """Validate pagination parameters"""
        return page >= 1 and 1 <= page_size <= 100

    @staticmethod
    def validate_pagination(page: int, page_size: int, endpoint_name: str) -> None:
        """Validate pagination parameters"""
        if page < 1:
            raise ValueError("Page number must be >= 1")

        config = ENDPOINT_CONFIG.get(endpoint_name, {})
        limits = config.get("page_size_limits", {"min": 10, "max": 500})

        if page_size < limits["min"] or page_size > limits["max"]:
            raise ValueError(
                f"Page size must be between {limits['min']} and {limits['max']}"
            )

    def validate_endpoint_params(self, endpoint_name: str, params: dict) -> dict:
        """Validate endpoint parameters"""
        errors = []
        if not self.validate_date_format(params.get("data_inicial")):
            errors.append("Invalid data_inicial")
        if not self.validate_date_format(params.get("data_final")):
            errors.append("Invalid data_final")
        if not self.validate_modalidade(params.get("modalidade")):
            errors.append("Invalid modalidade")
        if not self.validate_pagination_params(params.get("pagina"), 50):
            errors.append("Invalid pagina")
        return {"valid": not errors, "errors": errors}


# Convenience functions for common operations
def build_contratacao_url(
    data_inicial: str, data_final: str, modalidade: int, pagina: int = 1, **kwargs
) -> str:
    """Build URL for contratações/publicação endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("contratacoes_publicacao", **params)


def build_contratos_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for contratos endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("contratos", **params)


def build_atas_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for atas endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("atas", **params)


# URL builders for missing endpoints


def build_contratacoes_atualizacao_url(
    data_inicial: str, data_final: str, modalidade: int, pagina: int = 1, **kwargs
) -> str:
    """Build URL for contratações/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "codigoModalidadeContratacao": modalidade,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("contratacoes_atualizacao", **params)


def build_contratacoes_proposta_url(
    data_final: str, pagina: int = 1, modalidade: int = None, **kwargs
) -> str:
    """Build URL for contratações/proposta endpoint"""
    builder = URLBuilder()
    params = {
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    if modalidade:
        params["codigoModalidadeContratacao"] = modalidade
    return builder.build_url("contratacoes_proposta", **params)


def build_contratos_atualizacao_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for contratos/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("contratos_atualizacao", **params)


def build_atas_atualizacao_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for atas/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("atas_atualizacao", **params)


def build_instrumentos_cobranca_url(
    data_inicial: str, data_final: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for instrumentoscobranca/inclusao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicial": data_inicial,
        "dataFinal": data_final,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("instrumentoscobranca_inclusao", **params)


def build_pca_url(
    ano_pca: int, codigo_classificacao: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for pca endpoint"""
    builder = URLBuilder()
    params = {
        "anoPca": ano_pca,
        "codigoClassificacaoSuperior": codigo_classificacao,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("pca", **params)


def build_pca_usuario_url(
    ano_pca: int, id_usuario: int, pagina: int = 1, **kwargs
) -> str:
    """Build URL for pca/usuario endpoint"""
    builder = URLBuilder()
    params = {
        "anoPca": ano_pca,
        "idUsuario": id_usuario,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("pca_usuario", **params)


def build_pca_atualizacao_url(
    data_inicio: str, data_fim: str, pagina: int = 1, **kwargs
) -> str:
    """Build URL for pca/atualizacao endpoint"""
    builder = URLBuilder()
    params = {
        "dataInicio": data_inicio,
        "dataFim": data_fim,
        "pagina": pagina,
        **kwargs,
    }
    return builder.build_url("pca_atualizacao", **params)


def get_phase_2a_endpoints() -> List[str]:
    """Get list of Phase 2A priority endpoints"""
    return settings.PHASE_2A_ENDPOINTS


def get_all_pncp_endpoints() -> List[str]:
    """Get list of ALL PNCP endpoints for 100% coverage"""
    return settings.ALL_PNCP_ENDPOINTS
