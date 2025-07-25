"""Custom Pydantic types for the application."""

from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core.core_schema import CoreSchema, str_schema

from .validators import validate_cnpj, validate_cpf


class Cnpj(str):
    """A Pydantic type for CNPJ strings."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return str_schema(
            min_length=14,
            max_length=18,
            pattern=r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$",
            serialization={"type": "string", "format": "cnpj"},
            validation_filter=validate_cnpj,
        )


class Cpf(str):
    """A Pydantic type for CPF strings."""

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        return str_schema(
            min_length=11,
            max_length=14,
            pattern=r"^\d{3}\.\d{3}\.\d{3}-\d{2}$",
            serialization={"type": "string", "format": "cpf"},
            validation_filter=validate_cpf,
        )
