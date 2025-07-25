"""Custom validators for Pydantic models."""

import re


def validate_cnpj(v: str) -> str:
    """Validate and format a CNPJ string."""
    if not isinstance(v, str):
        raise TypeError("CNPJ must be a string")

    digits_only = re.sub(r"[^\d]", "", v)

    if len(digits_only) != 14:
        raise ValueError("CNPJ must have 14 digits")

    if not _validate_cnpj_digits(digits_only):
        raise ValueError("Invalid CNPJ")

    return digits_only


def _validate_cnpj_digits(cnpj: str) -> bool:
    """Validate the check digits of a CNPJ."""
    if len(cnpj) != 14 or len(set(cnpj)) == 1:
        return False

    weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum1 = sum(int(c) * w for c, w in zip(cnpj[:12], weights1))
    verifying_digit1 = (sum1 * 10) % 11
    if verifying_digit1 == 10:
        verifying_digit1 = 0
    if verifying_digit1 != int(cnpj[12]):
        return False

    weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    sum2 = sum(int(c) * w for c, w in zip(cnpj[:13], weights2))
    verifying_digit2 = (sum2 * 10) % 11
    if verifying_digit2 == 10:
        verifying_digit2 = 0
    return verifying_digit2 == int(cnpj[13])


def validate_cpf(v: str) -> str:
    """Validate and format a CPF string."""
    if not isinstance(v, str):
        raise TypeError("CPF must be a string")

    digits_only = re.sub(r"[^\d]", "", v)

    if len(digits_only) != 11:
        raise ValueError("CPF must have 11 digits")

    if not _validate_cpf_digits(digits_only):
        raise ValueError("Invalid CPF")

    return digits_only


def _validate_cpf_digits(cpf: str) -> bool:
    """Validate the check digits of a CPF."""
    if len(cpf) != 11 or len(set(cpf)) == 1:
        return False

    sum1 = sum(int(c) * (10 - i) for i, c in enumerate(cpf[:9]))
    verifying_digit1 = (sum1 * 10) % 11
    if verifying_digit1 == 10:
        verifying_digit1 = 0
    if verifying_digit1 != int(cpf[9]):
        return False

    sum2 = sum(int(c) * (11 - i) for i, c in enumerate(cpf[:10]))
    verifying_digit2 = (sum2 * 10) % 11
    if verifying_digit2 == 10:
        verifying_digit2 = 0
    return verifying_digit2 == int(cpf[10])
