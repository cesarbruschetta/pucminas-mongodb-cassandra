import logging
from datetime import datetime
from decimal import Decimal
from pathlib import Path


logger = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parent


def get_int_value(data, key):
    """Extrai um valor inteiro, tratando None ou strings vazias."""
    value = data.get(key)
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def get_float_value(data, key):
    """Extrai um valor float, tratando None ou strings vazias."""
    value = data.get(key)
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_decimal_value(data, key):
    """Extrai um valor decimal, tratando formatos de string e MongoDB $numberDecimal."""
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, dict) and "$numberDecimal" in value:
        try:
            return Decimal(value["$numberDecimal"])
        except (ValueError, TypeError):
            return None
    try:
        return Decimal(str(value))  # Converte para string antes de Decimal
    except (ValueError, TypeError):
        return None


def get_timestamp_value(data, key):
    """Extrai um valor timestamp, tratando o formato MongoDB $date.$numberLong."""
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, dict) and "$date" in value and "$numberLong" in value["$date"]:
        try:
            # $numberLong é em milissegundos, datetime.fromtimestamp espera segundos
            return datetime.fromtimestamp(int(value["$date"]["$numberLong"]) / 1000)
        except (ValueError, TypeError):
            return None
    # Se já for um timestamp ou outro formato que datetime.fromisoformat possa lidar
    # ou se for um inteiro (segundos desde a epoch)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except (ValueError, TypeError):
            return None
    return None  # Retorna None se o formato não for reconhecido ou inválido


def get_boolean_value(data, key):
    """Extrai um valor booleano, tratando strings e outros tipos."""
    value = data.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)
