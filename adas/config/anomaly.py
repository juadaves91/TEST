"""This module contains properties and functions for accessing anomaly
 environment variables."""


from adas.config.config import get_env_variable


def database() -> str:
    """Get ADAS_VBS_SILVER_ANOMALY_DATABASE environment variable value."""
    value = get_env_variable("ADAS_VBS_SILVER_ANOMALY_DATABASE")
    return value


def table() -> str:
    """Get ADAS_VBS_SILVER_ANOMALY_TABLE environment variable value."""
    value = get_env_variable("ADAS_VBS_SILVER_ANOMALY_TABLE")
    return value


def database_output_path() -> str:
    """Gets ADAS_VBS_SILVER_ANOMALY_DATABASE_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_SILVER_ANOMALY_DATABASE_PATH")
    return value


def output_path() -> str:
    """Gets ADAS_VBS_SILVER_ANOMALY_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_SILVER_ANOMALY_PATH")
    return value


def process_date() -> str:
    """Get ADAS_VBS_SILVER_ANOMALY_PROCESS_DATE environment variable. Default
    return value is None.
    """
    value = get_env_variable("ADAS_VBS_SILVER_ANOMALY_PROCESS_DATE")
    return value
