"""This module contains properties and functions for accessing Bronze
table environment variables."""


from adas.config.config import get_env_variable


def database() -> str:
    """Get ADAS_VBS_BRONZE_DATABASE environment variable value."""
    value = get_env_variable("ADAS_VBS_BRONZE_DATABASE")
    return value


def table() -> str:
    """Get ADAS_VBS_BRONZE_TABLE environment variable value."""
    value = get_env_variable("ADAS_VBS_BRONZE_TABLE")
    return value


def process_date() -> str:
    """Get ADAS_VBS_BRONZE_PROCESS_DATE environment variable value."""
    value = get_env_variable("ADAS_VBS_BRONZE_PROCESS_DATE")
    return value


def input_path() -> str:
    """Gets ADAS_VBS_BRONZE_INPUT_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_BRONZE_INPUT_PATH")
    return value


def path() -> str:
    """Gets ADAS_VBS_BRONZE_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_BRONZE_PATH")
    return value
