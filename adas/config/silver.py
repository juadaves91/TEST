"""This module contains properties and functions for accessing Silver
table environment variables."""

from os import environ

from adas import config
from adas.config.config import get_env_variable


def catalog() -> str:
    """Get ADAS_VBS_SILVER_CATALOG environment variable value."""
    value = get_env_variable("ADAS_VBS_SILVER_CATALOG")
    return value


def database() -> str:
    """Get ADAS_VBS_SILVER_DATABASE environment variable value."""
    value = get_env_variable("ADAS_VBS_SILVER_DATABASE")
    return value


def table() -> str:
    """Get ADAS_VBS_SILVER_TABLE environment variable value."""
    value = get_env_variable("ADAS_VBS_SILVER_TABLE")
    return value


def database_output_path() -> str:
    """Gets ADAS_VBS_SILVER_DATABASE_OUTPUT_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_SILVER_DATABASE_OUTPUT_PATH")
    return value


def output_path() -> str:
    """Gets ADAS_VBS_SILVER_OUTPUT_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_SILVER_OUTPUT_PATH")
    return value


def checkpoint_path() -> str:
    """Gets ADAS_VBS_SILVER_CHECKPOINT_PATH environment variable."""
    value = get_env_variable("ADAS_VBS_SILVER_CHECKPOINT_PATH")
    return value


def processing_mode() -> str:
    """Gets ADAS_VBS_SILVER_PROCESSING_MODE environment variable
    value or defaults to batch."""
    env_var = "ADAS_VBS_SILVER_PROCESSING_MODE"
    default = config.processingmodes.BATCH
    value = get_env_variable(env_var, default)
    return value


def delta_write_mode() -> str:
    """Gets ADAS_VBS_SILVER_DELTA_WRITE_MODE environment variable
    value or defaults to merge."""
    env_var = "ADAS_VBS_SILVER_DELTA_WRITE_MODE"
    default = config.writemodes.MERGE
    value = get_env_variable(env_var, default)
    return value


def checkpoint_to_adls() -> bool:
    """Checks if the data should be checkpointed to DBFS."""
    value = "ADAS_VBS_SILVER_CHECKPOINT_TO_ADLS" in environ
    return value


def disable_write_and_update_checkpoint_only() -> bool:
    """Checks if the data should be checkpointed to DBFS."""
    env_var = "ADAS_VBS_SILVER_DISABLE_WRITE_AND_UPDATE_CHECKPOINT_ONLY"
    value = env_var in environ
    return value
