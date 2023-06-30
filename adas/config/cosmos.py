"""This module contains properties and functions for accessing cosmos
 environment variables."""

from adas.config.config import get_env_variable

list_fields_id = ["vin_nbr"]


def scope() -> str:
    """Get COSMOS_SERVICE_CREDENTIAL_SCOPE environment variable value."""
    value = get_env_variable("COSMOS_SERVICE_CREDENTIAL_SCOPE")
    return value


def key() -> str:
    """Get COSMOS_SERVICE_CREDENTIAL_KEY environment variable value."""
    value = get_env_variable("COSMOS_SERVICE_CREDENTIAL_KEY")
    return value


def database_name() -> str:
    """Get COSMOS_DATABASE_NAME environment variable value."""
    value = get_env_variable("COSMOS_DATABASE_NAME")
    return value


def endpoint() -> str:
    """Get COSMOS_ENDPOINT environment variable value."""
    value = get_env_variable("COSMOS_ENDPOINT")
    return value


def subscription_id() -> str:
    """Get COSMOS_SUBSCRIPTION_ID environment variable value."""
    value = get_env_variable("COSMOS_SUBSCRIPTION_ID")
    return value


def tenant_id() -> str:
    """Get COSMOS_TENANT_ID environment variable value."""
    value = get_env_variable("COSMOS_TENANT_ID")
    return value


def resource_group_name() -> str:
    """Get COSMOS_RESOURCE_GROUP_NAME environment variable value."""
    value = get_env_variable("COSMOS_RESOURCE_GROUP_NAME")
    return value


def client_id() -> str:
    """Get COSMOS_CLIENT_ID environment variable value."""
    value = get_env_variable("COSMOS_CLIENT_ID")
    return value


def arizona_vin_table() -> str:
    """Get AZ_VIN_LIST_TABLE environment variable value."""
    value = get_env_variable("AZ_VIN_LIST_TABLE")
    return value


def table_path() -> str:
    """Get TABLE_PATH environment variable value."""
    value = get_env_variable("ADAS_VBS_SILVER_OUTPUT_PATH")
    return value


def cosmos_container() -> str:
    """Get COSMOS_CONTAINER environment variable value."""
    value = get_env_variable("COSMOS_CONTAINER")
    return value


def cosmos_checkpoint_path() -> str:
    """Get COSMOS_CHECKPOINT_PATH environment variable value."""
    value = get_env_variable("COSMOS_CHECKPOINT_PATH")
    return value
