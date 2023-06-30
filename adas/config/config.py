"""A module for accessing environment variables & config settings."""

import re
from datetime import date
from datetime import datetime as dt
from os import getenv
from typing import Any, List, Optional, Union

import IPython
from pyspark.sql import SparkSession


def get_env_variable(var_name: str, default: Optional[str] = None) -> str:
    """Get value of the specified environment variable.

    Parameters:
    -----------
    var_name: str
        The name of the environment variable.
    default: str (optional)
        The default value if the environment variable is not found.

    Returns:
    --------
    env_var: str
        The value of the environment variable.

    Raises:
    -------
    RuntimeError
        If the environment variable is not set and a default value
        is not provided.
    """
    env_var = getenv(var_name)
    if env_var is None and default is None:
        message = f"The environment variable, '{var_name}', must be set."
        raise RuntimeError(message)
    if env_var is None:
        env_var = default

    return env_var


def validate_date(input_date: str) -> bool:
    """Returns True if a string is a date of the form YYYY-MM-DD."""

    valid_format = False
    valid_year = False
    valid_month = False
    valid_day = False

    valid_format = bool(re.fullmatch(r"\d{4}\-\d{2}\-\d{2}", input_date))
    days_in_month = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    if valid_format:
        list_date = input_date.split("-")
        year = int(list_date[0])
        month = int(list_date[1])
        day = int(list_date[2])

        valid_year = year >= 0
        if valid_year:
            if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
                days_in_month[2] = 29
            valid_month = 1 <= month <= 12
            if valid_month:
                valid_day = 1 <= day <= days_in_month[month]

    is_date = valid_format and valid_year and valid_month and valid_day

    return is_date


def valid_p_date(p_date: str) -> bool:
    """Checks if a p_date (partition date) is valid. It must be in
    "YYYY-MM-DD" format and less than or equal to the current date.

    Parameters:
    -----------
    p_date: str (optional)
        String used for a date partition.

    Returns:
    --------
    valid_date: bool
        Indicates if the p_date is a valid format and date.
    """

    valid_date = False
    valid_format = validate_date(p_date)
    if valid_format:
        today = date.today()
        future_date = today < dt.strptime(p_date, "%Y-%m-%d").date()
        valid_date = valid_format and not future_date
    return valid_date


def validate_hour(hour: int) -> bool:
    """Returns True if the int is an hour of the day from 0 to 23."""
    hours = tuple(range(0, 24))
    valid_hour = hour in hours
    return valid_hour


def validate_hour_list(hours: List[int]) -> bool:
    """Returns True if hours is a list of ints from 0 to 23."""
    valid_hour_list = True
    if isinstance(hours, list) and len(hours) > 0:
        for hour in set(hours):
            valid_hour = validate_hour(hour)
            if not valid_hour:
                valid_hour_list = False
                break
    else:
        valid_hour_list = False
    return valid_hour_list


def valid_p_hours(p_hours: Union[int, str, None, list]) -> bool:
    """Check the p_hours input for the main function to determine if it
    is a valid value.

    Parameters:
    -----------
    p_hours: int or List(int) or None
        An integer or list of integers from 0 to 23.

    Returns:
    --------
    bool
        Returns True if p_hours is None, an integer, or a list of
        integers with values 0 through 23.
    """
    if p_hours is not None:
        if isinstance(p_hours, int):
            valid_hours = validate_hour(p_hours)
        elif isinstance(p_hours, str):
            valid_hours = validate_hour(int(p_hours))
        elif isinstance(p_hours, list):
            valid_hours = validate_hour_list(p_hours)
        else:
            valid_hours = False
    else:
        valid_hours = True
    return valid_hours


def spark_log_level() -> str:
    """Return the string value for SPARK_LOG_LEVEL or "ERROR"."""
    value = get_env_variable("SPARK_LOG_LEVEL", "ERROR")
    return value


def get_dbutils(spark: SparkSession) -> Any:
    """Return the dbutils object for interacting with filesystem.

    Parameters
    ----------
    spark: SparkSession
        The current spark session for the job.

    Returns
    -------
    DBUtils
        The dbutils object for interacting with filesystem."""

    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)

    return IPython.get_ipython().user_ns["dbutils"]
