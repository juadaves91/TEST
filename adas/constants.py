"""This module contains constants used by the data pipeline."""
# ISO_TIME_FORMAT = "yyyy-MM-dd HH:mm:ssXXX"
DATE_FORMAT = """^[0-9]{4}-[0-9]{2}-[0-9]{2}$"""
# Year format yyyy
YEAR_FORMAT = """^[0-9]{4}$"""
# Ex: 2022-01-01T00:00:00.0
RAW_TIMESTAMP_REGEX = (
    """^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]+$"""
)
# Ex: 2014-06-22T03:02:23.000-04:00
RAW_TIMESTAMP_REGEX_TZ = """^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}[+\\-][0-9]{2}:[0-9]{2}$"""

RAW_OFFSET_REGEX = """^[+-]((([0-9]|1[0-3]):([0-5][0-9]))|(14:00))$"""
