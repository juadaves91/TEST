"""
This modules is use for configurated spark session and client for azure ADLS and Cosmos DB
"""
import os.path
from typing import Any, Dict, List, Optional, Union

import IPython
from pyspark.sql import SparkSession
from pyspark.sql import types as T


def get_dbutils(spark: SparkSession) -> Any:
    """
    get db utils from databricks
    """
    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)

    return IPython.get_ipython().user_ns["dbutils"]


def pyspark_to_sql_type(pyspark_type: T.DataType) -> Union[str, None]:
    """Translates PySpark DataType classes to their equivalent HIVE SQL
    expression datatype for building SQL query strings."""
    simple_types = [
        (T.BinaryType(), "BINARY"),
        (T.BooleanType(), "BOOLEAN"),
        (T.ByteType(), "TINYINT"),
        (T.DateType(), "DATE"),
        *[
            (T.DecimalType(precision, scale), f"DECIMAL({precision},{scale})")
            for precision in range(1, 39)
            for scale in range(0, 39)
            if scale <= precision
        ],
        (T.DoubleType(), "DOUBLE"),
        (T.FloatType(), "FLOAT"),
        (T.IntegerType(), "INT"),
        (T.LongType(), "BIGINT"),
        (T.ShortType(), "SMALLINT"),
        (T.StringType(), "STRING"),
        (T.TimestampType(), "TIMESTAMP"),
    ]
    array_types = [
        (T.ArrayType(item[0]), f"ARRAY<{item[1]}>") for item in simple_types
    ]
    translator = dict(simple_types + array_types)
    return translator[pyspark_type]


def build_hive_database(
    spark: SparkSession,
    catalog: str,
    database: str,
    path: str,
    comment: Optional[str],
    db_properties: Optional[Dict[str, str]],
) -> str:
    """Builds the HIVE database layer if it does not exist.

    Parameters:
    -----------
    spark: SparkSession
        The current spark session.
    database: str
        The name of the database.
    path: str
        The root directory path for the database.

    Returns:
    --------
    query: str
        The database creation HIVE SQL query string that was used to
        generate the database.
    """
    if comment is None:
        comment = ""

    if db_properties is None:
        db_properties = dict()

    query_catalog = f"USE CATALOG {catalog}"
    spark.sql(query_catalog)

    query = f"CREATE SCHEMA IF NOT EXISTS {database} "

    if comment:
        query += f"COMMENT '{comment}' "

    query += f"MANAGED LOCATION '{path}'"

    if db_properties:
        query += " WITH DBPROPERTIES ("
        for property, value in db_properties.items():
            query += f"{property}={value}, "
        query = query[:-2] + ")"

    spark.sql(query)
    return query


def build_hive_table(
    spark: SparkSession,
    catalog: str,
    database: str,
    table: str,
    path: str,
    schema: T.StructType,
    partitions: Optional[List[str]],
    data_source: str,
    comment: Optional[str],
    table_properties: Dict[str, str],
    external: bool = True,
) -> str:
    """Builds the HIVE table layer for silver if it does not exist.

    Parameters:
    -----------
    spark: SparkSession
        The current spark session.
    database: str
        The name of the database.
    table: str
        The name of the table.
    path: str
        The root directory path for the database.
    schema: T.StructType
        The schema defined for the table consititing of a PySpark
        StructType of StructFields. Note that if a StructField
        contains a metadata dictionary with a key named 'comment', then
        the associated value will be added to the HIVE table metadata.
    partitions: Optional[List[str]] = list()
        The table partitioning columns.
    data_source: Optional[str] = 'DELTA'
        The data source type for the table: TEXT, AVRO, BINARYFILE, CSV,
        JSON, PARQUET, ORC, DELTA, JDBC, or LIBSVM.
    table_properties: Dict[str,str] = dict()
        HIVE table properties.
    external: bool = True
        A boolean flag indicating if the table should be external.

    Returns:
    --------
    query: str
        The table creation HIVE SQL query string that was used to
        generate the table.

    Exceptions:
    -----------
    ValueError
        Occurs when an invalid data_source value is supplied.
    """
    if external:
        query = f"CREATE EXTERNAL TABLE IF NOT EXISTS {catalog}.{database}.{table} ("
    else:
        query = f"CREATE TABLE IF NOT EXISTS {catalog}.{database}.{table} ("

    for field in schema:
        query += f"{field.name} {pyspark_to_sql_type(field.dataType)}"
        if field.metadata and "comment" in field.metadata.keys():
            query += f" COMMENT '{field.metadata['comment']}', "
        else:
            query += ", "

    allowed_sources = [
        "TEXT",
        "AVRO",
        "BINARYFILE",
        "CSV",
        "JSON",
        "PARQUET",
        "ORC",
        "DELTA",
        "JDBC",
        "LIBSVM",
    ]
    if data_source.upper() in allowed_sources:
        query = query[:-2] + f") USING {data_source.upper()} "
    else:
        message = f"An invalid data source format, {data_source}, was "
        message += "selected. Allowed values are: "
        message += ", ".join(allowed_sources) + "."
        raise ValueError(message)

    if partitions:
        query += "PARTITIONED BY ("
        for partition in partitions:
            query += f"{partition}, "
        query = query[:-2] + ") "

    query += f"LOCATION '{path}'"

    if comment:
        query += f" COMMENT '{comment}'"

    if table_properties:
        query += " TBLPROPERTIES ("
        for property, value in table_properties.items():
            query += f"{property}={value}, "
        query = query[:-2] + ")"

    spark.sql(query)
    return query
