"""
This module contains the silver python file used to transform ADAS data from bronze to silver
"""
import os
import warnings
from datetime import datetime, timezone
from typing import Optional

import pyspark.sql.functions as F
from delta import tables as DT
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from adas import constants, schemas, utils
from adas.config import anomaly, bronze, config, processingmodes, silver

# Columns used to upsert in the merge operation when writes in silver layer
_silver_id_cols = ["vin_nbr"]
_deduplicate_cols = ["vin"]


def define_schema(input_df: DataFrame) -> DataFrame:
    """
    Obtain json data and load the schema for the raw data
    return a dataframe with the raw data and the columns from the nested field
    """

    json_conv = F.from_json(F.col("data"), schemas.data_raw)
    transform_df = input_df.select("*", json_conv.alias("data_schema"))
    select_df = transform_df.select("data", "data_schema.*")

    return select_df


def deduplicate_batch(input_df: DataFrame) -> DataFrame:
    """
    Deduplicate using a max aggregation to make sure nulls are
    ignored (i.e. keys match, but one of the other columns has a
    null)

    Parameters:
    df

    Returns:
    df: df

    """
    return input_df.groupBy(*_deduplicate_cols).agg(
        *[
            F.max(x).alias(x)
            for x in input_df.columns
            if x not in _deduplicate_cols
        ]
    )


def flag_null(input_df: DataFrame, narrow_anomalies_var: dict) -> DataFrame:
    """
    Adds an anomaly flag to identify if some of the columns is null

    If the field value is missing, a flag is
    added.
    Ex: bad_marker_date

    Parameters:
    df: Event level dataframe
    narrow_anomalies_var (nested dictionary): nested dictionary
    containing information about each column.

    Returns:
    df: dataframe with anomaly flags added for fields in
    narrow_anomalies
    """

    # Returns true if column value is null
    def _check_null(can_be_null: bool, key_var: str) -> bool:
        if not can_be_null:
            return F.col(key_var).isNull()
        return F.lit(False)

    for key in narrow_anomalies_var.keys():
        # Gets information from nested dictionary
        can_be_null = narrow_anomalies_var[key]["can_be_null"]
        anomaly_column = narrow_anomalies_var[key]["anomaly_column"]
        cond = _check_null(can_be_null, key)
        anomaly_flag = cond

        input_df = input_df.withColumn(anomaly_column, anomaly_flag)

    return input_df


def flag_bad_date_time(input_df: DataFrame) -> DataFrame:
    """
    Adds an anomaly flag to identify if some of the date time
    fields have not a valid value

    If the field value or field timestamp is bad, a flag is
    added.
    Ex: bad_markerdate

    Parameters:
    df: Dataframe

    Returns:
    Dataframe with anomaly flag columns for each date field
    """
    not_event_year_format = ~F.col("year").rlike(constants.YEAR_FORMAT)
    bad_year = F.coalesce(not_event_year_format, F.lit(True))
    not_event_date_format = ~F.col("productionDate").rlike(
        constants.DATE_FORMAT
    )
    bad_date = F.coalesce(not_event_date_format, F.lit(True))
    transform_df = input_df.withColumn("bad_year", bad_year)
    transform_df = transform_df.withColumn("bad_production_date", bad_date)

    return transform_df


def flag_allowed_values(
    input_df: DataFrame, narrow_anomalies_var: dict
) -> DataFrame:
    """
    Verifies that the values of the column names contained in
    narrow_anomalies meet specific parameters. These parameters
    include  the column's associated event
    types, the values it can take on if the column's data type is a
    string, and the name a flag column that is marked true the
    column takes on a value outside of the given criteria.

    Normalizes values contained in string columns by trimming and
    lowercasing their contents.

    Parameters:
    df:  dataframe
    narrow_anomalies_var (nested dictionary): nested dictionary
    containing information about each column.

    Returns:
    df: dataframe with anomaly flags added for fields in
    narrow_anomalies
    """

    # Returns true if field value is not contained in a list of
    # values the field can take on.
    # ex: ign_cyl can be on or off. If a value in the ign_cyl column
    # is something outside of this, it is flagged.
    def _check_if_allowed_value(
        associated_values_list: list, key_var: str
    ) -> bool:
        if associated_values_list is not None:
            not_null = F.col(key_var).isNotNull()
            not_in_values = ~F.col(key_var).isin(associated_values_list)
            cond = not_null & not_in_values
            return cond
        return F.lit(False)

    for key in narrow_anomalies_var.keys():
        # Gets information from nested dictionary
        anomaly_column = narrow_anomalies_var[key]["anomaly_column"]
        associated_values = narrow_anomalies_var[key]["associated_values"]

        # normalizes values in field column
        if associated_values is not None:
            input_df = input_df.withColumn(key, F.lower(F.trim(F.col(key))))
            cond = _check_if_allowed_value(associated_values, key)
            anomaly_flag = cond

            input_df = input_df.withColumn(anomaly_column, anomaly_flag)

    return input_df


def add_is_anomaly_flag(input_df: DataFrame, anomaly_flags: list) -> DataFrame:
    """
    Adds a boolean column to indicate if any of the narrow anomaly
    flags are True.

    Parameters:
    anomaly_flags: list of anomaly flag columns to check

    Returns:
    Dataframe with is_anomaly column
    """

    is_anomaly = F.lit(False)

    for i in anomaly_flags:
        is_anomaly = is_anomaly | F.col(i)

    transform_df = input_df.withColumn("is_anomaly", is_anomaly)

    return transform_df


def add_metadata_columns(input_df: DataFrame) -> DataFrame:
    """
    Adds metadata columns -- partition, processed time, and
    processing mode
    """
    processing_mode = silver.processing_mode()

    transform_df = (
        input_df.withColumn(
            "silver_p_date",
            F.substring(F.current_timestamp().cast("string"), 1, 10),
        )
        .withColumn(
            "silver_processed_time", F.current_timestamp().cast("string")
        )
        .withColumn("processing_mode", F.lit(processing_mode))
    )

    return transform_df


def split_anomalies(input_df: DataFrame) -> DataFrame:
    """
    Splits clean and anomalous records into two separate dataframes

    Parameters:
    df: cleaned and validated dataframe

    Returns:
    clean_df: dataframe containing records with no flagged anomalies
    anomaly_df: dataframe containing records with one or more
    flagged anomalies
    """
    is_anomaly_filter = F.col("is_anomaly")

    anomaly_df = input_df.filter(is_anomaly_filter).drop("is_anomaly")

    not_anomaly = ~F.col("is_anomaly")
    clean_df = input_df.filter(not_anomaly).drop("is_anomaly")

    return (clean_df, anomaly_df)


def add_clean_partition_columns(input_df: DataFrame) -> DataFrame:
    """
    Adds silver partitions columns.
    Partitioned by markerdate date and hour
    """

    silver_df = input_df.withColumn(
        schemas.DATE_PARTITION_COLUMN, F.substring("productionDate", 1, 4)
    )

    return silver_df


def rename_columns(input_df: DataFrame, columns_rename_var: dict) -> DataFrame:
    """
    Renames the columns containing sensor readings
    """

    for key in columns_rename_var.keys():
        input_df = input_df.withColumnRenamed(key, columns_rename_var[key])

    return input_df


def transform_clean_records(input_df: DataFrame) -> DataFrame:
    """
    After the anomalous records have been identified and split out,
    this function does the remaining transformations for the clean
    records
    adding partition columns and rename the columns
    """

    # Converts silver_schema (which is a StructType) to a list
    columns = []
    for field in schemas.silver_schema:
        name = field.name
        dtype = field.dataType
        columns.append(F.col(name).cast(dtype))

    transform_df = input_df.transform(add_clean_partition_columns).transform(
        rename_columns, schemas.columns_rename
    )
    filter_df = transform_df.select(columns)

    return filter_df


def rename_anomaly_columns(input_df: DataFrame) -> DataFrame:
    """
    Partition anomaly table by bronze date partition (aka bronze
    process date)
    """

    transform_df = input_df.withColumnRenamed("vin", "vin_nbr")

    return transform_df


def add_anomaly_partition_columns(input_df: DataFrame) -> DataFrame:
    """
    Partition anomaly table by bronze date partition (aka bronze
    process date)
    """

    transform_df = input_df.withColumn(
        schemas.ANOMALY_PARTITION_COLUMN, F.col("silver_p_date")
    )
    return transform_df


def transform_anomaly_records(input_df: DataFrame) -> DataFrame:
    """
    After the clean records have been split out, this function does
    the remaining transformations for the anomaly records.
    Ex: adding partition column and defining schema
    """
    transform_df = input_df.transform(add_anomaly_partition_columns).transform(
        rename_anomaly_columns
    )

    columns = []
    for field in schemas.anomaly_schema():
        columns.append(F.col(field.name).cast(field.dataType))

    filter_df = transform_df.select(columns)

    return filter_df


def process_batch_main(bronze_df_var: DataFrame) -> tuple:
    """Process a batch of Bronze data into clean silver
    and anomaly dataframes .

    Parameters:
    -----------
    spark: SparkSession
        The spark session for the current pipeline.
    bronze_df: DataFrame
        A PySpark dataframe containing a batch of bronze data.

    Return:
    -----------
    The silver df will be writed in silver layer
    The rows with anomalies
    """
    # define the schema and add time column
    processed_df = bronze_df_var.transform(define_schema)

    # Deduplicate using a list of keys
    vehicle_df = deduplicate_batch(processed_df)

    # Add anomaly flag and metadata columns
    validated_df = (
        vehicle_df.transform(flag_null, schemas.narrow_anomalies)
        .transform(flag_bad_date_time)
        .transform(flag_allowed_values, schemas.narrow_anomalies)
        .transform(add_is_anomaly_flag, schemas.narrow_anomaly_flags)
        .transform(add_metadata_columns)
    )

    # Split the clean vs anomaly records
    (silver_df, anomaly_df) = split_anomalies(validated_df)

    # Final post-processing of each dataframe, including column/field
    # renaming and unit conversions
    silver_df = transform_clean_records(silver_df)
    anomaly_df = transform_anomaly_records(anomaly_df)

    return (silver_df, anomaly_df)


def get_dedupe_key_match_condition(
    table1_alias: str, table2_alias: str, columns: list
) -> str:
    """
    Given two table names and a list of columns, returns the SQL
    expression representing the matching of each column between the two
    tables

    Ex:
    table1Alias: "tableA"
    table2Alias: "tableB"
    columns: List("col1", "col2", "col3")

    Returns:
    "tableA.col1 = tableB.col1
    AND tableA.col2 = tableB.col2
    AND tableA.col3 = tableB.col3"

    Parameters:
    table1_alias (str): name of first table
    table2_alias (str): name of second table
    columns: list of columns to match on

    Returns:
    str: SQL expression
    """

    if not columns:
        raise ValueError("List of key columns for deduplication is empty.")

    # Ex: table1.colA = table2.colA
    def _get_match_expression(col_name: str) -> str:
        return f"{table1_alias}.{col_name} = {table2_alias}.{col_name}"

    # Loop through each column and concatenate the match expression
    # for each using an AND condition
    cond = _get_match_expression(columns[0])

    for i in range(1, len(columns)):
        cond += f" AND {_get_match_expression(columns[i])}"

    return cond


def write_clean_with_merge(
    batch_df: DataFrame,
    spark: SparkSession,
) -> None:
    """
    Insert records using MERGE to only insert if there's no match on
    the key. Applies partition pruning on deduplication step.

    Parameters:
    -----------
    batch_df: Dataframe
        Dataframe in micro-batch to write
    current_date: str
        batch_start_date from config if batch or current_date() if stream
    process_mode: str
        batch or stream
    dedupe_cols: list
        Columns used for deduplication
    """

    silver_table = DT.DeltaTable.forPath(spark, silver.output_path())

    silver_alias = "silver"
    batch_alias = "batch"

    # Get condition to match on key
    # Ex: "silver.vin_nbr = batch.vin_nbr AND ..."
    key_match_condition = get_dedupe_key_match_condition(
        silver_alias, batch_alias, _silver_id_cols
    )

    silver_table.alias(silver_alias).merge(
        source=batch_df.alias(batch_alias),
        condition=f"{key_match_condition}",
    ).whenNotMatchedInsertAll().execute()


def write_anomalies(anomaly_df: DataFrame, mode: str = "append") -> None:
    """
    Adds anomaly records into the anomaly delta table
    Partitioned by the bronze process date.

    Parameters:
    -----------
    df: DataFrame
        The dataframe of records to add to the Anomaly table.
    mode: str (optional)
        The mode in which records are written to the Anomaly table (e.g.
        "append", "overwrite", etc.), which is "append" by default.
    """

    # When overwriting data set the writer to only overwrite individual
    # partitions.
    partition_overwrite_mode = "static"
    if mode == "overwrite":
        partition_overwrite_mode = "dynamic"

    (
        anomaly_df.write.format("delta")
        .mode(mode)
        .option("partitionOverwriteMode", partition_overwrite_mode)
        .partitionBy(schemas.ANOMALY_PARTITION_COLUMN)
        .save(anomaly.output_path())
    )


def process_and_write_batch(spark: SparkSession, bronze_df: DataFrame) -> None:
    """Process a batch of Bronze data into clean silver, clean late,
    and anomaly dataframes and write them to their respective delta
    tables.

    Parameters:
    -----------
    spark: SparkSession
        The spark session for the current pipeline.
    bronze_df: DataFrame
        A PySpark dataframe containing a batch of bronze data.
    write_mode: str
        Determines how the processed data will be written to the Silver
        table (e.g. appended or merged).
    """

    (silver_df, anomaly_df) = process_batch_main(bronze_df)

    write_clean_with_merge(silver_df, spark)

    write_anomalies(anomaly_df)


def read_from_bronze_batch(
    spark: SparkSession,
    bronze_processed_date: str,
    bronze_processed_hour: Optional[str],
) -> DataFrame:
    """Reads a batch of data from the bronze delta table.

    Parameters:
    -----------
    spark: SparkSession
        The spark session for the current pipeline.

    Returns:
    --------
    df: DataFrame
        A dataframe containing a batch of bronze records.
    """

    dbutils = config.get_dbutils(spark)

    bronze_path = bronze.input_path()
    dt_path = os.path.join(bronze_path, bronze_processed_date)
    dir_list = dbutils.fs.ls(bronze_path)
    date_dirs = [item.path for item in dir_list if item.isDir()]
    valid_dt_path = dt_path + "/" in date_dirs
    if not valid_dt_path:
        message = f"Partition for {bronze_processed_date} not found. Check values of "
        message += "ADAS_VBS_BRONZE_PATH and ADAS_VBS_BRONZE_PROCESS_DATE "
        message += f"environment variables. Invalid path: {dt_path}"
        raise IOError(message)

    if bronze_processed_hour is None:
        paths = [os.path.join(dt_path, "*")]
    else:
        if isinstance(bronze_processed_hour, str):
            paths = [os.path.join(dt_path, bronze_processed_hour.zfill(2))]
        else:
            paths = [
                os.path.join(dt_path, str(hr).zfill(2))
                for hr in bronze_processed_hour
            ]
            paths = set(paths)

        hr_dir_list = dbutils.fs.ls(dt_path)
        hr_dirs = [item.path for item in hr_dir_list if item.isDir()]
        partitions_not_found = []
        for path in sorted(paths):
            if path + "/" not in hr_dirs:
                paths.remove(path)
                date = path.split("/")[-2]
                hour = path.split("/")[-1]
                partitions_not_found.append(f"{date}/{str(hour).zfill(2)}")

        message = "No partitions found."

        if not paths:
            message += ". None of the requested partitions could be "
            message += "loaded. Please check input p_hours value(s) "
            message += "and restart job."
            raise IOError(message)

        if partitions_not_found:
            message += ". Other requested partitions were loaded."
            warnings.warn(message, stacklevel=2)

    df_bronze = spark.read.schema(schemas.bronze_schema).parquet(*paths)
    return df_bronze


def process_and_write_stream(
    spark: SparkSession, df_bronze: DataFrame
) -> StreamingQuery:
    """
    Main write function to write to silver, anomaly, and late tables
    using a foreachBatch function

    Parameters:
    spark (SparkSession): spark session
    df (DataFrame):

    Returns:
    int: Description of return value
    """

    def process_and_write_streaming(
        batch_df: DataFrame, batch_id: int  # noqa
    ) -> None:
        """Handler function called in foreachBatch of write step."""

        # If the checkpoint directory was deleted (for instance, if we had
        # to update the number of shuffle partitions), we need to iterate
        # over the already persisted data, without reprocessing it, so the
        # checkpoint file can catch up. Here we'll just do an inexpensive
        # 'take' operation - so the progress can be tracked in the UI, but
        # we wont actually process the files
        if silver.disable_write_and_update_checkpoint_only():
            batch_df.take(1)
            return

        process_and_write_batch(spark, batch_df)

    # checkpoint_path = config.silver.checkpoint_path()
    # checkpoint_path = os.path.join(checkpoint_path, "streaming_checkpoint")
    writer = (
        df_bronze.writeStream.outputMode("update")
        .format("delta")
        .foreachBatch(process_and_write_streaming)
        .trigger(availableNow=True)
        .option("checkpointLocation", silver.checkpoint_path())
        .start()
    )

    return writer


def read_from_bronze_stream(spark: SparkSession) -> DataFrame:
    """Reads a stream of data from the bronze delta table.
    Parameters:
    -----------
    spark: SparkSession
        The spark session for the current pipeline.

    Returns:
    --------
    df: DataFrame
    A streaming dataframe containing of bronze records.
    """
    df_bronze = (
        spark.readStream.format("cloudFiles")
        .option("cloudfiles.format", "parquet")
        .schema(schemas.bronze_schema)
        .option("cloudFiles.useIncrementalListing", "true")
        .option("cloudfiles.maxFilesPerTrigger", 1000)
        .option("cloudfiles.maxFileAge", "1 year")
        .load(bronze.path())
    )
    return df_bronze


def setup_database_and_tables(spark: SparkSession) -> None:
    """Setup the Silver layer database(s) and the Silver and
    Anomaly tables.

    Parameters:
    -----------
    spark: SparkSession
        The current spark session for the pipeline.
    """

    # Build the catalog if it doesn't exist.
    catalog = silver.catalog()
    database = silver.database()
    database_path = silver.database_output_path()
    path = silver.output_path()
    create_time = datetime.now(timezone.utc)

    # Build the silver database/schema if it doesn't exist.
    db_comment = "ADAS Schema automatically created by the Silver "
    db_comment += f"Pipeline at {create_time}."
    schema_silver_properties = dict()
    utils.build_hive_database(
        spark,
        catalog,
        database,
        database_path,
        db_comment,
        schema_silver_properties,
    )

    # Build the silver table if it doesn't exist
    table = silver.table()
    schema = schemas.silver_schema
    partitions = [schemas.DATE_PARTITION_COLUMN]
    comment = "ADAS Silver table automatically created by the "
    comment += f"Silver Pipeline at {create_time}."
    table_properties = dict(
        [
            ("delta.autoOptimize.optimizeWrite", "true"),
            ("delta.autoOptimize.autoCompact", "true"),
        ]
    )
    utils.build_hive_table(
        spark,
        catalog,
        database,
        table,
        path,
        schema,
        partitions,
        "delta",
        comment,
        table_properties,
    )

    # Build the anomaly database if it doesn't exist.
    database = anomaly.database()
    database_path = anomaly.database_output_path()
    path = anomaly.output_path()
    schema_anomaly_properties = dict()
    utils.build_hive_database(
        spark,
        catalog,
        database,
        database_path,
        db_comment,
        schema_anomaly_properties,
    )

    # Build the anomaly table if it doesn't exist
    table = anomaly.table()
    schema = schemas.anomaly_schema()
    partitions = [schemas.ANOMALY_PARTITION_COLUMN]
    comment = "ADAS anomaly table automatically created by the "
    comment += f"Silver Pipeline at {create_time}."
    utils.build_hive_table(
        spark,
        catalog,
        database,
        table,
        path,
        schema,
        partitions,
        "delta",
        comment,
        table_properties,
    )


def main(
    process_mode: str = processingmodes.BATCH,
    p_date: Optional[str] = None,
    p_hours: Optional[str] = None,
) -> None:
    """
    Main function of the program
    Create the Spark session
    Read the data from bronze table
    Process and write the curate data in silver table and
    the anomalies of the process
    Stop de spark session
    """
    spark = SparkSession.builder.appName("ADAS-VBS-silver").getOrCreate()

    setup_database_and_tables(spark)

    if process_mode == processingmodes.BATCH:
        p_date = bronze.process_date() if p_date is None else p_date
        if not config.valid_p_date(p_date):
            message = f"Invalid p_date value: {p_date}."
            raise ValueError(message)
        if not config.valid_p_hours(p_hours):
            message = f"Invalid p_hours value: {p_hours}."
            raise ValueError(message)
        bronze_df = read_from_bronze_batch(spark, p_date, p_hours)
        process_and_write_batch(spark, bronze_df)

    elif process_mode == processingmodes.STREAM:
        if (p_date is not None) or (p_hours is not None):
            message = "Arguments for p_date and p_hours are not used in "
            message += "streaming mode."
            warnings.warn(message, stacklevel=2)
        bronze_df = read_from_bronze_stream(spark)
        process_and_write_stream(spark, bronze_df).awaitTermination()

    else:
        message = f"Invalid process mode argument: {process_mode}"
        raise ValueError(message)
