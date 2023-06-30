"""
This module contains the logic used for reading data from ADAS Silver layer,
Task220 Gold layer and write on cosmos DB container
"""
import hashlib

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from adas.config import config, cosmos


def read_data(spark: SparkSession, table_path: str) -> DataFrame:
    """
    Receive the path and read the data from delta table
    """
    df_delta_table = spark.readStream.format("delta").load(table_path)
    return df_delta_table


def read_arizona_data(spark: SparkSession, table_name: str) -> DataFrame:
    """
    Receive the table_name and read the data from delta table p
    """
    df_arizona_data = spark.read.table(table_name)
    return df_arizona_data


def join_data(
    df_delta_table: DataFrame, df_arizona_data: DataFrame
) -> DataFrame:
    """
    Receive dataframe for task220/adas and arizona vin
    make a join between the two df
    """
    df_az_data = df_delta_table.join(
        F.broadcast(df_arizona_data), "vin_nbr", "leftsemi"
    )
    return df_az_data


def transform_data(df_delta_table: DataFrame) -> DataFrame:
    """
    Receive a dataframe and applied this tranformations:
    Generate an Id using hash256 and concatenate specific columns
    Transform date type and timestamp type to string for  writing on cosmos
    """

    def _str_to_hash256(value: str) -> str:
        id_hash_256 = hashlib.sha256(value.encode())
        return id_hash_256.hexdigest()

    hash_function_udf = F.udf(_str_to_hash256, T.StringType())
    id_fields = cosmos.list_fields_id
    id_string = ", ".join([f"nvl({elem}, '')" for elem in id_fields])
    string_select_exp = f"concat({id_string}) as id"

    df_with_id = df_delta_table.selectExpr(string_select_exp, "*")

    date_columns = [col[0] for col in df_with_id.dtypes if col[1] == "date"]
    ts_columns = [col[0] for col in df_with_id.dtypes if col[1] == "timestamp"]

    for column in ts_columns:
        df_with_id = df_with_id.withColumn(
            column, F.date_format(F.col(column), "yyyy-MM-dd HH:mm:ss.SSS")
        )

    for column in date_columns:
        df_with_id = df_with_id.withColumn(
            column, F.date_format(F.col(column), "yyyy-MM-dd")
        )

    df_transform = df_with_id.withColumn(
        "id", hash_function_udf(df_with_id["id"])
    )
    return df_transform


def write_stream(spark: SparkSession, df_input: DataFrame) -> StreamingQuery:
    """
    Main write function to write to silver, anomaly, and late tables
    using a foreachBatch function

    Parameters:
    spark (SparkSession): spark session
    df (DataFrame):

    Returns:
    int: Description of return value
    """
    dbutils = config.get_dbutils(spark)

    service_credential = dbutils.secrets.get(
        scope=cosmos.scope(),
        key=cosmos.key(),
    )

    cosmos_database_name = cosmos.database_name()
    cosmos_container_name = cosmos.cosmos_container()
    auth_type = "ServicePrinciple"
    cosmos_endpoint = cosmos.endpoint()
    subscription_id = cosmos.subscription_id()
    tenant_id = cosmos.tenant_id()
    resource_group_name = cosmos.resource_group_name()
    client_id = cosmos.client_id()
    client_secret = service_credential

    cosmos_cfg = {
        "spark.cosmos.accountEndpoint": cosmos_endpoint,
        "spark.cosmos.auth.type": auth_type,
        "spark.cosmos.account.subscriptionId": subscription_id,
        "spark.cosmos.account.tenantId": tenant_id,
        "spark.cosmos.account.resourceGroupName": resource_group_name,
        "spark.cosmos.auth.aad.clientId": client_id,
        "spark.cosmos.auth.aad.clientSecret": client_secret,
        "spark.cosmos.database": cosmos_database_name,
        "spark.cosmos.container": cosmos_container_name,
        "spark.cosmos.write.bulk.enabled": "true",
        # "spark.cosmos.throughputControl.enabled": "true",
        # "spark.cosmos.throughputControl.name": "adas_throughput_control",
        # "spark.cosmos.throughputControl.targetThroughputThreshold": "0.95",
        # "spark.cosmos.throughputControl.globalControl.database": cosmos_database_name,
        # "spark.cosmos.throughputControl.globalControl.container": "throughput_control",
    }

    def write_cosmos(batch_df: DataFrame, batch_id: int) -> None:  # noqa
        """Handler function called in foreachBatch of write step."""
        (
            batch_df.write.format("cosmos.oltp")
            .options(**cosmos_cfg)
            .mode("APPEND")
            .save()
        )

    # checkpoint_path = config.silver.checkpoint_path()
    # checkpoint_path = os.path.join(checkpoint_path, "streaming_checkpoint")
    writer = (
        df_input.writeStream.foreachBatch(write_cosmos)
        .trigger(availableNow=True)
        .option(
            "checkpointLocation",
            cosmos.cosmos_checkpoint_path(),
        )
        .start()
    )

    return writer


def main() -> None:
    """
    Main function
    read the data for that specific task
    transform the data and write on cosmos db
    """
    spark = SparkSession.builder.appName("write-adas-cosmos-db").getOrCreate()
    df_input = read_data(spark, cosmos.table_path())
    df_arizona_vin = read_arizona_data(spark, cosmos.arizona_vin_table())
    df_transform = df_input.transform(join_data, df_arizona_vin).transform(
        transform_data
    )
    write_stream(spark, df_transform).awaitTermination()
