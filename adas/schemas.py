"""
This module contains the schemas used by ADAS VBS's bronze and silver layer
"""
from pyspark.sql import types as T

# Possible values for string -> boolean types
boolean_items = ["false", "true"]

# Columns for partition date
DATE_PARTITION_COLUMN = "p_year"
ANOMALY_PARTITION_COLUMN = "p_date"
# schema of data in bronze ADLS
bronze_schema = (
    T.StructType()
    .add("topic_name", T.StringType())
    .add("message_id", T.StringType())
    .add("partition_key", T.StringType())
    .add("data", T.StringType())
    .add("schema_version", T.StringType())
    .add("event_ts_unix_mil", T.LongType())
    .add("publish_ts_unix_mil", T.LongType())
    .add("properties", T.StringType())
    .add("redelivery_count", T.StringType())
    .add("bronze_processed_ts_utc", T.StringType())
)
# Schema of json data field in bronze layer
data_raw = (
    T.StructType()
    .add("vin", T.StringType())
    .add("year", T.StringType())
    .add("make", T.StringType())
    .add("model", T.StringType())
    .add("trim", T.StringType())
    .add("convertible", T.StringType())
    .add("sunroof", T.StringType())
    .add("panoramic_sunroof", T.StringType())
    .add("adaptive_cruise_control", T.StringType())
    .add("piloted_driving", T.StringType())
    .add("forward_collision_warning", T.StringType())
    .add("forward_collision_mitigation", T.StringType())
    .add("rear_collision_warning", T.StringType())
    .add("rear_collision_mitigation", T.StringType())
    .add("lane_departure_warning", T.StringType())
    .add("lane_keeping_system", T.StringType())
    .add("lane_centering", T.StringType())
    .add("driver_drowsiness_detection", T.StringType())
    .add("driver_monitoring", T.StringType())
    .add("emergency_driver_assist", T.StringType())
    .add("blindspot_monitoring", T.StringType())
    .add("night_vision", T.StringType())
    .add("pedestrian_detection", T.StringType())
    .add("rear_cross_traffic_alert", T.StringType())
    .add("adaptive_headlights", T.StringType())
    .add("advanced_parking_assist", T.StringType())
    .add("speed_limit_sign_detection", T.StringType())
    .add("engineRpo", T.StringType())
    .add("engineRpoDesc", T.StringType())
    .add("productionDate", T.StringType())
    .add("markerdate", T.StringType())
)

# dic use for define and check the anomalies in the data
narrow_anomalies = {
    "vin": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_vin",
    },
    "year": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_year",
    },
    "make": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_make",
    },
    "model": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_model",
    },
    "trim": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_trim",
    },
    "convertible": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_convertible",
    },
    "sunroof": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_sunroof",
    },
    "panoramic_sunroof": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_panoramic_sunroof",
    },
    "adaptive_cruise_control": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_adaptive_cruise_control",
    },
    "piloted_driving": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_piloted_driving",
    },
    "forward_collision_warning": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_forward_collision_warning",
    },
    "forward_collision_mitigation": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_forward_collision_mitigation",
    },
    "rear_collision_warning": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_rear_collision_warning",
    },
    "rear_collision_mitigation": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_rear_collision_mitigation",
    },
    "lane_departure_warning": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_lane_departure_warning",
    },
    "lane_keeping_system": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_lane_keeping_system",
    },
    "lane_centering": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_lane_centering",
    },
    "driver_drowsiness_detection": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_driver_drowsiness_detection",
    },
    "driver_monitoring": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_driver_monitoring",
    },
    "emergency_driver_assist": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_emergency_driver_assist",
    },
    "blindspot_monitoring": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_blindspot_monitoring",
    },
    "night_vision": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_night_vision",
    },
    "pedestrian_detection": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_pedestrian_detection",
    },
    "rear_cross_traffic_alert": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_rear_cross_traffic_alert",
    },
    "adaptive_headlights": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_adaptive_headlights",
    },
    "advanced_parking_assist": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_advanced_parking_assist",
    },
    "speed_limit_sign_detection": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": boolean_items,
        "anomaly_column": "bad_speed_limit_sign_detection",
    },
    "engineRpo": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_engine_rpo",
    },
    "engineRpoDesc": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_engine_rpo_desc",
    },
    "productionDate": {
        "lower_bound": None,
        "upper_bound": None,
        "can_be_null": False,
        "associated_values": None,
        "anomaly_column": "bad_production_date",
    },
}

# dic use for renames columns
columns_rename = {
    "vin": "vin_nbr",
    "year": "model_year",
    "engineRpo": "engine_rpo",
    "engineRpoDesc": "engine_rpo_desc",
    "productionDate": "production_date",
}

silver_schema = (
    T.StructType()
    .add("vin_nbr", T.StringType())
    .add("model_year", T.ShortType())
    .add("make", T.StringType())
    .add("model", T.StringType())
    .add("trim", T.StringType())
    .add("convertible", T.BooleanType())
    .add("sunroof", T.BooleanType())
    .add("panoramic_sunroof", T.BooleanType())
    .add("adaptive_cruise_control", T.BooleanType())
    .add("piloted_driving", T.BooleanType())
    .add("forward_collision_warning", T.BooleanType())
    .add("forward_collision_mitigation", T.BooleanType())
    .add("rear_collision_warning", T.BooleanType())
    .add("rear_collision_mitigation", T.BooleanType())
    .add("lane_departure_warning", T.BooleanType())
    .add("lane_keeping_system", T.BooleanType())
    .add("lane_centering", T.BooleanType())
    .add("driver_drowsiness_detection", T.BooleanType())
    .add("driver_monitoring", T.BooleanType())
    .add("emergency_driver_assist", T.BooleanType())
    .add("blindspot_monitoring", T.BooleanType())
    .add("night_vision", T.BooleanType())
    .add("pedestrian_detection", T.BooleanType())
    .add("rear_cross_traffic_alert", T.BooleanType())
    .add("adaptive_headlights", T.BooleanType())
    .add("advanced_parking_assist", T.BooleanType())
    .add("speed_limit_sign_detection", T.BooleanType())
    .add("engine_rpo", T.StringType())
    .add("engine_rpo_desc", T.StringType())
    .add("production_date", T.DateType())
    .add("silver_processed_time", T.StringType())
    .add(DATE_PARTITION_COLUMN, T.StringType())
)

# flags for anomalie dataframe
narrow_anomaly_flags = [
    "bad_vin",
    "bad_year",
    "bad_make",
    "bad_model",
    "bad_trim",
    "bad_convertible",
    "bad_sunroof",
    "bad_panoramic_sunroof",
    "bad_adaptive_cruise_control",
    "bad_piloted_driving",
    "bad_forward_collision_warning",
    "bad_forward_collision_mitigation",
    "bad_rear_collision_warning",
    "bad_rear_collision_mitigation",
    "bad_lane_departure_warning",
    "bad_lane_keeping_system",
    "bad_lane_centering",
    "bad_driver_drowsiness_detection",
    "bad_driver_monitoring",
    "bad_emergency_driver_assist",
    "bad_blindspot_monitoring",
    "bad_night_vision",
    "bad_pedestrian_detection",
    "bad_rear_cross_traffic_alert",
    "bad_adaptive_headlights",
    "bad_advanced_parking_assist",
    "bad_speed_limit_sign_detection",
    "bad_engine_rpo",
    "bad_engine_rpo_desc",
    "bad_production_date",
]


# Anomaly Table
def anomaly_schema() -> T.StructType:
    """Generates the anomaly table schema."""

    schema = T.StructType().add("vin_nbr", T.StringType())

    for flag_col in narrow_anomaly_flags:
        schema = schema.add(flag_col, T.BooleanType())

    schema = (
        schema.add("silver_processed_time", T.StringType())
        .add(ANOMALY_PARTITION_COLUMN, T.StringType())
        .add("data", T.StringType())
    )
    return schema
