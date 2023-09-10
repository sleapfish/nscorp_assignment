from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, input_file_name
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    TimestampType,
)
import datetime
from enum import Enum

# TODO: "meta_curated_validation_input" might sill be useful ?
META_COLUMNS_SCHEMA = StructType(
    [
        StructField("meta_pii_flag", BooleanType(), True),
        StructField("meta_data_classification", StringType(), True),
        StructField("meta_original_source", StringType(), True),
        StructField("meta_source_table", StringType(), True),
        StructField("meta_pipeline_name", StringType(), True),
        StructField("meta_inserted_timestamp", TimestampType(), True),
        StructField("meta_updated_timestamp", TimestampType(), True),
        StructField("meta_source_file_path", StringType(), True),
    ]
)


class NispClassification(str, Enum):
    """
    Enum for NISP classifications
    """

    public = "Public"
    restricted_use = "Restricted Use"
    highly_confidential = "Highly Confidential"


def add_lineage_fields(
    df: DataFrame,
    meta_original_source: str = None,
    meta_source_table: str = None,
    meta_pipeline_name: str = None,
    pii_flag: bool = False,
    data_classification: NispClassification = NispClassification.restricted_use.value,
) -> DataFrame:
    col_map = {
        "meta_pii_flag": pii_flag,
        "meta_data_classification": data_classification,
        "meta_original_source": meta_original_source,
        "meta_source_table": meta_source_table,
        "meta_pipeline_name": meta_pipeline_name,
        "meta_inserted_timestamp": datetime.datetime.utcnow(),
        "meta_updated_timestamp": datetime.datetime.utcnow(),
    }

    meta_columns_to_overwrite = [
        "meta_pipeline_name",
        "meta_inserted_timestamp",
        "meta_updated_timestamp",
    ]

    meta_columns_schema_dict = {}
    for field in META_COLUMNS_SCHEMA.fields:
        meta_columns_schema_dict[field.name] = field.dataType

    for name, value in col_map.items():
        if name not in df.columns or name in meta_columns_to_overwrite:
            df = df.withColumn(
                name, lit(value).cast(meta_columns_schema_dict.get(name))
            )

    if "meta_source_file_path" not in df.columns:
        df = df.withColumn("meta_source_file_path", input_file_name())

    return df
