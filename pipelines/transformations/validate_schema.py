from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def raw_filter_and_quarantine(raw_df_parsed: DataFrame) -> (DataFrame, DataFrame):
    """
    Filters and quarantines the parsed DataFrame based on the quarantine flag.

    Args:
        raw_df_parsed (DataFrame): The DataFrame containing parsed data.

    Returns:
        Tuple[DataFrame, DataFrame]: A tuple of two DataFrames - the verified data and the quarantined data.
    """

    # Filter the parsed DataFrame based on the quarantine flag and select only the 'parsed_value' columns
    raw_df_parsed_verified = raw_df_parsed.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Filter the parsed DataFrame based on the quarantine flag and drop 'parsed_value' and 'quarantine_flag' columns
    raw_df_quarantine = raw_df_parsed.filter(col("_corrupt_record").isNotNull()).select("_corrupt_record")

    return raw_df_parsed_verified, raw_df_quarantine