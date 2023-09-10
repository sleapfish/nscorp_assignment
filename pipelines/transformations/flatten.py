from pyspark.sql.types import StructType, ArrayType, MapType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict

# TODO: This code is based on https://github.com/MrPowers/quinn/pull/64/ but adapted to our needs
# Check when this gets merged and if we're able to use the code from there


def get_complex_fields(df: DataFrame) -> Dict[str, object]:
    """
    Returns a dictionary of complex field names and their data types from the input DataFrame's schema.

    Args:
        df (DataFrame): The input PySpark DataFrame.

    Returns:
        Dict[str, object]: A dictionary with complex field names as keys and their respective data types as values.
    """
    complex_fields = {
        field.name: field.dataType
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, StructType, MapType))
    }
    return complex_fields


def flatten_structs(
    df: DataFrame, col_name: str, complex_fields: dict, sep: str = "."
) -> DataFrame:
    """
    Flattens the specified StructType column in the input DataFrame and returns a new DataFrame with the flattened columns.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        col_name (str): The column name of the StructType to be flattened.
        complex_fields (dict): A dictionary of complex field names and their data types.
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to '_'.

    Returns:
        DataFrame: The DataFrame with the flattened StructType column.
    """
    # Below backticks ` are necessary if the col_name contains dots
    expanded = [
        F.col(f"`{col_name}`.{k}").alias(col_name + sep + k)
        for k in [n.name for n in complex_fields[col_name]]
    ]
    df = df.select("*", *expanded).drop(col_name)
    return df


def explode_arrays(
    df: DataFrame, col_name: str, add_array_index: bool = False, sep: str = "[]"
) -> DataFrame:
    """
    Explodes the specified ArrayType column in the input DataFrame and returns a new DataFrame with the exploded column.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        col_name (str): The column name of the ArrayType to be exploded.
        add_array_index (bool, optional): Add the index of the exploded value in the array as a seperate column named col_name_index.
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to '[]'.

    Returns:
        DataFrame: The DataFrame with the exploded ArrayType column.

    Raises:
        ValueError: If the specified column is not an ArrayType column.
    """
    if not isinstance(df.schema[col_name].dataType, ArrayType):
        raise ValueError(f"Column {col_name} is not an array column.")
    if add_array_index:
        explode_expression = (
            f"posexplode_outer(`{col_name}`) AS (`{col_name}_index`, `{col_name}{sep}`)"
        )
    else:
        explode_expression = f"explode_outer(`{col_name}`) AS `{col_name}{sep}`"
    df = df.selectExpr("*", explode_expression).drop(col_name)

    return df


def flatten_maps(df: DataFrame, col_name: str, sep: str = "->") -> DataFrame:
    """
    Flattens the specified MapType column in the input DataFrame and returns a new DataFrame with the flattened columns.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        col_name (str): The column name of the MapType to be flattened.
        sep (str, optional): The separator to use in the resulting flattened column names. Defaults to '->'.

    Returns:
        DataFrame: The DataFrame with the flattened MapType column.
    """
    keys_df = df.select(F.explode_outer(F.map_keys(F.col(col_name)))).distinct()
    keys = list(map(lambda row: row[0], keys_df.collect()))
    key_cols = list(
        map(
            lambda f: F.col(col_name).getItem(f).alias(col_name + sep + f),
            keys,
        )
    )
    drop_column_list = [col_name]
    df = df.select(
        [
            col_to_keep
            for col_to_keep in df.columns
            if col_to_keep not in drop_column_list
        ]
        + key_cols
    )
    return df


def flatten_dataframe(
    df: DataFrame, sort_columns: bool = False, add_array_index: bool = False
) -> DataFrame:
    """
    Flattens all complex data types (StructType, ArrayType, and MapType) in the input DataFrame and returns a
    new DataFrame with the flattened columns.

    Args:
        df (DataFrame): The input PySpark DataFrame.
        sort_columns (bool, optional): Sort the columns alphabetically. Defaults to False.
        add_array_index (bool, optional): Add the index of the exploded value in the array as a seperate column named col_name_index
        when there are arrays present in the DataFrame that needs to be flattened.

    Returns:
        DataFrame: The DataFrame with all complex data types flattened.

    Note:
        This function assumes the input DataFrame has a consistent schema across all rows. If you have files with
        different schemas, use the read_and_flatten_nested_files function instead.

    Example:
    >>> data = [
            (
                1,
                ("Alice", 25),
                {"A": 100, "B": 200},
                ["apple", "banana"],
                {"key": {"nested_key": 10}},
                {"A#": 1000, "B@": 2000},
            ),
            (
                2,
                ("Bob", 30),
                {"A": 150, "B": 250},
                ["orange", "grape"],
                {"key": {"nested_key": 20}},
                {"A#": 1500, "B@": 2500},
            ),
        ]
    >>> schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name_age", StructType([
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True)
            ]), True),
            StructField("scores", MapType(StringType(), IntegerType()), True),
            StructField("fruits", ArrayType(StringType()), True),
            StructField("nested_key_map", MapType(StringType(), MapType(StringType(), IntegerType())), True),
            StructField("special_chars_map", MapType(StringType(), IntegerType()), True),
        ])
    >>> df = spark.createDataFrame(data, schema)
    >>> flattened_df = flatten_dataframe(df)
    >>> flattened_df.show()
    >>> flattened_df_with_hyphen = flatten_dataframe(df, replace_char="-")
    >>> flattened_df_with_hyphen.show()
    """
    complex_fields = get_complex_fields(df)

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]

        if isinstance(complex_fields[col_name], StructType):
            df = flatten_structs(df, col_name, complex_fields)
        elif isinstance(complex_fields[col_name], ArrayType):
            df = explode_arrays(df, col_name, add_array_index)
        elif isinstance(complex_fields[col_name], MapType):
            df = flatten_maps(df, col_name)

        complex_fields = get_complex_fields(df)

    if sort_columns:
        columns_backticked = [f"`{x}`" for x in df.columns]
        df = df.select(sorted(columns_backticked))

    return df
