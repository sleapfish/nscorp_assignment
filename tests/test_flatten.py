import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pipelines.transformations.flatten import flatten_structs, flatten_maps, flatten_dataframe
from chispa import assert_df_equality

spark = SparkSession.builder.getOrCreate()

class TestFlatten:
    def dataframe_with_array(self, spark):
        # Define input data
        data = [
            (
                1,
                "John",
                {
                    "age": 30,
                    "gender": "M",
                    "address": {"city": "New York", "state": "NY"},
                },
                [
                    {"type": "home", "number": "555-1234"},
                    {"type": "work", "number": "555-5678"},
                ],
            ),
            (
                2,
                "Jane",
                {
                    "age": 25,
                    "gender": "F",
                    "address": {"city": "San Francisco", "state": "CA"},
                },
                [{"type": "home", "number": "555-4321"}],
            ),
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField(
                    "details",
                    StructType(
                        [
                            StructField("age", IntegerType(), True),
                            StructField("gender", StringType(), True),
                            StructField(
                                "address",
                                StructType(
                                    [
                                        StructField("city", StringType(), True),
                                        StructField("state", StringType(), True),
                                    ]
                                ),
                                True,
                            ),
                        ]
                    ),
                    True,
                ),
                StructField(
                    "phone_numbers",
                    ArrayType(
                        StructType(
                            [
                                StructField("type", StringType(), True),
                                StructField("number", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
            ]
        )
        dataframe_with_array = spark.createDataFrame(data, schema)
        yield dataframe_with_array

    def dataframe_with_multiple_arrays(self, spark):
        # Define input data
        data = [
            ("John", 30, [("Math", ["A", "B", "C"]), ("Science", ["B", "C", "D"])]),
            ("Alice", 25, [("English", ["A", "A", "B"]), ("History", ["B", "C", "C"])]),
        ]
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField(
                    "subjects",
                    ArrayType(
                        StructType(
                            [
                                StructField("name", StringType(), True),
                                StructField("grades", ArrayType(StringType()), True),
                            ]
                        ),
                        True,
                    ),
                    True,
                ),
            ]
        )
        dataframe_with_multiple_arrays = spark.createDataFrame(data, schema)
        yield dataframe_with_multiple_arrays

    def test_flatten_structs(self, spark):
        data = [
            (1, ("name1", "address1", 20)),
            (2, ("name2", "address2", 30)),
            (3, ("name3", "address3", 40)),
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField(
                    "details",
                    StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("address", StringType(), True),
                            StructField("age", IntegerType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
        df = spark.createDataFrame(data, schema)
        complex_fields = {
            "details": StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("address", StringType(), True),
                    StructField("age", IntegerType(), True),
                ]
            )
        }
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("details.name", StringType(), True),
                StructField("details.address", StringType(), True),
                StructField("details.age", IntegerType(), True),
            ]
        )
        expected_data = [
            (1, "name1", "address1", 20),
            (2, "name2", "address2", 30),
            (3, "name3", "address3", 40),
        ]
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        flattened_df = flatten_structs(df, "details", complex_fields)
        assert_df_equality(flattened_df, expected_df)

    def test_flatten_maps(self, spark):
        data = [
            (1, {"name": "Alice", "age": 25}),
            (2, {"name": "Bob", "age": 30}),
            (3, {"name": "Charlie", "age": 35}),
        ]
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("details", MapType(StringType(), StringType()), True),
            ]
        )
        df = spark.createDataFrame(data, schema)
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("details->name", StringType(), True),
                StructField("details->age", StringType(), True),
            ]
        )
        expected_data = [
            (1, "Alice", "25"),
            (2, "Bob", "30"),
            (3, "Charlie", "35"),
        ]
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        flattened_df = flatten_maps(df, "details")
        assert_df_equality(flattened_df, expected_df)

    def test_flatten_dataframe(self, spark, dataframe_with_array):
        # Define expected output
        expected_data = [
            (1, "John", 30, "M", "New York", "NY", "home", "555-1234"),
            (1, "John", 30, "M", "New York", "NY", "work", "555-5678"),
            (2, "Jane", 25, "F", "San Francisco", "CA", "home", "555-4321"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("details.age", IntegerType(), True),
                StructField("details.gender", StringType(), True),
                StructField("details.address.city", StringType(), True),
                StructField("details.address.state", StringType(), True),
                StructField("phone_numbers[].type", StringType(), True),
                StructField("phone_numbers[].number", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Apply function to input data
        result_df = flatten_dataframe(dataframe_with_array, sort_columns=True)

        # Check if result has the same columns as expected
        result_columns_backticked = [f"`{x}`" for x in result_df.columns]
        expected_columns_backticked = [f"`{x}`" for x in expected_df.columns]
        assert set(result_columns_backticked) == set(expected_columns_backticked)

        # Check if result matches expected output
        # Can't use chispa's assert_df_equality with ignore_column_order True option due to the backticks that are necessary
        result_df_ordered = result_df.select(expected_columns_backticked)
        assert_df_equality(result_df_ordered, expected_df)

    def test_flatten_dataframe_array_of_arrays(
        self, spark, dataframe_with_multiple_arrays
    ):
        # Define expected output
        expected_data = [
            ("John", 30, "Math", "A"),
            ("John", 30, "Math", "B"),
            ("John", 30, "Math", "C"),
            ("John", 30, "Science", "B"),
            ("John", 30, "Science", "C"),
            ("John", 30, "Science", "D"),
            ("Alice", 25, "English", "A"),
            ("Alice", 25, "English", "A"),
            ("Alice", 25, "English", "B"),
            ("Alice", 25, "History", "B"),
            ("Alice", 25, "History", "C"),
            ("Alice", 25, "History", "C"),
        ]

        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("subjects[].name", StringType(), True),
                StructField("subjects[].grades[]", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Apply function to input data
        result_df = flatten_dataframe(dataframe_with_multiple_arrays, sort_columns=True)

        # Check if result has the same columns as expected
        result_columns_backticked = [f"`{x}`" for x in result_df.columns]
        expected_columns_backticked = [f"`{x}`" for x in expected_df.columns]
        assert set(result_columns_backticked) == set(expected_columns_backticked)

        # Check if result matches expected output
        # Can't use chispa's assert_df_equality with ignore_column_order True option due to the backticks that are necessary
        result_df_ordered = result_df.select(expected_columns_backticked)
        assert_df_equality(result_df_ordered, expected_df)

    def test_flatten_dataframe_array_index(self, spark, dataframe_with_array):
        # Define expected output
        expected_data = [
            (1, "John", 30, "M", "New York", "NY", 0, "home", "555-1234"),
            (1, "John", 30, "M", "New York", "NY", 1, "work", "555-5678"),
            (2, "Jane", 25, "F", "San Francisco", "CA", 0, "home", "555-4321"),
        ]
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("details.age", IntegerType(), True),
                StructField("details.gender", StringType(), True),
                StructField("details.address.city", StringType(), True),
                StructField("details.address.state", StringType(), True),
                StructField("phone_numbers_index", IntegerType(), True),
                StructField("phone_numbers[].type", StringType(), True),
                StructField("phone_numbers[].number", StringType(), True),
            ]
        )
        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Apply function to input data
        result_df = flatten_dataframe(
            dataframe_with_array, sort_columns=True, add_array_index=True
        )

        # Check if result has the same columns as expected
        result_columns_backticked = [f"`{x}`" for x in result_df.columns]
        expected_columns_backticked = [f"`{x}`" for x in expected_df.columns]
        assert set(result_columns_backticked) == set(expected_columns_backticked)

        # Check if result matches expected output
        # Can't use chispa's assert_df_equality with ignore_column_order True option due to the backticks that are necessary
        result_df_ordered = result_df.select(expected_columns_backticked)
        assert_df_equality(result_df_ordered, expected_df)

    def test_flatten_dataframe_array_of_arrays_index(
        self, spark, dataframe_with_multiple_arrays
    ):
        # Define expected output
        expected_data = [
            ("John", 30, 0, "Math", 0, "A"),
            ("John", 30, 0, "Math", 1, "B"),
            ("John", 30, 0, "Math", 2, "C"),
            ("John", 30, 1, "Science", 0, "B"),
            ("John", 30, 1, "Science", 1, "C"),
            ("John", 30, 1, "Science", 2, "D"),
            ("Alice", 25, 0, "English", 0, "A"),
            ("Alice", 25, 0, "English", 1, "A"),
            ("Alice", 25, 0, "English", 2, "B"),
            ("Alice", 25, 1, "History", 0, "B"),
            ("Alice", 25, 1, "History", 1, "C"),
            ("Alice", 25, 1, "History", 2, "C"),
        ]

        expected_schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("subjects_index", IntegerType(), True),
                StructField("subjects[].name", StringType(), True),
                StructField("subjects[].grades_index", IntegerType(), True),
                StructField("subjects[].grades[]", StringType(), True),
            ]
        )

        expected_df = spark.createDataFrame(expected_data, expected_schema)

        # Apply function to input data
        result_df = flatten_dataframe(
            dataframe_with_multiple_arrays, sort_columns=True, add_array_index=True
        )

        # Check if result has the same columns as expected
        result_columns_backticked = [f"`{x}`" for x in result_df.columns]
        expected_columns_backticked = [f"`{x}`" for x in expected_df.columns]
        assert set(result_columns_backticked) == set(expected_columns_backticked)

        # Check if result matches expected output
        # Can't use chispa's assert_df_equality with ignore_column_order True option due to the backticks that are necessary
        result_df_ordered = result_df.select(expected_columns_backticked)
        assert_df_equality(result_df_ordered, expected_df)
