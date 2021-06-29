from functools import reduce
import pyspark.sql.functions as F
import pyspark.sql.types as T


def apply_function_to_cols(col_names, f):
    def inner(source_df):
        return reduce(
            lambda df, col_name: df.withColumn(col_name, f(F.col(col_name))),
            set(col_names),
            source_df
        )
    return inner


def cast_cols_as_string(col_names):
    return apply_function_to_cols(col_names, lambda col: col.cast(T.StringType()))


def cast_cols_as_date(col_names):
    return apply_function_to_cols(col_names, lambda col: F.to_date(F.substring(col, 1, 10), "yyyy-MM-dd"))


def cast_cols_as_ts(col_names):
    return apply_function_to_cols(col_names, lambda col: F.from_unixtime(col.cast(T.IntegerType())))


def cast_cols_as_integer(col_names):
    return apply_function_to_cols(col_names, lambda col: col.cast(T.IntegerType()))


def add_exception_column(exception_column_name, malformed_column_names,
                         malformed_column_error, timestamp_column_names, timestamp_column_error):
    """
    Add column check unix timestamp / check if malformed detected
    """
    def inner(df):
        has_malformed = reduce(
            lambda x, y: x | y,
            [F.lower(F.trim(F.col(c))).isNotNull() for c in malformed_column_names]
        )
        rx = "^\d{0,10}(\.\d{1,4})?$"
        has_wrong_timestamp = reduce(
            lambda x, y: x | y,
            [(F.col(c)).rlike(rx) for c in timestamp_column_names]
        )
        return (
            df.withColumn(
                exception_column_name,
                F.when(
                    has_malformed,
                    F.when(
                        has_wrong_timestamp,
                        "{}|{}".format(malformed_column_error, timestamp_column_error))
                    .otherwise(malformed_column_error))
                .otherwise(
                    F.when(has_wrong_timestamp, timestamp_column_error)
                    .otherwise(None))
                )
        )
    return inner

