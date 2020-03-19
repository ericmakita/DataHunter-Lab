import logging
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, FloatType, StringType, TimestampType

from src.conf.settings import Settings


def get_unit_string_value(df: DataFrame, df_column: str):
    """Get the value of in the column as a string

       :param df: dataframe
       :param df_column: column name
       :return:
       """
    return str(
        df.select(F.col(df_column)).distinct().toPandas()[df_column].values[0]
    )


def get_unit_float_value(df: DataFrame, df_column: str):
    """Get the value of in the column as a float

    :param df: dataframe
    :param df_column: column name
    :return:
    """
    return float(
        df.select(F.col(df_column)).distinct().toPandas()[df_column].values[0]
    )


def get_unit_datetime_value(df: DataFrame, df_column: str):
    """Get the value of in the column as a datetime

    :param df: dataframe
    :param df_column: column name
    :return:
    """
    return datetime(
        df.select(F.col(df_column)).distinct().toPandas()[df_column].values[0]
    )


def get_unit_int_value(df: DataFrame, df_column: str):
    """Get the value of in the column as a int

    :param df: dataframe
    :param df_column: column name
    :return:
    """
    return int(
        df.select(F.col(df_column)).distinct().toPandas()[df_column].values[0]
    )


def get_list_double_value(df: DataFrame, df_columns_to_double_type: list):
    """Cast some columns to double

    :param df: dataframe
    :param df_columns_to_double_type: columns name
    :return:
    """

    for c in df_columns_to_double_type:
        df = df.withColumn(c, df[c].cast(DoubleType()))

    return df


def get_list_float_value(df: DataFrame, df_columns_to_float_type: list):
    """Cast some columns to float

    :param df: dataframe
    :param df_columns_to_float_type: columns name
    :return:
    """

    for c in df_columns_to_float_type:
        df = df.withColumn(c, df[c].cast(FloatType()))

    return df


def get_list_string_value(df: DataFrame, df_columns_to_string_type: list):
    """Cast some columns to string

    :param df: dataframe
    :param df_columns_to_string_type: columns name
    :return:
    """

    for c in df_columns_to_string_type:
        df = df.withColumn(c, df[c].cast(StringType()))

    return df


def get_list_timestamp_value(df: DataFrame, df_columns_to_timestamp_type: list):
    """Cast some columns to string

    :param df: dataframe
    :param df_columns_to_timestamp_type: columns name
    :return:
    """

    for c in df_columns_to_timestamp_type:
        df = df.withColumn(c, df[c].cast(TimestampType()))

    return df


def check_for_duplicate(df: DataFrame, key_name: str):
    """
    Check for duplicate key_name in df
    - df_with_duplicate contains the data with keys appearing more than one
    - num_lines_with_duplicate count the number of line in the data with duplicated keys
    - keys_in_duplicate_to_remove contains the keys that appears more than once
    """

    df_with_duplicate = df.groupBy(key_name).count().filter(F.col("count") > 1)
    num_lines_with_duplicate = df_with_duplicate.count()
    keys_in_duplicate_to_remove = list(
        df_with_duplicate.select(F.col(key_name)).distinct().toPandas()[key_name]
    )

    return df_with_duplicate, num_lines_with_duplicate, keys_in_duplicate_to_remove


def save_dataframe(df: DataFrame, settings: Settings, data_directory: str):
    """Save new deduplicate data

    :param df: prepared data
    :param settings: class of settings with path to save
    :param data_directory: directory to save, ex : "/segmentation/deduplicated"
    :param save_format: either csv or avro, default is avro
    :return:
    """
    path = settings.processed_files_path + data_directory
    logging.info("Saving dataframe to " + path)

    if settings.output_format == "csv":
        # save in csv
        df.repartition(1).write.format("csv").option("header", "true").option(
            "delimiter", ","
        ).mode("overwrite").save(path)

    else:
        # save in avro format
        df.repartition(1).write.format("csv").mode("overwrite").save(path)


def show_me(df: DataFrame, num_to_show=1):
    print("count: " + str(df.count()))
    df.printSchema()
    df.show(num_to_show, vertical=True, truncate=False)


def hr_counts_of_businessTravel_and_JobRole_and_EmployeeNumber(
        df: DataFrame,
        count_businessTravel_name="count_make",
        count_JobRole_name="count_model",
        count_employeeNumber_name="count_uid"
) -> DataFrame:
    count_df = df.groupBy(F.col("Department")) \
        .agg(F.countDistinct(F.col("make")).alias(count_businessTravel_name),
             F.countDistinct(F.col("model")).alias(count_JobRole_name),
             F.countDistinct(F.col("unique_identity")).alias(count_employeeNumber_name))

    return count_df


def get_current_month():
    return datetime.date.today().replace(day=1)


def date_to_int(d: datetime.date):
    return d.year * 10000 + d.month * 100 + d.day
