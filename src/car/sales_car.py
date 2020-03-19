import logging

from pyspark.sql import SparkSession, DataFrame

from src.conf.settings import Settings, load_settings


def load_cars(spark: SparkSession, settings: Settings) -> DataFrame:
    """Read question dataset from input data

    :param spark: sparkSession
    :param settings: settings of paths
    :return:
    """
    path = settings.input_data_path + "/car/*.csv"
    logging.info("load raw questions from " + path)

    dfCars = (
        spark.read.option("delimiter", ",")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)

    )
    return dfCars


def run():
    settings = load_settings()
    spark = SparkSession.builder.appName("cars-analysis").getOrCreate()

    dfCars = load_cars(spark, settings)
    for types in dfCars.dtypes:
        print(types)


if __name__ == "__main__":
    run()
