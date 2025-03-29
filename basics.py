import logging
from pyspark.sql import SparkSession


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Add StreamHandler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)

# Initialize Spark session
spark = SparkSession.builder.appName("BasicApp").getOrCreate()


# Decorator for func name printing
def func_name_decorator(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Running: {func.__name__=}")
        func(*args, **kwargs)
    return wrapper


@func_name_decorator
def basic_commands() -> None:
    textFile = spark.read.text("README.md")
    logger.info(f"{textFile.count()=}")
    logger.info(f"{textFile.first()=}")
    logger.info(f"{textFile.filter(textFile.value.contains("Spark")).count()=}")


@func_name_decorator
def more_dataset_operations() -> None:
    pass

@func_name_decorator
def caching() -> None:
    pass


def main() -> None:
    basic_commands()
    more_dataset_operations()



if __name__ == '__main__':
    main()