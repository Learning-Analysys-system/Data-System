from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

class SparkUtils:
    def __init__(self, cores = 4):
        self.spark = SparkSession.builder\
            .master(f"local[{cores}]")\
            .config("spark.driver.memory", "4g")\
            .config("spark.executor.memory", "4g")\
            .config("spark.driver.maxResultSize", "2g")\
            .config("spark.driver.host", "127.0.0.1")\
            .config("spark.driver.bindAddress", "127.0.0.1")\
            .getOrCreate()
        
    def __enter__(self):
        return self.spark
        
    def __exit__(self, exc_type, exc_value, traceback):
        pass