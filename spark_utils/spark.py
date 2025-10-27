from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

class SparkUtils:
    def __init__(self, cores = 4):
        self.spark = SparkSession.builder\
            .master(f"local[{cores}]")\
            .getOrCreate()
        
    def __enter__(self):
        return self.spark
        
    def __exit__(self, exc_type, exc_value, traceback):
        pass