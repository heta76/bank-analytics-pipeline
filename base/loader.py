from abc import ABC, abstractmethod

class Loader(ABC):
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    @abstractmethod
    def load(self, df, table_name: str):
        pass