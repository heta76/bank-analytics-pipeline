from abc import ABC, abstractmethod

class Transformer(ABC):
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    @abstractmethod
    def transform(self, input_df=None):
        pass