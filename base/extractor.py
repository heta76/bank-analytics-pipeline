from abc import ABC, abstractmethod

class Extractor(ABC):
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    @abstractmethod
    def extract(self):
        pass