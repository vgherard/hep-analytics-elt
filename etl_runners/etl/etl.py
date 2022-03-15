class ETL:
    def __init__(self):
        pass

    def setup(self):
        pass

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def cleanup(self):
        pass

    @classmethod
    def run(cls, **kwargs):
        etl_instance = cls(**kwargs)
        etl_instance.setup()
        etl_instance.extract()
        etl_instance.transform()
        etl_instance.load()
        etl_instance.cleanup()
