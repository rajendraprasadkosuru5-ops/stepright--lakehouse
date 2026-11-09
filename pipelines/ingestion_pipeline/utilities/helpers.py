from pyspark.sql import SparkSession

def get_catalog() -> str:
    spark = SparkSession.getActiveSession()
    if spark is None:
        return 'dev1'
    return spark.conf.get("stepright1.catalog", 'dev1')

def landing_path(subfolder: str) -> str:
    return f"/Volumes/{get_catalog()}/stepright1/landing1/{subfolder}"