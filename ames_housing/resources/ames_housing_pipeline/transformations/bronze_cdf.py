from pyspark import pipelines as dp

@dp.view()
def bronze_cdf():
    df = spark.readStream.option("readChangeFeed", "true").table(f"bronze")
    return df