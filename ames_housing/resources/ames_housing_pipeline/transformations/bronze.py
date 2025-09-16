import dlt
from pyspark.sql.functions import col, input_file_name, current_timestamp

path = spark.conf.get("volume_path_use")

@dlt.table(
    name="bronze",
    comment="Ingested housing data with metadata",
    table_properties={
        'quality': 'bronze',
        'delta.enableChangeDataFeed': 'true',
        'delta.enableDeletionVectors': 'true',
        'delta.enableRowTracking': 'true',
        'delta.autoOptimize.optimizeWrite': 'true',
        'delta.autoOptimize.autoCompact': 'true',
        'delta.feature.variantType-preview': 'supported'
    },
    cluster_by_auto=True
)
def bronze():
    """
    Reads the raw housing data CSV files as a streaming source using Auto Loader.
    """

    df = (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .option("mode", "PERMISSIVE")
        .option("delimiter", ",")
        .option("cloudFiles.schemaHints", "property_id STRING, sale_price NUMERIC")
        .load(path))

    return (
        df.selectExpr(
            "sha2(concat(_metadata.*), 256) as index_file_source_id",
            "_metadata as file_metadata",
            "CURRENT_TIMESTAMP() as ingest_time",
            "_metadata.file_modification_time as rcrd_timestamp",
            "*"
        )
    )
