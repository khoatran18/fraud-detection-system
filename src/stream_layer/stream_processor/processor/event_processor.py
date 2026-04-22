from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

from stream_layer.stream_processor.schema.stream_schema import STREAM_SCHEMA


def event_processor(df: DataFrame):
    """
    Full logic to process event
    """

    df = cast_event(df)
    df = event_string_indexer(df)
    return df

def cast_event(df: DataFrame):
    """
    Cast event to string and parse it
    """

    return df.select(col("value").cast("string").alias("raw_data")) \
        .select(col("raw_data"), from_json(col("raw_data"), STREAM_SCHEMA).alias("data")) \
        .select("data.*")

def event_string_indexer(df: DataFrame):
    SKIP = {"TransactionID", "isFraud"}
    str_cols = [f.name for f in df.schema.fields
                if isinstance(f.dataType, StringType) and f.name not in SKIP]
    if not str_cols:
        return df

    indexers = [
        StringIndexer(inputCol=c, outputCol=c + "_idx", handleInvalid="keep")
        for c in str_cols
    ]
    pipeline = Pipeline(stages=indexers)
    encoded_df = pipeline.fit(df).transform(df)

    for c in str_cols:
        encoded_df = encoded_df.drop(c).withColumnRenamed(c + "_idx", c)

    return encoded_df