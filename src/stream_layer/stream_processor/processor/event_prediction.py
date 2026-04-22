from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from stream_layer.stream_processor.schema.stream_schema import SELECTED_COLS


def event_prediction(df: DataFrame, model):
    assembler = VectorAssembler(
        inputCols=SELECTED_COLS,
        outputCol="features",
        handleInvalid="keep"
    )

    ensembled_df = assembler.transform(df)

    df_prediction = model.transform(ensembled_df)
    df_prediction = df_prediction.withColumn("probability", vector_to_array(col("probability")))

    return df_prediction
