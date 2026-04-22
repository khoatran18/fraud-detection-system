import logging

from pyspark.ml.classification import GBTClassificationModel

from common.logging.logging_config import setup_logging
from common.runtime.clickhouse.clickhouse_init import init_clickhouse
from common.runtime.clickhouse.prepare_clickhouse_native import prepare_clickhouse_native
from common.runtime.spark.spark_builder_minio_clickhouse import create_spark_kafka_minio_clickhouse
from common.sinks.clickhouse_sink import ClickHouseSink
from common.sources.kafka_source import read_kafka_stream
from config.settings import load_settings
from stream_layer.stream_processor.processor.event_prediction import event_prediction
from stream_layer.stream_processor.processor.event_processor import event_processor

MODEL_PATH = "../../model_ml/model/gbt_model"

def run_stream():
    # Init logging and Kafka producer
    setup_logging()
    logger = logging.getLogger(__name__)
    settings = load_settings()
    logger.info("Start stream processor...")

    try:
        init_clickhouse()
        topic = settings.kafka.topics.topic
        spark = create_spark_kafka_minio_clickhouse(app_name="stream_processor", settings=settings)
        spark = spark.getOrCreate()
        spark = prepare_clickhouse_native(spark=spark)
        clickhouse_writer = ClickHouseSink(spark=spark)
        raw_df = read_kafka_stream(spark, settings, topic)

        # Preprocess data
        # preprocess_df = event_processor(raw_df)

        # Predict
        model = GBTClassificationModel.load(MODEL_PATH)
        # prediction_df = event_prediction(preprocess_df, model)

        # prediction_df.writeStream.format("console").option("truncate", "false") \
        #     .option("numRows", 100) \
        #     .start().awaitTermination()
        def process_batch(batch_df, batch_id):
            # Kiểm tra nếu batch không trống
            if not batch_df.isEmpty():
                print(f"--- Processing Batch: {batch_id} ---")

                # 1. Preprocess (Bây giờ batch_df là tĩnh nên fit() sẽ chạy được)
                preprocess_df = event_processor(batch_df)

                # 2. Predict (Sử dụng transform bên trong event_prediction)
                prediction_df = event_prediction(preprocess_df, model)

                # 3. Output ra console để kiểm tra
                prediction_df.select("TransactionID", "prediction", "probability").show(truncate=False)

                # prediction_df.write.format("clickhouse").mode("append").save()
                clickhouse_writer.write_table(prediction_df, settings.storage.clickhouse.table)

        query = raw_df.writeStream \
            .foreachBatch(process_batch) \
            .start()

        query.awaitTermination()

    except Exception as e:
        logger.error("Error when stream processor: %s", e)

if __name__ == "__main__":
    run_stream()