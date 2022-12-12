spark-submit \
--master "local[*]" \
--files configs/config.json \
etl_job/main_spark.py