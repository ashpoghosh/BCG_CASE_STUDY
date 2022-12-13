unzip Data.zip
mkdir -p output_files/analysis_1
mkdir -p output_files/analysis_2
mkdir -p output_files/analysis_3
mkdir -p output_files/analysis_4
mkdir -p output_files/analysis_5
mkdir -p output_files/analysis_6
mkdir -p output_files/analysis_7
mkdir -p output_files/analysis_8
spark-submit \
--master "local[*]" \
--files configs/config.json \
etl_job/main_spark.py
