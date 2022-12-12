mkdir data
unzip Data.zip -d data/

mkdir output_files/analysis_1
mkdir output_files/analysis_2
mkdir output_files/analysis_3
mkdir output_files/analysis_4
mkdir output_files/analysis_5
mkdir output_files/analysis_6
mkdir output_files/analysis_7
mkdir output_files/analysis_8

spark-submit \
--master "local[*]" \
--files configs/config.json \
etl_job/main_spark.py
