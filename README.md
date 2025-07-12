spark-csv-to-parquet

a simple pyspark script to convert CSV files to Parquet, JSON, or ORC

usage

python spark_ingest.py 
  --input data/file.csv 
  --output output_dir 
  --format parquet 
  --infer-schema true 
  --repartition 4

dependencies:

- python 3.9+
- pyspark
- java 11/17
- spark 3.5X or 4.0.0

