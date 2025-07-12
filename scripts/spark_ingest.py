import os
import time
import argparse
from pyspark.sql import SparkSession

def parse_args():
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet/JSON/ORC using PySpark.")
    parser.add_argument("--input", type=str, help="Input CSV file path")
    parser.add_argument("--output", type=str, default="output_spark", help="Output directory")
    parser.add_argument("--format", type=str, default="parquet", choices=["parquet", "json", "orc"], help="Output format")
    parser.add_argument("--infer-schema", type=str, default="false", help="Whether to infer schema (true/false)")
    parser.add_argument("--repartition", type=int, help="Optional number of output partitions")
    return parser.parse_args()

def find_first_csv(directory="data"):
    files = os.listdir(directory)
    for f in files:
        if f.endswith(".csv"):
            return os.path.join(directory, f)
    return None

def main():
    args = parse_args()

    input_path = args.input or find_first_csv()
    output_dir = args.output
    output_format = args.format
    infer_schema = args.infer_schema.lower() == "true"
    repartition = args.repartition

    if not input_path or not os.path.exists(input_path):
        print(f"input file not found: {input_path}")
        return

    os.makedirs(output_dir, exist_ok=True)
    output_filename = os.path.splitext(os.path.basename(input_path))[0] + f".{output_format}"
    output_path = os.path.join(output_dir, output_filename)

    print(f"reading from: {input_path}")
    print(f"writing to: {output_path}")
    print(f"format: {output_format}, infer_schema: {infer_schema}, repartition: {repartition or 'default'}")

    spark = SparkSession.builder.appName("CSV Converter").getOrCreate()

    try:
        start_time = time.time()
        df = spark.read.option("header", "true").option("inferSchema", str(infer_schema).lower()).csv(input_path)
        row_count = df.count()
        print(f"rows: {row_count}")
        df.printSchema()

        if repartition:
            df = df.repartition(repartition)

        df.write.mode("overwrite").format(output_format).save(output_path)
        elapsed = time.time() - start_time
        print(f"done in {elapsed:.2f} seconds")
    except Exception as e:
        print(f"error: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
