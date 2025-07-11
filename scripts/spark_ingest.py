from pyspark.sql import SparkSession
import os
import time

print("🧭 JAVA_HOME =", os.environ.get("JAVA_HOME"))
print("📂 Current working dir =", os.getcwd())
print("📄 Listing input dir...")
print(os.listdir("data"))  # Adjust if your folder isn't named input

def main():
    total_start = time.time()

    print("🟢 Starting Spark session...")
    spark = SparkSession.builder \
        .appName("CSV to Parquet Converter") \
        .getOrCreate()
    print("✅ Spark session created.")

    input_path = "data/Border_Crossing_Entry_Data.csv"
    output_dir = "output_spark"

    # Check path
    if not os.path.exists(input_path):
        print(f"❌ File does not exist: {input_path}")
        return
    print(f"📁 Found input file: {input_path}")

    # Read CSV
    try:
        read_start = time.time()
        df = spark.read.option("header", "true").csv(input_path)
        row_count = df.count()
        read_time = time.time() - read_start
        print(f"📊 DataFrame read: {row_count} rows in {read_time:.2f} seconds")
        
    except Exception as e:
        print(f"❌ Failed to read CSV: {e}")
        return

    # Ensure output dir exists
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "data.parquet")

    # Write Parquet
    try:
        write_start = time.time()
        df.write.mode("overwrite").parquet(output_path)
        write_time = time.time() - write_start
        print(f"✅ Successfully wrote to {output_path} in {write_time:.2f} seconds")
    except Exception as e:
        print(f"❌ Failed to write Parquet: {e}")
        return

    spark.stop()
    total_time = time.time() - total_start
    print("🔴 Spark session stopped.")
    print(f"⏱️ Total time elapsed: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
