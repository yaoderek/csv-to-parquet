import pandas as pd
import os
import time
import glob
from concurrent.futures import ProcessPoolExecutor

INPUT_DIR = "data/splits"
OUTPUT_DIR = "output"

def convert_csv_to_parquet(csv_path):
    try:
        file_name = os.path.basename(csv_path).replace(".csv", ".parquet")
        output_path = os.path.join(OUTPUT_DIR, file_name)

        df = pd.read_csv(csv_path)
        df.to_parquet(output_path, engine="pyarrow", compression="snappy")
    except Exception:
        pass  # Silently skip failures (or log later if needed)

def main():
    start = time.time()

    if not os.path.exists(INPUT_DIR):
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"))
    if not csv_files:
        return

    with ProcessPoolExecutor() as executor:
        list(executor.map(convert_csv_to_parquet, csv_files))

    end = time.time()
    print(f"✅ Done in {end - start:.2f} seconds.")

if __name__ == "__main__":
    main()
