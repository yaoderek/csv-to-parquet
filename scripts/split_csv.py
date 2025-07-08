import pandas as pd
import os

INPUT_FILE = "data/Border_Crossing_Entry_Data.csv"  # your big CSV
OUTPUT_DIR = "data/splits"
CHUNK_SIZE = 100000  # rows per file

def split_csv():
    if not os.path.exists(INPUT_FILE):
        print(f"[✘] File not found: {INPUT_FILE}")
        return
    
    print(f"Splitting {INPUT_FILE} into chunks of {CHUNK_SIZE} rows each...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for i, chunk in enumerate(pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE)):
        out_path = os.path.join(OUTPUT_DIR, f"chunk_{i}.csv")
        chunk.to_csv(out_path, index=False)
        print(f"[✔] Wrote {out_path} with {len(chunk)} rows")

if __name__ == "__main__":
    split_csv()
