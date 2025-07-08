import pandas as pd
import time


start = time.time()
df = pd.read_csv('data/Border_Crossing_Entry_data.csv')

print("Loaded CSV with shape:", df.shape)

df.to_parquet("output/borderdata.parquet", engine = 'pyarrow')
end = time.time()  

print(f"Time taken: {end - start:.2f} seconds")