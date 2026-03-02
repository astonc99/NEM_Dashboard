import pandas as pd

# Read the parquet file
df = pd.read_parquet('path/to/your/file.parquet')

# Print the first 20 rows
print(df.head(20))
