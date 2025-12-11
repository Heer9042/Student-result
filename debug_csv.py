"""Debug: Check transformed dataframe columns"""
import pandas as pd
from utils.file_reader import read_csv_file, transform_long_to_wide_format

# Read raw CSV
df_raw = pd.read_csv('bca_exam_results.csv')

print("=" * 80)
print("Step 1: RAW CSV columns:")
print(list(df_raw.columns))

print("\n" + "=" * 80)
print("Step 2: Transform to wide format")
success, df_wide, msg = transform_long_to_wide_format(df_raw)
print(f"Success: {success}")
print(f"Message: {msg}")

if success:
    print("\n" + "=" * 80)
    print("Step 3: WIDE FORMAT columns:")
    for i, col in enumerate(df_wide.columns, 1):
        print(f"  {i}. {col}")
    
    print("\n" + "=" * 80)
    print("Step 4: First row data:")
    if len(df_wide) > 0:
        first = df_wide.iloc[0]
        print(f"  Enrollment No.: {first.get('Enrollment No.', 'NOT FOUND')}")
        print(f"  Seat No.: {first.get('Seat No.', 'NOT FOUND')}")
        print(f"  Student Name: {first.get('Student Name', 'NOT FOUND')}")
        print(f"  SP ID: {first.get('SP ID', 'NOT FOUND')}")
