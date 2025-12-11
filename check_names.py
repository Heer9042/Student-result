"""Check Student Name values after transformation"""
from utils.file_reader import read_csv_file
import pandas as pd

s, df, m = read_csv_file('bca_exam_results.csv')

print("Success:", s)
print("Shape:", df.shape)
print("\nStudent Name column info:")
print("  dtype:", df['Student Name'].dtype)
print("  null count:", df['Student Name'].isna().sum())
print("  non-null count:", df['Student Name'].notna().sum())

print("\nFirst 5 Student Name values:")
for i in range(min(5, len(df))):
    val = df['Student Name'].iloc[i]
    print(f"  Row {i}: {repr(val)}")

print("\nFirst row all metadata:")
first_row = df.iloc[0]
for col in ['Student Name', 'Seat No.', 'Enrollment No.', 'SP ID']:
    if col in df.columns:
        print(f"  {col}: {repr(first_row[col])}")
