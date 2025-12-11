"""Debug transformation with print statements"""
import pandas as pd

# Read raw CSV
df = pd.read_csv('bca_exam_results.csv')

print("Step 1: Check if Subject Name exists:", 'Subject Name' in df.columns)
print("Step 2: Check if Student Name exists:", 'Student Name' in df.columns)

# Simulate the transformation logic
metadata_to_preserve = ['Seat No.', 'Student Name', 'Enrollment No.', 'SP ID']
id_priority = ['Enrollment No.', 'Seat No.', 'SP ID', 'Student ID', 'Roll No.']

student_id_col = None
for col in id_priority:
    if col in df.columns:
        student_id_col = col
        break

print(f"\nStep 3: student_id_col = '{student_id_col}'")

metadata_cols = []
for col in metadata_to_preserve:
    if col in df.columns:
        metadata_cols.append(col)

print(f"Step 4: metadata_cols = {metadata_cols}")

metadata_select = [col for col in metadata_cols if col != student_id_col]
print(f"Step 5: metadata_select = {metadata_select}")

# Test groupby
if metadata_select:
    metadata_df = df.groupby(student_id_col)[metadata_select].first().reset_index()
    print(f"\nStep 6: metadata_df shape = {metadata_df.shape}")
    print(f"Step 7: metadata_df columns = {list(metadata_df.columns)}")
    print(f"\nStep 8: First row of metadata_df:")
    print(metadata_df.iloc[0])
