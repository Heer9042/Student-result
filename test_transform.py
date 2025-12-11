"""Test the column order in filtered results"""
import pandas as pd
from utils.file_reader import read_student_marks
from utils.data_processor import StudentMarksProcessor

# Read the BCA exam results
success, df, message = read_student_marks('bca_exam_results.csv')

if success:
    processor = StudentMarksProcessor(df)
    
    # Test passed students filter
    print("=" * 80)
    print("✅ PASSED STUDENTS")
    print("=" * 80)
    passed = processor.filter_passed_students()
    print(f"Total passed: {len(passed)}")
    print(f"\nColumn Order:")
    for i, col in enumerate(passed.columns, 1):
        print(f"  {i}. {col}")
    
    if len(passed) > 0:
        print(f"\nFirst 3 passed students:")
        print(passed.head(3).to_string(index=False))
    
    # Test failed students filter
    print("\n" + "=" * 80)
    print("❌ FAILED STUDENTS")
    print("=" * 80)
    failed = processor.filter_failed_students()
    print(f"Total failed: {len(failed)}")
    print(f"\nColumn Order:")
    for i, col in enumerate(failed.columns, 1):
        print(f"  {i}. {col}")
    
    if len(failed) > 0:
        print(f"\nFirst 3 failed students:")
        print(failed.head(3).to_string(index=False))
else:
    print(f"Error: {message}")
