"""Demo: Test multiple exams for same student"""
import pandas as pd

# Create sample data with one student taking 2 different exams
data = {
    'Seat No.': [100, 100, 100, 100, 101, 101],
    'Exam Name': ['Semester 1 Exam', 'Semester 1 Exam', 'Semester 1 Exam', 
                   'Semester 2 Exam', 'Semester 2 Exam', 'Semester 1 Exam'],
    'Student Name': ['Amit Kumar', 'Amit Kumar', 'Amit Kumar', 
                     'Amit Kumar', 'Sneha Patel', 'Sneha Patel'],
    'Enrollment No.': ['ABC123', 'ABC123', 'ABC123', 'ABC123', 'XYZ456', 'XYZ456'],
    'SP ID': [11111, 11111, 11111, 11111, 22222, 22222],
    'Subject Name': ['Mathematics', 'Physics', 'Chemistry', 
                     'Mathematics', 'Mathematics', 'Physics'],
    'Total Marks': [85, 70, 65, 90, 75, 80]
}

df = pd.DataFrame(data)

# Save to CSV
df.to_csv('demo_multiple_exams.csv', index=False)
print("Created demo_multiple_exams.csv")
print("\nData preview:")
print(df.to_string(index=False))

# Now transform it
from utils.file_reader import read_student_marks
from utils.data_processor import StudentMarksProcessor

success, df_transformed, msg = read_student_marks('demo_multiple_exams.csv')

if success:
    print("\n" + "=" * 80)
    print("TRANSFORMED DATA:")
    print("=" * 80)
    print(f"Shape: {df_transformed.shape}")
    print(f"\nColumns: {list(df_transformed.columns)}")
    print(f"\nData:")
    print(df_transformed.to_string(index=False))
    
    print("\n" + "=" * 80)
    print("PASS/FAIL ANALYSIS:")
    print("=" * 80)
    processor = StudentMarksProcessor(df_transformed)
    passed = processor.filter_passed_students()
    failed = processor.filter_failed_students()
    
    print(f"\nPassed ({len(passed)} records):")
    if len(passed) > 0:
        print(passed.to_string(index=False))
    
    print(f"\nFailed ({len(failed)} records):")
    if len(failed) > 0:
        print(failed.to_string(index=False))
else:
    print(f"Error: {msg}")
