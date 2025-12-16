"""
Module for processing and filtering student marks data.

This module provides functionality to:
- Calculate overall pass/fail status for students
- Calculate subject-wise pass/fail status
- Filter students based on various criteria
- Generate filtered result reports
"""

import pandas as pd
from typing import List, Dict, Tuple
import numpy as np
from config import PASS_THRESHOLD


class StudentMarksProcessor:
    """
    A class to process and filter student marks data.
    """
    
    def __init__(self, dataframe: pd.DataFrame, pass_threshold: int = PASS_THRESHOLD):
        """
        Initialize the processor with a dataframe of student marks.
        
        Args:
            dataframe (pd.DataFrame): DataFrame with Student Name and subject marks
            pass_threshold (int): Marks required to pass (default: 40)
        """
        self.df = dataframe.copy()
        self.pass_threshold = pass_threshold
        self.subject_columns = self._get_subject_columns()
    
    def _get_subject_columns(self) -> List[str]:
        """
        Get list of subject columns (excludes student metadata columns).
        
        For new CSV format (BBA_SEM5_OCT25.csv):
        - Excludes Int1-6 (internal marks) and Ext1-6 (external marks)
        - Only includes Total1-6 (final subject marks) for pass/fail evaluation
        
        Returns:
            List[str]: List of subject column names
        """
        # Define metadata columns that should not be treated as subjects
        metadata_columns = [
            'Student Name', 'Seat No.', 'Enrollment No.', 'SP ID', 
            'College Name', 'Exam Name', 'Student ID', 'Roll No.',
            'Overall Status', 'Passed Subjects', 'Failed Subjects',
            # New format columns
            'SeatNo', 'SPID', 'Gender', 'Name',
            # Summary columns
            'Total_INT', 'Total_EXT', 'Combined_Total', 'Pass_Fail'
        ]
        
        # Exclude patterns: Grade columns, Float columns, Int columns, Ext columns
        exclude_patterns = ['Grade', 'Int', 'Ext']
        
        # Add Float columns (Float_1 through Float_19)  
        for i in range(1, 20):
            metadata_columns.append(f'Float_{i}')
        
        # Filter out metadata columns and columns matching exclude patterns
        subject_cols = []
        for col in self.df.columns:
            # Skip if in metadata
            if col in metadata_columns:
                continue
            
            # Skip if matches any exclude pattern
            # But be careful: "Int" and "Ext" should exclude Int1, Ext1, etc.
            # but NOT exclude words like "Interest" or "External Affairs"
            skip = False
            for pattern in exclude_patterns:
                if pattern == 'Grade':
                    if pattern in col:
                        skip = True
                        break
                elif pattern == 'Int' or pattern == 'Ext':
                    # Only exclude if it's Int1, Int2, etc. or Ext1, Ext2, etc.
                    # Pattern: starts with Int or Ext followed by a digit
                    if col.startswith(pattern) and len(col) > len(pattern) and col[len(pattern)].isdigit():
                        skip = True
                        break
            
            if not skip:
                subject_cols.append(col)
        
        return subject_cols
    
    def calculate_overall_status(self) -> pd.DataFrame:
        """
        Calculate overall pass/fail status for each student.
        
        If CSV has 'Pass_Fail' column, use it (official university result).
        Otherwise, calculate based on subject marks.
        
        Returns:
            pd.DataFrame: Original dataframe with additional columns:
                - 'Overall Status': 'Pass' or 'Fail'
                - 'Passed Subjects': Count of subjects passed
                - 'Failed Subjects': Count of subjects failed
        """
        result_df = self.df.copy()
        
        # Count passed and failed subjects for each student
        passed_count = (result_df[self.subject_columns] >= self.pass_threshold).sum(axis=1)
        failed_count = (result_df[self.subject_columns] < self.pass_threshold).sum(axis=1)
        
        # Use Pass_Fail column from CSV if available (official result)
        if 'Pass_Fail' in result_df.columns:
            # Use the official Pass_Fail status from CSV
            overall_status = result_df['Pass_Fail'].fillna('Fail').tolist()
        else:
            # Calculate: pass only if all subjects passed
            overall_status = ['Pass' if failed_count[i] == 0 else 'Fail' 
                             for i in range(len(result_df))]
        
        result_df['Passed Subjects'] = passed_count
        result_df['Failed Subjects'] = failed_count
        result_df['Overall Status'] = overall_status
        
        return result_df
    
    def calculate_average_marks(self) -> pd.DataFrame:
        """
        Calculate average marks for each student across all subjects.
        
        Returns:
            pd.DataFrame: Original dataframe with 'Average Marks' column added
        """
        result_df = self.df.copy()
        result_df['Average Marks'] = result_df[self.subject_columns].mean(axis=1).round(2)
        return result_df
    
    def get_overall_pass_fail(self) -> pd.DataFrame:
        """
        Filter and return only students with their overall pass/fail status.
        
        Returns:
            pd.DataFrame: Dataframe with Student Name/Name, subjects, and overall status
        """
        df_with_status = self.calculate_overall_status()
        
        # Determine which name column exists
        name_col = 'Student Name' if 'Student Name' in df_with_status.columns else 'Name'
        
        result_cols = [name_col, 'Overall Status', 'Passed Subjects', 'Failed Subjects']
        # Only include columns that exist
        result_cols = [col for col in result_cols if col in df_with_status.columns]
        
        return df_with_status[result_cols]
    
    def is_passed_all_semesters(self, row):
        """
        Check if a student passed all semesters (all Float columns are numeric/pass values).
        
        Args:
            row: DataFrame row
            
        Returns:
            bool: True if passed all semesters, False otherwise
        """
        float_cols = self.get_float_columns()
        for col in float_cols:
            value = str(row[col]).strip()
            # If starts with 'F-' or 'F' followed by digit (like F-1), it's fail
            if value.startswith('F-') or (value.startswith('F') and len(value) > 1 and value[1].isdigit()):
                return False
            # Try to convert to float - if fails, consider as fail
            try:
                float(value)
            except ValueError:
                return False
        return True
    
    def filter_passed_students(self) -> pd.DataFrame:
        """
        Filter students who passed overall (passed in all semesters).
        Returns only student identity columns in specified order.
        
        Returns:
            pd.DataFrame: Dataframe with student info for those who passed all semesters
        """
        df_with_status = self.calculate_overall_status()
        
        # Filter only passed students (now based on semesters)
        passed_df = df_with_status[df_with_status.apply(lambda row: self.is_passed_all_semesters(row), axis=1)].copy()
        
        # Define desired column order: support both old and new format
        desired_order = [
            'Student Name', 'Name', 'Gender', 
            'Seat No.', 'SeatNo', 'Enrollment No.', 'SP ID', 'SPID', 
            'Exam Name', 
            'Float_1', 'Float_2', 'Float_3', 'Float_4', 'Float_5',  # CGPA and failure tracking
            'Passed Subjects', 'Overall Status'
        ]
        
        # Select only columns that exist in the dataframe, in the desired order
        result_cols = [col for col in desired_order if col in passed_df.columns]
        
        return passed_df[result_cols]
    
    def filter_failed_students(self) -> pd.DataFrame:
        """
        Filter students who failed overall (failed in at least one semester).
        Returns only student identity columns in specified order.
        
        Returns:
            pd.DataFrame: Dataframe with student info for those who failed at least one semester
        """
        df_with_status = self.calculate_overall_status()
        
        # Filter only failed students (now based on semesters)
        failed_df = df_with_status[~df_with_status.apply(lambda row: self.is_passed_all_semesters(row), axis=1)].copy()
        
        # Define desired column order: support both old and new format
        desired_order = [
            'Student Name', 'Name', 'Gender',
            'Seat No.', 'SeatNo', 'Enrollment No.', 'SP ID', 'SPID',
            'Exam Name', 
            'Float_1', 'Float_2', 'Float_3', 'Float_4', 'Float_5',  # CGPA and failure tracking
            'Failed Subjects', 'Overall Status'
        ]
        
        # Select only columns that exist in the dataframe, in the desired order
        result_cols = [col for col in desired_order if col in failed_df.columns]
        
        return failed_df[result_cols]
    
    def filter_subject_wise_pass(self, subject: str) -> pd.DataFrame:
        """
        Filter students who passed a specific subject.
        
        Args:
            subject (str): Name of the subject to filter by
            
        Returns:
            pd.DataFrame: Dataframe containing students who passed the subject
        """
        if subject not in self.subject_columns:
            raise ValueError(f"Subject '{subject}' not found in data")

        # Use dataframe with overall status to include pass/fail counts
        df_with_status = self.calculate_overall_status()
        # Add subject-specific status
        df_with_status['Status in ' + subject] = df_with_status[subject].apply(
            lambda x: 'Pass' if pd.notna(x) and x >= self.pass_threshold else 'Fail'
        )

        # Return full details: original columns + subject status and overall columns
        result_cols = list(self.df.columns) + ['Status in ' + subject, 'Overall Status', 'Passed Subjects', 'Failed Subjects']
        return df_with_status[df_with_status['Status in ' + subject] == 'Pass'][result_cols].copy()
    
    def filter_subject_wise_fail(self, subject: str) -> pd.DataFrame:
        """
        Filter students who failed a specific subject.
        
        Args:
            subject (str): Name of the subject to filter by
            
        Returns:
            pd.DataFrame: Dataframe containing students who failed the subject
        """
        if subject not in self.subject_columns:
            raise ValueError(f"Subject '{subject}' not found in data")

        df_with_status = self.calculate_overall_status()
        df_with_status['Status in ' + subject] = df_with_status[subject].apply(
            lambda x: 'Pass' if pd.notna(x) and x >= self.pass_threshold else 'Fail'
        )

        result_cols = list(self.df.columns) + ['Status in ' + subject, 'Overall Status', 'Passed Subjects', 'Failed Subjects']
        return df_with_status[df_with_status['Status in ' + subject] == 'Fail'][result_cols].copy()
    
    def get_subject_wise_summary(self) -> pd.DataFrame:
        """
        Get a summary of pass/fail statistics for each subject.
        
        Returns:
            pd.DataFrame: Summary with subject names and pass/fail counts
        """
        summary_data = []
        
        for subject in self.subject_columns:
            passed = (self.df[subject] >= self.pass_threshold).sum()
            failed = (self.df[subject] < self.pass_threshold).sum()
            total = len(self.df)
            pass_percentage = (passed / total * 100) if total > 0 else 0
            
            summary_data.append({
                'Subject': subject,
                'Passed': int(passed),
                'Failed': int(failed),
                'Total': total,
                'Pass Percentage': f"{pass_percentage:.2f}%"
            })
        
        return pd.DataFrame(summary_data)
    
    def get_overall_statistics(self) -> Dict[str, any]:
        """
        Get overall statistics for all students.
        
        Returns:
            Dict: Dictionary containing various statistics
        """
        df_with_status = self.calculate_overall_status()
        
        total_students = len(df_with_status)
        passed_students = len(df_with_status[df_with_status['Overall Status'] == 'Pass'])
        failed_students = total_students - passed_students
        pass_percentage = (passed_students / total_students * 100) if total_students > 0 else 0
        
        # Calculate statistics using nanmean, nanmax, nanmin to handle NaN values
        marks_data = self.df[self.subject_columns].values
        average_marks = np.nanmean(marks_data)
        highest_marks = np.nanmax(marks_data)
        lowest_marks = np.nanmin(marks_data)
        
        return {
            'Total Students': total_students,
            'Passed Students': passed_students,
            'Failed Students': failed_students,
            'Pass Percentage': f"{pass_percentage:.2f}%",
            'Average Class Marks': f"{average_marks:.2f}",
            'Highest Marks': f"{highest_marks:.0f}",
            'Lowest Marks': f"{lowest_marks:.0f}"
        }
    
    def filter_float_pass(self, float_column: str) -> pd.DataFrame:
        """
        Filter students who passed a specific semester (Float column has numeric value).
        
        Args:
            float_column (str): Float column name (e.g., 'Float_1', 'Float_2')
            
        Returns:
            pd.DataFrame: Dataframe containing students who passed that semester
        """
        if float_column not in self.df.columns:
            raise ValueError(f"Column '{float_column}' not found in data")
        
        df_with_status = self.calculate_overall_status()
        
        # Check if value is numeric (pass) or starts with 'F-' (fail)
        def is_passed(value):
            if pd.isna(value):
                return False
            value_str = str(value).strip()
            # If it starts with 'F-' or 'F' followed by hyphen, it's a fail
            if value_str.startswith('F-') or value_str.startswith('F'):
                return False
            # Try to convert to float - if successful, it's a numeric pass value
            try:
                float(value_str)
                return True
            except:
                return False
        
        df_with_status[f'Status in {float_column}'] = df_with_status[float_column].apply(
            lambda x: 'Pass' if is_passed(x) else 'Fail'
        )
        
        # Define desired column order
        desired_order = [
            'Student Name', 'Name', 'Gender',
            'Seat No.', 'SeatNo', 'Enrollment No.', 'SP ID', 'SPID',
            'Exam Name', 
            float_column, f'Status in {float_column}',
            'Float_1', 'Float_2', 'Float_3', 'Float_4', 'Float_5'
        ]
        
        # Select only columns that exist
        result_cols = [col for col in desired_order if col in df_with_status.columns]
        
        return df_with_status[df_with_status[f'Status in {float_column}'] == 'Pass'][result_cols].copy()
    
    def filter_float_fail(self, float_column: str) -> pd.DataFrame:
        """
        Filter students who failed a specific semester (Float column has F-n value).
        
        Args:
            float_column (str): Float column name (e.g., 'Float_1', 'Float_2')
            
        Returns:
            pd.DataFrame: Dataframe containing students who failed that semester
        """
        if float_column not in self.df.columns:
            raise ValueError(f"Column '{float_column}' not found in data")
        
        df_with_status = self.calculate_overall_status()
        
        # Check if value is numeric (pass) or starts with 'F-' (fail)
        def is_passed(value):
            if pd.isna(value):
                return False
            value_str = str(value).strip()
            # If it starts with 'F-' or 'F' followed by hyphen, it's a fail
            if value_str.startswith('F-') or value_str.startswith('F'):
                return False
            # Try to convert to float - if successful, it's a numeric pass value
            try:
                float(value_str)
                return True
            except:
                return False
        
        df_with_status[f'Status in {float_column}'] = df_with_status[float_column].apply(
            lambda x: 'Pass' if is_passed(x) else 'Fail'
        )
        
        # Define desired column order
        desired_order = [
            'Student Name', 'Name', 'Gender',
            'Seat No.', 'SeatNo', 'Enrollment No.', 'SP ID', 'SPID',
            'Exam Name',
            float_column, f'Status in {float_column}',
            'Float_1', 'Float_2', 'Float_3', 'Float_4', 'Float_5'
        ]
        
        # Select only columns that exist
        result_cols = [col for col in desired_order if col in df_with_status.columns]
        
        return df_with_status[df_with_status[f'Status in {float_column}'] == 'Fail'][result_cols].copy()
    
    def get_float_columns(self) -> List[str]:
        """
        Get list of Float columns available in the dataframe.
        
        Returns:
            List[str]: List of Float column names (e.g., ['Float_1', 'Float_2', ...])
        """
        return [col for col in self.df.columns if col.startswith('Float_')]
