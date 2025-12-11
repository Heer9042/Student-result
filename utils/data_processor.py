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
        
        Returns:
            List[str]: List of subject column names
        """
        # Define metadata columns that should not be treated as subjects
        metadata_columns = [
            'Student Name', 'Seat No.', 'Enrollment No.', 'SP ID', 
            'College Name', 'Exam Name', 'Student ID', 'Roll No.',
            'Overall Status', 'Passed Subjects', 'Failed Subjects'
        ]
        return [col for col in self.df.columns if col not in metadata_columns]
    
    def calculate_overall_status(self) -> pd.DataFrame:
        """
        Calculate overall pass/fail status for each student.
        
        A student passes if they pass in all subjects, otherwise they fail.
        
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
        
        # Determine overall status (pass only if all subjects passed)
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
            pd.DataFrame: Dataframe with Student Name, subjects, and overall status
        """
        df_with_status = self.calculate_overall_status()
        return df_with_status[['Student Name', 'Overall Status', 'Passed Subjects', 'Failed Subjects']]
    
    def filter_passed_students(self) -> pd.DataFrame:
        """
        Filter students who passed overall (passed in all subjects).
        Returns only student identity columns in specified order.
        
        Returns:
            pd.DataFrame: Dataframe with student info for those who passed all subjects
        """
        df_with_status = self.calculate_overall_status()
        
        # Filter only passed students
        passed_df = df_with_status[df_with_status['Overall Status'] == 'Pass'].copy()
        
        # Define desired column order: Student Name, Seat No., Enrollment No., SP ID, Exam Name, Passed Subjects, Overall Status
        desired_order = ['Student Name', 'Seat No.', 'Enrollment No.', 'SP ID', 'Exam Name', 'Passed Subjects', 'Overall Status']
        
        # Select only columns that exist in the dataframe, in the desired order
        result_cols = [col for col in desired_order if col in passed_df.columns]
        
        return passed_df[result_cols]
    
    def filter_failed_students(self) -> pd.DataFrame:
        """
        Filter students who failed overall (failed in at least one subject).
        Returns only student identity columns in specified order.
        
        Returns:
            pd.DataFrame: Dataframe with student info for those who failed at least one subject
        """
        df_with_status = self.calculate_overall_status()
        
        # Filter only failed students
        failed_df = df_with_status[df_with_status['Overall Status'] == 'Fail'].copy()
        
        # Define desired column order: Student Name, Seat No., Enrollment No., SP ID, Exam Name, Failed Subjects, Overall Status
        desired_order = ['Student Name', 'Seat No.', 'Enrollment No.', 'SP ID', 'Exam Name', 'Failed Subjects', 'Overall Status']
        
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
