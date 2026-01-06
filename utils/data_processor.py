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
    
    def __init__(self, dataframe: pd.DataFrame, pass_threshold: int = PASS_THRESHOLD, selected_subjects: List[str] = None):
        """
        Initialize the processor with a dataframe of student marks.
        
        Args:
            dataframe (pd.DataFrame): DataFrame with Student Name and subject marks
            pass_threshold (int): Marks required to pass (default: 40)
            selected_subjects (List[str], optional): If provided, use this list of subjects. Defaults to None.
        """
        self.df = dataframe.copy()
        # Normalize column names to lowercase for consistency
        self.df.columns = [str(col).lower() for col in self.df.columns]
        self.pass_threshold = pass_threshold
        if selected_subjects is not None:
            self.subject_columns = [s.lower() for s in selected_subjects if s.lower() in self.df.columns]
        else:
            self.subject_columns = self._get_subject_columns()

    def _get_subject_columns(self) -> List[str]:
        """
        Get list of subject columns (excludes student metadata columns).
        """
        # Define metadata columns that should not be treated as subjects
        metadata_columns = [
            'student name', 'seat no.', 'seat no', 'enrollment no.', 'enrollment no', 
            'sp id', 'spid', 'college name', 'exam name', 'student id', 'roll no.',
            'overall status', 'passed subjects', 'failed subjects',
            # New format columns
            'seatno', 'gender', 'name', 'cast', 'id', 'result', 'out of 10',
            # Summary columns
            'total_int', 'total_ext', 'combined_total', 'pass_fail'
        ]  
        
        # Exclude patterns
        exclude_patterns = ['grade', ',,', ' total', 'sem-', 'float_']
        
        subject_cols = []
        for col in self.df.columns:
            # Skip metadata columns
            if col in metadata_columns:
                continue
            # Skip columns matching exclude patterns
            if any(pattern in col for pattern in exclude_patterns):
                continue
            subject_cols.append(col)
        
        return subject_cols


    def get_sem_columns(self) -> List[str]:
        """
        Get a unique, sorted list of semester column prefixes available in the dataframe.
        e.g., ['sem-1', 'sem-2']
        """
        sem_cols = set()
        for col in self.df.columns:
            if str(col).lower().startswith('sem-'):
                # Extract 'sem-X' part, ignoring ' seat'
                parts = str(col).lower().split(' ')
                sem_cols.add(parts[0])
        
        # Sort the semesters numerically
        try:
            sorted_sem_cols = sorted(list(sem_cols), key=lambda x: int(x.split('-')[1]))
        except (ValueError, IndexError):
            # Handle cases where parsing fails
            sorted_sem_cols = sorted(list(sem_cols))
            
        return sorted_sem_cols

    def calculate_overall_status(self) -> pd.DataFrame:
        """
        Calculates the current overall pass/fail status for each student.
        It uses a combination of the 'result' column and subject-wise marks.
        """
        result_df = self.df.copy()

        # --- Enhanced Status Calculation ---

        # 1. Normalize subject marks to numeric, coercing errors
        numeric_df = result_df[self.subject_columns].copy()
        for col in numeric_df.columns:
            numeric_df[col] = pd.to_numeric(numeric_df[col], errors='coerce')

        # 2. A student fails if they have any mark below the threshold or a NaN value
        failed_by_marks = (numeric_df < self.pass_threshold).any(axis=1)
        failed_by_nan = numeric_df.isna().any(axis=1)

        # 3. Check for an explicit 'FAIL' in the 'result' column
        failed_by_result_col = pd.Series(False, index=result_df.index)
        if 'result' in result_df.columns:
            failed_by_result_col = result_df['result'].str.strip().str.upper() == 'FAIL'

        # 4. Combine all failure conditions
        is_failed = failed_by_marks | failed_by_nan | failed_by_result_col

        # 5. Determine final status
        result_df['overall status'] = np.where(is_failed, 'Fail', 'Pass')

        # --- End of Enhanced Calculation ---

        # Calculate passed/failed subject counts for information
        passed_count = (numeric_df >= self.pass_threshold).sum(axis=1)
        failed_count = (numeric_df < self.pass_threshold).sum(axis=1) + numeric_df.isna().sum(axis=1)

        result_df['passed subjects'] = passed_count
        result_df['failed subjects'] = failed_count
        
        return result_df

    def is_passed_all_semesters(self, row, upto_semester=None):
        """
        Strictly checks if a student has passed all semesters.
        A student must have a valid, non-failing SGPA for every semester
        column present in the data. An empty/NaN value means they have NOT
        passed all semesters.
        """
        sem_prefixes = self.get_sem_columns()
        if not sem_prefixes:
            return True # No semester columns, so trivially true

        sem_to_check = sem_prefixes
        if upto_semester:
            try:
                limit_sem_num = int(upto_semester.split('-')[1])
                sem_to_check = [s for s in sem_prefixes if int(s.split('-')[1]) <= limit_sem_num]
            except (ValueError, IndexError):
                pass # Ignore invalid upto_semester format

        for prefix in sem_to_check:
            sem_col = f'{prefix}'
            sem_seat_col = f'{prefix} seat'
            
            sem_val = row.get(sem_col)
            sem_seat_val = row.get(sem_seat_col)

            # At least one of the semester columns must have a value.
            # If both are empty/NaN, they haven't cleared this semester.
            if pd.isna(sem_val) and pd.isna(sem_seat_val):
                return False
            if str(sem_val).strip() == '' and str(sem_seat_val).strip() == '':
                 return False

            # Check for failure marks in any of the columns
            for value in [sem_val, sem_seat_val]:
                if pd.isna(value) or str(value).strip() == '':
                    continue

                value_str = str(value).strip().lower()
                if value_str.startswith('f-') or 'fail' in value_str:
                    return False
                try:
                    # Check for low SGPA
                    sgpa = float(value_str)
                    if sgpa < 4.0: # Assuming 4.0 is the minimum passing SGPA
                        return False
                except (ValueError, TypeError):
                    continue # Not a float, just continue checking
        
        return True

    def filter_passed_students(self, upto_semester=None) -> pd.DataFrame:
        """
        Filter students who passed in ALL semesters up to a limit.
        """
        passed_mask = self.df.apply(self.is_passed_all_semesters, axis=1, upto_semester=upto_semester)
        passed_df = self.df[passed_mask].copy()
        
        # Add a status for display purposes
        passed_df['overall status'] = 'Pass'
        
        return passed_df

    def filter_failed_students(self, upto_semester=None) -> pd.DataFrame:
        """
        Filter students who failed in AT LEAST ONE semester up to a limit.
        """
        # A student fails if they did NOT pass all semesters.
        failed_mask = ~self.df.apply(self.is_passed_all_semesters, axis=1, upto_semester=upto_semester)
        failed_df = self.df[failed_mask].copy()
        
        # Add a status for display purposes
        failed_df['overall status'] = 'Fail'
        
        return failed_df

    def _get_sem_status(self, row, sem_column_prefix):
        """
        Determines the pass/fail status for a single semester.
        """
        relevant_cols = [col for col in row.index if col.startswith(sem_column_prefix)]

        if not relevant_cols:
            return 'Fail'

        has_any_value = False
        for col_name in relevant_cols:
            value = row.get(col_name)
            if pd.notna(value) and str(value).strip() != '':
                has_any_value = True
                value_str = str(value).strip().lower()
                try:
                    sgpa = float(value_str)
                    if sgpa < 4.0:
                        return 'Fail'
                except (ValueError, TypeError):
                    if 'f-' in value_str or 'fail' in value_str:
                        return 'Fail'
        
        if not has_any_value:
            return 'Fail'

        return 'Pass'

    def filter_sem_pass(self, sem_column_prefix: str) -> pd.DataFrame:
        """
        Filter students who passed a specific semester.
        """
        df_copy = self.df.copy()
        status_col_name = f"status in {sem_column_prefix.replace('-', ' ').title()}"
        
        df_copy[status_col_name] = df_copy.apply(lambda row: self._get_sem_status(row, sem_column_prefix), axis=1)

        passed_in_sem_df = df_copy[df_copy[status_col_name] == 'Pass']
        
        display_cols = ['name', 'spid', status_col_name, sem_column_prefix, f"{sem_column_prefix} seat"]
        if 'name' not in passed_in_sem_df.columns:
            display_cols[0] = 'student name'

        display_cols = [col for col in display_cols if col in passed_in_sem_df.columns]
        return passed_in_sem_df[display_cols]


    def filter_sem_fail(self, sem_column_prefix: str) -> pd.DataFrame:
        """
        Filter students who failed a specific semester.
        """
        df_copy = self.df.copy()
        status_col_name = f"status in {sem_column_prefix.replace('-', ' ').title()}"
        
        df_copy[status_col_name] = df_copy.apply(lambda row: self._get_sem_status(row, sem_column_prefix), axis=1)

        failed_in_sem_df = df_copy[df_copy[status_col_name] == 'Fail']
        
        display_cols = ['name', 'spid', status_col_name, sem_column_prefix, f"{sem_column_prefix} seat"]
        if 'name' not in failed_in_sem_df.columns:
            display_cols[0] = 'student name'
            
        display_cols = [col for col in display_cols if col in failed_in_sem_df.columns]
        return failed_in_sem_df[display_cols]
    
    def get_overall_pass_fail(self) -> pd.DataFrame:
        df_with_status = self.calculate_overall_status()
        name_col = 'name' if 'name' in df_with_status.columns else 'student name'
        result_cols = [name_col, 'overall status', 'passed subjects', 'failed subjects']
        result_cols = [col for col in result_cols if col in df_with_status.columns]
        return df_with_status[result_cols]

    def filter_subject_wise_pass(self, subject: str) -> pd.DataFrame:
        subject = str(subject).lower()
        if subject not in self.df.columns:
            matching_subjects = [s for s in self.subject_columns if subject in s or s in subject]
            if not matching_subjects:
                raise ValueError(f"Subject '{subject}' not found in data")
            subject = matching_subjects[0]

        # Use a fresh copy to avoid carrying over status columns
        df_copy = self.df.copy()
        df_copy['status in ' + subject] = pd.to_numeric(df_copy[subject], errors='coerce').apply(
            lambda x: 'Pass' if pd.notna(x) and x >= self.pass_threshold else 'Fail'
        )
        filtered_df = df_copy[df_copy['status in ' + subject] == 'Pass']
        
        desired_order = ['name', 'spid', 'gender', subject, 'status in ' + subject]
        if 'name' not in filtered_df.columns:
            desired_order[0] = 'student name'
        result_cols = [col for col in desired_order if col in filtered_df.columns]
        return filtered_df[result_cols]

    def filter_subject_wise_fail(self, subject: str) -> pd.DataFrame:
        subject = str(subject).lower()
        if subject not in self.df.columns:
            matching_subjects = [s for s in self.subject_columns if subject in s]
            if not matching_subjects:
                raise ValueError(f"Subject '{subject}' not found in data")
            subject = matching_subjects[0]

        df_copy = self.df.copy()
        df_copy['status in ' + subject] = pd.to_numeric(df_copy[subject], errors='coerce').apply(
            lambda x: 'Pass' if pd.notna(x) and x >= self.pass_threshold else 'Fail'
        )
        filtered_df = df_copy[df_copy['status in ' + subject] == 'Fail']
        
        desired_order = ['name', 'spid', 'gender', subject, 'status in ' + subject]
        if 'name' not in filtered_df.columns:
            desired_order[0] = 'student name'
        result_cols = [col for col in desired_order if col in filtered_df.columns]
        return filtered_df[result_cols]

    def get_subject_wise_summary(self) -> pd.DataFrame:
        summary_data = []
        if not self.subject_columns:
            return pd.DataFrame(summary_data)
            
        for subject in self.subject_columns:
            numeric_col = pd.to_numeric(self.df[subject], errors='coerce')
            passed = (numeric_col >= self.pass_threshold).sum()
            failed = (numeric_col < self.pass_threshold).sum() + numeric_col.isna().sum()
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
        # Use the corrected status calculation for statistics
        df_with_status = self.calculate_overall_status()
        
        total_students = len(df_with_status)
        passed_students = len(df_with_status[df_with_status['overall status'] == 'Pass'])
        failed_students = total_students - passed_students
        pass_percentage = (passed_students / total_students * 100) if total_students > 0 else 0
        
        # Total from 'total' column if available. The last column named 'total'.
        total_cols = [col for col in self.df.columns if 'total' in col]
        
        avg_marks = 0
        highest_marks = 0
        lowest_marks = 0

        total_marks_col_name = None
        if 'final total,,total' in self.df.columns:
            total_marks_col_name = 'final total,,total'
        elif total_cols:
            total_marks_col_name = total_cols[-1]

        if total_marks_col_name:
            total_marks_col = pd.to_numeric(self.df[total_marks_col_name], errors='coerce')
            avg_marks = total_marks_col.mean()
            highest_marks = total_marks_col.max()
            lowest_marks = total_marks_col.min()
        else: 
            if self.subject_columns:
                marks_data = self.df[self.subject_columns].copy()
                for col in marks_data.columns:
                    marks_data[col] = pd.to_numeric(marks_data[col], errors='coerce')
                
                row_totals = marks_data.sum(axis=1, skipna=True)
                
                avg_marks = row_totals.mean()
                highest_marks = row_totals.max()
                lowest_marks = row_totals.min()

        return {
            'Total Students': total_students,
            'Passed Students': int(passed_students),
            'Failed Students': int(failed_students),
            'Pass Percentage': f"{pass_percentage:.2f}%",
            'Average Class Marks': f"{avg_marks:.2f}" if pd.notna(avg_marks) else "0.00",
            'Highest Marks': f"{highest_marks:.0f}" if pd.notna(highest_marks) else "0",
            'Lowest Marks': f"{lowest_marks:.0f}" if pd.notna(lowest_marks) else "0"
        }
