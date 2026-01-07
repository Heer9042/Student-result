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
import re
import logging
from config import PASS_THRESHOLD

logger = logging.getLogger(__name__)


class StudentMarksProcessor:
    """
    A class to process and filter student marks data.
    """
    
    def __init__(self, 
                 dataframe: pd.DataFrame, 
                 pass_threshold: int = PASS_THRESHOLD, 
                 selected_subjects: List[str] = None,
                 practical_subjects: List[str] = None,
                 theory_subjects: List[str] = None):
        """
        Initialize the processor with a dataframe of student marks.
        
        Args:
            dataframe (pd.DataFrame): DataFrame with Student Name and subject marks
            pass_threshold (int): Marks required to pass (default: 40)
            selected_subjects (List[str], optional): If provided, use this list of subjects.
            practical_subjects (List[str], optional): List of practical subjects.
            theory_subjects (List[str], optional): List of theory subjects.
        """
        self.df = dataframe.copy()
        # Normalize column names to lowercase for consistency
        self.df.columns = [str(col).lower() for col in self.df.columns]
        self.pass_threshold = pass_threshold
        
        self.practical_columns = [s.lower() for s in (practical_subjects or []) if s.lower() in self.df.columns]
        self.theory_columns = [s.lower() for s in (theory_subjects or []) if s.lower() in self.df.columns]
        
        if selected_subjects is not None:
            self.subject_columns = [s.lower() for s in selected_subjects if s.lower() in self.df.columns]
        elif self.practical_columns or self.theory_columns:
            self.subject_columns = list(set(self.practical_columns + self.theory_columns))
        else:
            self.subject_columns = self._get_subject_columns()

    def _get_subject_columns(self) -> List[str]:
        """
        Get list of subject columns (excludes student metadata columns).
        Prioritizes 'TOTAL' columns to avoid cluttering with internal/external components.
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
        
        potential_cols = []
        for col in self.df.columns:
            # Skip metadata columns
            if col in metadata_columns:
                continue
            # Skip columns matching exclude patterns
            if any(pattern in col for pattern in exclude_patterns):
                continue
            potential_cols.append(col)
        
        # Group SUBn_ columns to show only the TOTAL or main column
        sub_groups = {}
        for col in potential_cols:
            match = re.match(r'^(sub\d+)', col.lower())
            if match:
                base = match.group(1)
                if base not in sub_groups:
                    sub_groups[base] = []
                sub_groups[base].append(col)
            else:
                # Keep non-patterned columns as-is
                sub_groups[col] = [col]
        
        subject_cols = []
        for base, cols in sub_groups.items():
            # If it's a SUBn group, try to find the TOTAL column
            if base.startswith('sub'):
                total_col = next((c for c in cols if 'total' in c.lower()), cols[0])
                subject_cols.append(total_col)
            else:
                subject_cols.extend(cols)
        
        return sorted(list(set(subject_cols)))


    def get_sem_columns(self) -> List[str]:
        """
        Get a unique, sorted list of semester column prefixes available in the dataframe.
        Supports 'sem-1', 'sem1_sgpa', 'sem 1', etc.
        """
        import re
        sem_prefixes = set()
        for col in self.df.columns:
            # Look for patterns like 'sem-1', 'sem1', 'sem 1'
            match = re.search(r'sem[-_ ]?(\d+)', str(col).lower())
            if match:
                sem_num = match.group(1)
                # Keep the format that appears in the column name if possible
                if f'sem-{sem_num}' in str(col).lower():
                    sem_prefixes.add(f'sem-{sem_num}')
                elif f'sem{sem_num}' in str(col).lower():
                    sem_prefixes.add(f'sem{sem_num}')
                else:
                    sem_prefixes.add(f'sem-{sem_num}')
        
        # Sort the semesters numerically
        try:
            sorted_sem_cols = sorted(list(sem_prefixes), key=lambda x: int(re.search(r'(\d+)', x).group(1)))
        except (ValueError, IndexError, AttributeError):
            sorted_sem_cols = sorted(list(sem_prefixes))
            
        return sorted_sem_cols

    def _get_subject_status(self, subject_col: str) -> pd.Series:
        """
        Determines pass/fail status for a subject column.
        Returns a boolean series (True for Pass, False for Fail).
        """
        subject_col = str(subject_col).lower()
        # Find grade column
        grade_col = None
        base_match = re.match(r'^(sub\d+)', subject_col)
        if base_match:
            base = base_match.group(1)
            potential = f"{base}_grade"
            if potential in self.df.columns:
                grade_col = potential
        
        if grade_col:
            # Use grade column logic (Handles dynamic thresholds like 18/50 vs 40/100)
            # A student fails if grade contains 'Fail' or 'F-' or is missing
            is_fail = self.df[grade_col].astype(str).str.contains('Fail|F-|NA|NaN', case=False, na=True) | self.df[grade_col].isna()
            return ~is_fail
        else:
            # Fallback to threshold
            numeric_col = pd.to_numeric(self.df[subject_col], errors='coerce')
            return (numeric_col >= self.pass_threshold) & numeric_col.notna()

    def calculate_overall_status(self) -> pd.DataFrame:
        """
        Calculates the current overall pass/fail status for each student.
        Prioritizes the 'RESULT' column if it exists.
        """
        result_df = self.df.copy()

        # Check for explicit result columns
        result_col = None
        for col_name in ['result', 'overall status', 'status']:
            if col_name in result_df.columns:
                result_col = col_name
                break

        if result_col:
            # User rule: If there is data inside it means Pass, else Fail. 
            # We also check for the explicit string 'FAIL'.
            result_df['overall status'] = result_df[result_col].apply(
                lambda x: 'Fail' if pd.isna(x) or str(x).strip() == '' or str(x).strip().upper() in ['FAIL', 'NAN'] else 'Pass'
            )
        else:
            # Fallback to marks-based logic
            # Correctly handle dynamic thresholds per subject
            all_pass = True
            for col in self.subject_columns:
                is_pass = self._get_subject_status(col)
                if all_pass is True:
                    all_pass = is_pass
                else:
                    all_pass = all_pass & is_pass
            
            result_df['overall status'] = np.where(all_pass, 'Pass', 'Fail')

        # Standard calculation for subject counts
        passed_count = pd.Series(0, index=result_df.index)
        failed_count = pd.Series(0, index=result_df.index)
        
        for col in self.subject_columns:
            is_pass = self._get_subject_status(col)
            passed_count += is_pass.astype(int)
            failed_count += (~is_pass).astype(int)

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
            # Find all columns for this semester
            relevant_cols = [col for col in self.df.columns if col.startswith(prefix)]
            
            has_value = False
            for col in relevant_cols:
                value = row.get(col)
                if pd.notna(value) and str(value).strip() != '':
                    # User rule: data exists means pass.
                    # We only fail if it explicitly says 'FAIL'.
                    val_str = str(value).strip().upper()
                    if val_str == 'FAIL' or val_str.startswith('F-'):
                        return False
                    has_value = True
            
            # If a student is missing data for a semester that we are checking, 
            # we consider them as not having passed ALL.
            if not has_value:
                # SPECIAL CASE: In many results, older semesters might be empty if 
                # they were direct entries. We only count it as a fail if 
                # there's a LATER semester that has data.
                later_sems_have_data = False
                current_sem_num = int(re.search(r'(\d+)', prefix).group(1))
                for other_prefix in sem_prefixes:
                    other_num = int(re.search(r'(\d+)', other_prefix).group(1))
                    if other_num > current_sem_num:
                        other_cols = [c for c in self.df.columns if c.startswith(other_prefix)]
                        if any(pd.notna(row.get(oc)) and str(row.get(oc)).strip() != '' for oc in other_cols):
                            later_sems_have_data = True
                            break
                
                if later_sems_have_data:
                    return False
        
        return True


    def filter_passed_students(self, upto_semester=None) -> pd.DataFrame:
        """
        Filter students who passed in ALL semesters (including the current one).
        """
        df_with_status = self.calculate_overall_status()
        
        # Check current status AND historical semester status
        current_pass = df_with_status['overall status'] == 'Pass'
        sem_pass = df_with_status.apply(self.is_passed_all_semesters, axis=1, upto_semester=upto_semester)
        
        passed_mask = current_pass & sem_pass
        passed_df = df_with_status[passed_mask].copy()
        
        # Add a status for display purposes
        passed_df['overall status'] = 'Pass'
        
        # Show specific columns requested by user: SPID, name, id, seatno
        sem_prefixes = self.get_sem_columns()
        if upto_semester:
            try:
                limit_sem_num = int(re.search(r'(\d+)', upto_semester).group(1))
                sem_prefixes = [s for s in sem_prefixes if int(re.search(r'(\d+)', s).group(1)) <= limit_sem_num]
            except (ValueError, IndexError, AttributeError):
                pass

        sem_cols = []
        for p in sem_prefixes:
            sem_cols.extend([c for c in passed_df.columns if c.startswith(p)])
        
        # Mapping possible names to standard display names
        name_col = next((c for c in ['name', 'student name'] if c in passed_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in passed_df.columns), 'spid')
        seatno_col = next((c for c in ['seatno', 'seat no'] if c in passed_df.columns), 'seatno')
        id_col = next((c for c in ['id', 'student id'] if c in passed_df.columns), 'id')
        
        cols_to_show = [spid_col, name_col, id_col, seatno_col, 'overall status'] + sorted(list(set(sem_cols)))
        cols_to_show = [c for c in cols_to_show if c in passed_df.columns]
        
        return passed_df[cols_to_show]

    def filter_failed_students(self, upto_semester=None) -> pd.DataFrame:
        """
        Filter students who failed in AT LEAST ONE semester (including current).
        Shows only the semesters where a failure actually occurred for at least one student in the list.
        """
        df_with_status = self.calculate_overall_status()
        
        # A student fails if they did NOT pass current OR did NOT pass historical
        current_pass = df_with_status['overall status'] == 'Pass'
        sem_pass = df_with_status.apply(self.is_passed_all_semesters, axis=1, upto_semester=upto_semester)
        
        failed_mask = ~(current_pass & sem_pass)
        failed_df = df_with_status[failed_mask].copy()
        
        # User requested fields: id, name, spid, and the failed sem
        name_col = next((c for c in ['name', 'student name'] if c in failed_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in failed_df.columns), 'spid')
        id_col = next((c for c in ['id', 'student id', 'enrollment no', 'enrollment no.'] if c in failed_df.columns), 'id')
        
        # Identification columns in requested order
        base_cols = [id_col, name_col, spid_col]
        
        # Find all semesters that have a failure in this student list
        sem_prefixes = self.get_sem_columns()
        if upto_semester:
            try:
                limit_sem_num = int(re.search(r'(\d+)', upto_semester).group(1))
                sem_prefixes = [s for s in sem_prefixes if int(re.search(r'(\d+)', s).group(1)) <= limit_sem_num]
            except (ValueError, IndexError, AttributeError):
                pass

        failed_sem_cols = []
        for prefix in sem_prefixes:
            # Check if any student in the failed list actually has a failure in THIS specific semester
            has_fail = failed_df.apply(lambda row: self._get_sem_status(row, prefix) == 'Fail', axis=1).any()
            if has_fail:
                failed_sem_cols.extend([c for c in failed_df.columns if c.startswith(prefix)])
        
        # Include current result columns if current status is 'Fail'
        if (failed_df['overall status'] == 'Fail').any():
            # If current semester result exists, and it's a fail, the 'overall status' or marks would show it.
            # We'll include the 'overall status' at the end for clarity of the 'current' failure.
            base_cols.append('overall status')

        cols_to_show = base_cols + sorted(list(set(failed_sem_cols)))
        cols_to_show = [c for c in cols_to_show if c in failed_df.columns]
        
        return failed_df[cols_to_show]

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
                # Data exists rule
                val_str = str(value).strip().upper()
                if val_str == 'FAIL' or val_str.startswith('F-'):
                    return 'Fail'
                has_any_value = True
        
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
        passed_in_sem_df = df_copy[df_copy[status_col_name] == 'Pass'].copy()
        
        name_col = next((c for c in ['name', 'student name'] if c in passed_in_sem_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in passed_in_sem_df.columns), 'spid')
        id_col = next((c for c in ['id', 'student id', 'enrollment no', 'enrollment no.'] if c in passed_in_sem_df.columns), 'id')
        
        # Identification columns + the specific semester columns
        sem_cols = [c for c in passed_in_sem_df.columns if c.startswith(sem_column_prefix)]
        
        cols_to_show = [id_col, name_col, spid_col, status_col_name] + sorted(sem_cols)
        cols_to_show = [c for c in cols_to_show if c in passed_in_sem_df.columns]
        
        return passed_in_sem_df[cols_to_show]


    def filter_sem_fail(self, sem_column_prefix: str) -> pd.DataFrame:
        """
        Filter students who failed a specific semester.
        """
        df_copy = self.df.copy()
        status_col_name = f"status in {sem_column_prefix.replace('-', ' ').title()}"
        
        df_copy[status_col_name] = df_copy.apply(lambda row: self._get_sem_status(row, sem_column_prefix), axis=1)
        failed_in_sem_df = df_copy[df_copy[status_col_name] == 'Fail'].copy()
        
        name_col = next((c for c in ['name', 'student name'] if c in failed_in_sem_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in failed_in_sem_df.columns), 'spid')
        id_col = next((c for c in ['id', 'student id', 'enrollment no', 'enrollment no.'] if c in failed_in_sem_df.columns), 'id')
        
        # Identification columns + the specific semester columns
        sem_cols = [c for c in failed_in_sem_df.columns if c.startswith(sem_column_prefix)]
        
        cols_to_show = [id_col, name_col, spid_col, status_col_name] + sorted(sem_cols)
        cols_to_show = [c for c in cols_to_show if c in failed_in_sem_df.columns]
        
        return failed_in_sem_df[cols_to_show]
    
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
        is_pass = self._get_subject_status(subject)
        df_copy['status in ' + subject] = np.where(is_pass, 'Pass', 'Fail')
        filtered_df = df_copy[is_pass].copy()
        
        # Find related columns (Practical, Theory, etc.)
        match = re.match(r'^(sub\d+)', subject)
        base = match.group(1) if match else subject.split('_')[0] if '_' in subject else subject
        
        related_cols = [c for c in self.df.columns if c.startswith(base) and 'grade' not in c.lower()]
        if not related_cols:
            related_cols = [subject]

        name_col = next((c for c in ['name', 'student name'] if c in filtered_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in filtered_df.columns), 'spid')
        
        desired_order = [name_col, spid_col, 'gender'] + sorted(list(set(related_cols))) + ['status in ' + subject]
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
        is_pass = self._get_subject_status(subject)
        df_copy['status in ' + subject] = np.where(is_pass, 'Pass', 'Fail')
        filtered_df = df_copy[~is_pass].copy()
        
        # Find related columns (Practical, Theory, etc.)
        match = re.match(r'^(sub\d+)', subject)
        base = match.group(1) if match else subject.split('_')[0] if '_' in subject else subject
        
        related_cols = [c for c in self.df.columns if c.startswith(base) and 'grade' not in c.lower()]
        if not related_cols:
            related_cols = [subject]

        name_col = next((c for c in ['name', 'student name'] if c in filtered_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in filtered_df.columns), 'spid')
        
        result_cols = [col for col in desired_order if col in filtered_df.columns]
        return filtered_df[result_cols]

    def filter_by_type(self, sub_type: str, status: str) -> pd.DataFrame:
        """
        Filters students who passed all or failed any subjects of a specific type.
        sub_type: 'practical' or 'theory'
        status: 'pass' (passed all) or 'fail' (failed at least one)
        """
        cols = self.practical_columns if sub_type == 'practical' else self.theory_columns
        if not cols:
            return pd.DataFrame()
            
        df_copy = self.df.copy()
        
        # Determine status for each subject in the group
        status_series = []
        for col in cols:
            is_pass = self._get_subject_status(col)
            status_series.append(is_pass)
            
        if not status_series:
            return pd.DataFrame()
            
        # combined_pass is True only if student passed ALL subjects in this category
        combined_pass = pd.concat(status_series, axis=1).all(axis=1)
        
        if status == 'pass':
            filtered_df = df_copy[combined_pass].copy()
        else:
            filtered_df = df_copy[~combined_pass].copy()
            
        name_col = next((c for c in ['name', 'student name'] if c in filtered_df.columns), 'student name')
        spid_col = next((c for c in ['spid', 'sp id'] if c in filtered_df.columns), 'spid')
        
        # Output columns: IDs + all subjects of that type
        desired_order = [name_col, spid_col, 'gender'] + sorted(list(set(cols)))
        result_cols = [col for col in desired_order if col in filtered_df.columns]
        return filtered_df[result_cols]

    def get_subject_wise_summary(self) -> pd.DataFrame:
        summary_data = []
        if not self.subject_columns:
            return pd.DataFrame(summary_data)
            
        for subject in self.subject_columns:
            is_pass = self._get_subject_status(subject)
            passed = is_pass.sum()
            failed = (~is_pass).sum()
            total = len(self.df)
            pass_percentage = (passed / total * 100) if total > 0 else 0
            fail_percentage = (failed / total * 100) if total > 0 else 0
            
            sub_type = "Practical" if subject in self.practical_columns else "Theory" if subject in self.theory_columns else "General"
            
            summary_data.append({
                'Subject': subject.upper(),
                'Type': sub_type,
                'Total Students': int(total),
                'Passed': int(passed),
                'Failed': int(failed),
                'Pass %': f"{pass_percentage:.2f}%",
                'Fail %': f"{fail_percentage:.2f}%"
            })
        
        # Add Overall Statistics as a row
        overall_stats = self.calculate_overall_status()
        passed_overall = (overall_stats['overall status'] == 'Pass').sum()
        failed_overall = len(self.df) - passed_overall
        summary_data.append({
            'Subject': 'OVERALL CLASS PERFORMANCE',
            'Type': 'SUMMARY',
            'Total Students': int(len(self.df)),
            'Passed': int(passed_overall),
            'Failed': int(failed_overall),
            'Pass %': f"{(passed_overall / len(self.df) * 100):.2f}%",
            'Fail %': f"{(failed_overall / len(self.df) * 100):.2f}%"
        })
        
        return pd.DataFrame(summary_data)

    def get_overall_statistics(self) -> Dict[str, any]:
        # Use the corrected status calculation for statistics
        df_with_status = self.calculate_overall_status()
        
        total_students = len(df_with_status)
        passed_students = len(df_with_status[df_with_status['overall status'] == 'Pass'])
        failed_students = total_students - passed_students
        pass_percentage = (passed_students / total_students * 100) if total_students > 0 else 0
        
        total_cols = [col for col in self.df.columns if 'total' in col]
        
        avg_marks = 0
        highest_marks = 0
        lowest_marks = 0

        # Priority for total marks statistics
        total_marks_col_name = None
        for c in ['grand_total', 'grand total', 'final total,,total', 'total marks']:
            if c in self.df.columns:
                total_marks_col_name = c
                break
        
        if not total_marks_col_name:
            if total_cols:
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
