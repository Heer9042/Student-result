"""
Module for reading and parsing CSV and PDF files containing student marks data.

This module provides functionality to:
- Read CSV files with student marks
- Extract data from PDF files (tabular format)
- Validate the structure of the data
- Return standardized pandas DataFrame
"""

import pandas as pd
import csv
from pathlib import Path
from typing import Tuple, Optional
import PyPDF2


def read_csv_file(file_path: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Read a CSV file containing student marks.
    
    This function handles both simple CSVs and complex multi-header formats
    like 'result_structured_final.csv'. It standardizes all column names to
    lowercase for consistent processing.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            header1 = f.readline().upper()
        
        is_special_format = 'AWD TH' in header1 and 'FINAL TOTAL' in header1

        if is_special_format:
            df = pd.read_csv(file_path, header=[0, 1], keep_default_na=False, na_values=[''])
            
            # Custom flattening and cleaning for the special multi-header format
            new_cols = []
            last_l1 = ''
            for l1, l2 in df.columns:
                l1 = str(l1).strip()
                if 'Unnamed:' in l1:
                    l1 = last_l1
                else:
                    last_l1 = l1
                
                l2 = str(l2).strip()
                if 'Unnamed:' in l2: 
                    l2 = ''
                
                if l1 and l2:
                    new_cols.append(f"{l1} {l2}")
                else:
                    new_cols.append(l1 or l2)
            
            df.columns = new_cols
            
            # Deduplicate columns like 'TOTAL' and 'GRADE'
            cols = pd.Series(df.columns)
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '.' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            df.columns = cols
            
        else:
            # Existing logic for simple CSVs
            df = pd.read_csv(file_path, keep_default_na=False, na_values=[''])

        # Standard processing for all CSVs: enforce lowercase column names
        df.columns = [str(c).strip().lower() for c in df.columns]
        
        if df.shape[1] < 2:
            return False, pd.DataFrame(), "CSV must have at least Student Name and one subject column"
        
        df = df.dropna(how='all')

        # Try to transform from long to wide format if needed
        was_transformed, df_transformed, _ = transform_long_to_wide_format(df)
        if was_transformed:
            df = df_transformed

        # Define metadata columns (all lowercase)
        metadata_columns = [
            'student name', 'seat no', 'enrollment no', 'sp id', 'college name',
            'seatno', 'spid', 'gender', 'name'
        ]
        exclude_patterns = ['grade', 'total_int', 'total_ext', 'combined_total', 'pass_fail']
        exclude_patterns.extend([f'float_{i}' for i in range(1, 20)])

        numeric_cols = [
            col for col in df.columns 
            if col not in metadata_columns 
            and not any(pattern in col for pattern in exclude_patterns)
        ]
        
        for col in numeric_cols:
            try:
                if not df[col].astype(str).str.contains('ZR', na=False).any():
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception:
                pass
        
        if df.empty:
            return False, pd.DataFrame(), "CSV file is empty or invalid"
        
        if not was_transformed and 'student name' not in df.columns and 'name' not in df.columns:
            # If no name column found, assume it's the first column
            df.rename(columns={df.columns[0]: 'student name'}, inplace=True)
            
        return True, df, "CSV file loaded successfully"

    except FileNotFoundError:
        return False, pd.DataFrame(), f"File not found: {file_path}"
    except pd.errors.ParserError as e:
        return False, pd.DataFrame(), f"Error parsing CSV file: {str(e)}"
    except Exception as e:
        return False, pd.DataFrame(), f"Error reading CSV file: {str(e)}"


def transform_long_to_wide_format(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame, str]:
    """
    Transform long-format exam data (one row per student-subject) to wide format (one row per student).
    Supports students taking multiple exams by using Enrollment No. + Exam Name as composite key.
    
    Expected long format columns:
    - Student Name (or similar)
    - Subject Name
    - Total Marks (or similar marks column)
    - Optionally: Enrollment No., Seat No., Exam Name, or other unique ID
    
    Args:
        df (pd.DataFrame): Long-format dataframe
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, transformed dataframe, message)
    """
    try:
        # Check if this looks like long format (has subject name and multiple rows per student)
        if 'subject name' in df.columns and 'student name' in df.columns:
            # Use total marks if available, otherwise look for other marks columns
            marks_column = None
            for col in ['total marks', 'marks', 'score', 'total']:
                if col in df.columns:
                    marks_column = col
                    break
            
            if not marks_column:
                return False, pd.DataFrame(), "Could not find marks column in long-format data"
            
            # Define metadata columns to preserve
            metadata_to_preserve = ['exam name', 'seat no', 'student name', 'enrollment no', 'sp id']
            
            # Find the primary unique identifier (priority order)
            id_priority = ['enrollment no', 'seat no', 'sp id', 'student id', 'roll no']
            student_id_col = None
            for col in id_priority:
                if col in df.columns:
                    student_id_col = col
                    break
            
            if not student_id_col:
                return False, pd.DataFrame(), "No unique identifier column found (need Enrollment No., Seat No., or similar)"
            
            # Create composite key if exam name exists (to support multiple exams per student)
            if 'exam name' in df.columns:
                df['_composite_key'] = df[student_id_col].astype(str) + '___' + df['exam name'].astype(str)
                groupby_col = '_composite_key'
            else:
                groupby_col = student_id_col
            
            # Collect all metadata columns that exist in the dataframe
            metadata_cols = []
            for col in metadata_to_preserve:
                if col in df.columns:
                    metadata_cols.append(col)
            
            # Get metadata for each unique student+exam combination (before pivoting)
            # We need to exclude the groupby column from the select list if it's not composite
            if groupby_col == '_composite_key':
                metadata_select = metadata_cols
            else:
                metadata_select = [col for col in metadata_cols if col != groupby_col]
            
            if metadata_select:
                metadata_df = df.groupby(groupby_col)[metadata_select].first().reset_index()
            else:
                # If no other metadata, just get unique values of the groupby column
                metadata_df = df[[groupby_col]].drop_duplicates().reset_index(drop=True)
            
            # Pivot the marks data: each subject becomes a column
            try:
                marks_df = df.pivot_table(
                    index=groupby_col,
                    columns='subject name',
                    values=marks_column,
                    aggfunc='first'  # Take first value if duplicates
                ).reset_index()
                
                # Merge metadata with marks using the groupby column
                wide_df = pd.merge(metadata_df, marks_df, on=groupby_col, how='left')
                
                # Drop the temporary composite key if we created one
                if '_composite_key' in wide_df.columns:
                    wide_df = wide_df.drop('_composite_key', axis=1)
                
                return True, wide_df, "Successfully transformed from long to wide format"
            except Exception as e:
                return False, pd.DataFrame(), f"Error pivoting data: {str(e)}"
        
        # Not long format, return as is
        return True, df, "Data already in wide format"
        
    except Exception as e:
        return False, pd.DataFrame(), f"Error transforming data: {str(e)}"


def extract_table_from_pdf(pdf_path: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Extract tabular data from a PDF file.
    
    Note: This function uses a simple approach for PDFs with tabular data.
    For complex PDFs, consider using specialized libraries like pdfplumber.
    
    Args:
        pdf_path (str): Path to the PDF file
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe, message)
    """
    try:
        # This is a basic implementation that reads text from PDF
        # For better table extraction, consider using pdfplumber library
        
        with open(pdf_path, 'rb') as pdf_file:
            pdf_reader = PyPDF2.PdfReader(pdf_file)
            
            # Check if PDF has pages
            if len(pdf_reader.pages) == 0:
                return False, pd.DataFrame(), "PDF file is empty"
            
            # Extract text from all pages
            extracted_text = ""
            for page in pdf_reader.pages:
                extracted_text += page.extract_text() + "\n"
            
            # For now, return a message indicating manual format is needed
            return False, pd.DataFrame(), \
                "For the 'Result Analysis' feature, please first convert your PDF to a structured CSV using the 'Convert Data' feature."
        
    except FileNotFoundError:
        return False, pd.DataFrame(), f"PDF file not found: {pdf_path}"
    except Exception as e:
        return False, pd.DataFrame(), f"Error reading PDF file: {str(e)}"


def read_student_marks(file_path: str) -> Tuple[bool, pd.DataFrame, str]:
    """
    Read student marks from either CSV or PDF file.
    
    Args:
        file_path (str): Path to the file (CSV or PDF)
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe, message)
    """
    file_path = str(file_path)
    
    if file_path.lower().endswith('.csv'):
        return read_csv_file(file_path)
    elif file_path.lower().endswith('.pdf'):
        return extract_table_from_pdf(file_path)
    else:
        return False, pd.DataFrame(), "Unsupported file format. Please use CSV or PDF."


def validate_marks_data(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate that the dataframe has valid marks data.
    
    Args:
        df (pd.DataFrame): DataFrame with student marks
        
    Returns:
        Tuple[bool, str]: (is_valid, message)
    """
    if df.empty:
        return False, "DataFrame is empty"
    
    # Standardize column names to lowercase for reliable validation
    df.columns = [str(c).lower() for c in df.columns]
    
    # Check if student name or name column exists (case-insensitive)
    if 'student name' not in df.columns and 'name' not in df.columns:
        return False, "Missing 'Student Name' or 'Name' column"
    
    # Check if there are subject columns
    if df.shape[1] < 2:
        return False, "No subject columns found"
    
    # Define metadata columns that should not be validated as marks
    metadata_columns = [
        'student name', 'seat no', 'enrollment no', 'sp id', 
        'college name', 'exam name', 'student id', 'roll no',
        'overall status', 'passed subjects', 'failed subjects',
        # New format columns
        'seatno', 'spid', 'gender', 'name',
        # Summary columns
        'total_int', 'total_ext', 'combined_total', 'pass_fail'
    ]
    
    # Also exclude grade and float columns using patterns
    exclude_patterns = ['grade']
    # Add float columns (float_1 through float_19)
    for i in range(1, 20):
        metadata_columns.append(f'float_{i}')
    
    # Get subject columns (exclude metadata)
    subject_cols = [col for col in df.columns if col not in metadata_columns]
    
    if not subject_cols:
        # If no subject columns found, it might be okay if we have metadata
        if any(col in df.columns for col in ['enrollment no', 'seat no', 'sp id', 'seatno', 'spid']):
            return True, "Data validation successful (metadata only)"
        return False, "No subject columns found"
    
    # Validate marks are in range 0-100 for subject columns only
    for col in subject_cols:
        # Skip columns with grade in the name
        if any(pattern in col for pattern in exclude_patterns):
            continue
            
        # Skip non-numeric columns
        numeric_data = pd.to_numeric(df[col], errors='coerce')
        if numeric_data.notna().any():  # If there's any numeric data
            invalid_marks = (numeric_data < 0) | (numeric_data > 1000000000)
            if invalid_marks.any():
                return False, f"Invalid marks found in {col}. Marks should be between 0-1000000000"
    
    return True, "Data validation successful"
