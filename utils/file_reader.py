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
    
    Expected CSV format:
    Student Name, Subject1, Subject2, Subject3, ...
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        Tuple[bool, pd.DataFrame, str]: (success, dataframe, message)
            - success: Boolean indicating if file was read successfully
            - dataframe: pandas DataFrame with student data or empty if failed
            - message: Success or error message
    """
    try:
        # Read CSV file with student name as first column
        df = pd.read_csv(file_path)
        
        # Validate that the file has at least 2 columns (name + at least 1 subject)
        if df.shape[1] < 2:
            return False, pd.DataFrame(), "CSV must have at least Student Name and one subject column"
        
        # Validate that first column exists (assumed to be student name)
        if not df.columns[0]:
            return False, pd.DataFrame(), "First column should be Student Name"
        
        # Clean column names (remove extra spaces)
        df.columns = df.columns.str.strip()
        
        # Remove any completely empty rows
        df = df.dropna(how='all')
        
        # Try to transform from long to wide format if needed
        was_transformed = False
        success, transformed_df, transform_msg = transform_long_to_wide_format(df)
        if success and not transformed_df.empty:
            df = transformed_df
            was_transformed = True
        
        # Convert all numeric columns to float, handle errors
        # Skip metadata columns that should not be converted
        metadata_columns = ['Student Name', 'Seat No.', 'Enrollment No.', 'SP ID', 'College Name']
        numeric_cols = [col for col in df.columns if col not in metadata_columns]
        for col in numeric_cols:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception:
                pass
        
        # Validate that we have valid data
        if df.empty:
            return False, pd.DataFrame(), "CSV file is empty or invalid"
        
        # Rename the first column to 'Student Name' for consistency (only if not transformed)
        if not was_transformed and 'Student Name' not in df.columns:
            df.rename(columns={df.columns[0]: 'Student Name'}, inplace=True)
        
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
        # Check if this looks like long format (has Subject Name and multiple rows per student)
        if 'Subject Name' in df.columns and 'Student Name' in df.columns:
            # Use Total Marks if available, otherwise look for other marks columns
            marks_column = None
            for col in ['Total Marks', 'Marks', 'Score', 'Total']:
                if col in df.columns:
                    marks_column = col
                    break
            
            if not marks_column:
                return False, pd.DataFrame(), "Could not find marks column in long-format data"
            
            # Define metadata columns to preserve
            metadata_to_preserve = ['Exam Name', 'Seat No.', 'Student Name', 'Enrollment No.', 'SP ID']
            
            # Find the primary unique identifier (priority order)
            id_priority = ['Enrollment No.', 'Seat No.', 'SP ID', 'Student ID', 'Roll No.']
            student_id_col = None
            for col in id_priority:
                if col in df.columns:
                    student_id_col = col
                    break
            
            if not student_id_col:
                return False, pd.DataFrame(), "No unique identifier column found (need Enrollment No., Seat No., or similar)"
            
            # Create composite key if Exam Name exists (to support multiple exams per student)
            if 'Exam Name' in df.columns:
                df['_composite_key'] = df[student_id_col].astype(str) + '___' + df['Exam Name'].astype(str)
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
                    columns='Subject Name',
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
                "PDF extraction not fully implemented. Please use CSV format or provide CSV export of your PDF data."
        
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
    
    # Check if Student Name column exists
    if 'Student Name' not in df.columns:
        return False, "Missing 'Student Name' column"
    
    # Check if there are subject columns
    if df.shape[1] < 2:
        return False, "No subject columns found"
    
    # Define metadata columns that should not be validated as marks
    metadata_columns = [
        'Student Name', 'Seat No.', 'Enrollment No.', 'SP ID', 
        'College Name', 'Exam Name', 'Student ID', 'Roll No.',
        'Overall Status', 'Passed Subjects', 'Failed Subjects'
    ]
    
    # Get subject columns (exclude metadata)
    subject_cols = [col for col in df.columns if col not in metadata_columns]
    
    if not subject_cols:
        # If no subject columns found, it might be okay if we have metadata
        if any(col in df.columns for col in ['Enrollment No.', 'Seat No.', 'SP ID']):
            return True, "Data validation successful (metadata only)"
        return False, "No subject columns found"
    
    # Validate marks are in range 0-100 for subject columns only
    for col in subject_cols:
        # Skip non-numeric columns
        numeric_data = pd.to_numeric(df[col], errors='coerce')
        if numeric_data.notna().any():  # If there's any numeric data
            invalid_marks = (numeric_data < 0) | (numeric_data > 100)
            if invalid_marks.any():
                return False, f"Invalid marks found in {col}. Marks should be between 0-100"
    
    return True, "Data validation successful"
