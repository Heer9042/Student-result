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
        
        # Convert all numeric columns to float, handle errors
        numeric_cols = df.columns[1:]  # All columns except the first (name)
        for col in numeric_cols:
            try:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            except Exception:
                pass
        
        # Validate that we have valid data
        if df.empty:
            return False, pd.DataFrame(), "CSV file is empty or invalid"
        
        # Rename the first column to 'Student Name' for consistency
        df.rename(columns={df.columns[0]: 'Student Name'}, inplace=True)
        
        return True, df, "CSV file loaded successfully"
        
    except FileNotFoundError:
        return False, pd.DataFrame(), f"File not found: {file_path}"
    except pd.errors.ParserError as e:
        return False, pd.DataFrame(), f"Error parsing CSV file: {str(e)}"
    except Exception as e:
        return False, pd.DataFrame(), f"Error reading CSV file: {str(e)}"


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
    
    # Validate marks are in range 0-100
    subject_cols = df.columns[1:]
    for col in subject_cols:
        # Skip non-numeric columns
        numeric_data = pd.to_numeric(df[col], errors='coerce')
        if numeric_data.notna().any():  # If there's any numeric data
            invalid_marks = (numeric_data < 0) | (numeric_data > 100)
            if invalid_marks.any():
                return False, f"Invalid marks found in {col}. Marks should be between 0-100"
    
    return True, "Data validation successful"
