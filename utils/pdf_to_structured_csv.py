import logging
import os
import re
import sys
from typing import List, Optional, Tuple, Dict, Any

# Third-party imports with check
try:
    import pandas as pd
    import pdfplumber
except ImportError as e:
    logging.error(f"Missing Dependency: {e}")
    raise ImportError(f"Error importing required libraries: {e}\nPlease ensure 'pandas' and 'pdfplumber' are installed.\nTry running: pip install pandas pdfplumber openpyxl")

# Configure logging
logger = logging.getLogger(__name__)

class ResultParser:
    """
    Parses student result PDFs and converts them to structured data.
    Now optimized for web-based streaming updates.
    """
    def __init__(self, total_subjects: int, practical_subjects: int, metadata_df: Optional[pd.DataFrame] = None):
        self.total_subjects = total_subjects
        self.practical_subjects = practical_subjects
        self.theory_subjects = total_subjects - practical_subjects
        self.metadata_df = metadata_df

    @staticmethod
    def normalize(v: Optional[str]) -> str:
        """Normalizes a string value, handling None and 'Zero'."""
        if not v:
            return "0"
        v = v.strip()
        if v.upper() == "ZERO":
            return "0"
        return v

    @staticmethod
    def to_numeric(v: str) -> int:
        """Converts a value to an integer, handling '+' notation for split marks."""
        v = ResultParser.normalize(v)
        if "+" in v:
            try:
                return sum(int(x) for x in v.split("+") if x.strip().isdigit())
            except ValueError:
                return 0
        return int(v) if v.isdigit() else 0

    @staticmethod
    def get_grade(marks: int, max_marks: int = 100) -> str:
        """Determines the grade based on marks and max marks."""
        if max_marks == 50:
            if marks >= 35: return "Distinction"
            if marks >= 30: return "First"
            if marks >= 25: return "Second"
            if marks >= 18: return "Pass"
            return "Fail"
        else:
            if marks >= 70: return "Distinction"
            if marks >= 60: return "First"
            if marks >= 50: return "Second"
            if marks >= 35: return "Pass"
            return "Fail"

    @staticmethod
    def clean_section_tokens(tokens: List[str]) -> List[str]:
        """Merges tokens that are split by spaces around special characters like '+'."""
        if not tokens: return []
        raw_text = " ".join(tokens)
        cleaned_text = re.sub(r'\s*\+\s*', '+', raw_text)
        return cleaned_text.split()

    def parse_student_block(self, block: List[str]) -> Optional[List[Any]]:
        """Parses a single student's block of text lines into a row of data."""
        text = " ".join(block)
        tokens = text.split()
        if len(tokens) < 4:
            return None

        # Basic Info
        seat_no = tokens[0]
        spid = tokens[1]
        gender = tokens[2]

        indices = {"EXT": -1, "INT": -1, "TOTAL": -1}
        for kw in indices.keys():
            try:
                indices[kw] = tokens.index(kw)
            except ValueError:
                pass
        
        sorted_indices = sorted([(idx, kw) for kw, idx in indices.items() if idx != -1])
        
        ext_tokens: List[str] = []
        int_tokens: List[str] = []
        total_tokens: List[str] = []
        
        for i, (start_idx, kw) in enumerate(sorted_indices):
            end_idx = sorted_indices[i+1][0] if i+1 < len(sorted_indices) else len(tokens)
            raw_section = [self.normalize(t) for t in tokens[start_idx+1:end_idx]]
            cleaned_section = self.clean_section_tokens(raw_section)
            
            if kw == "EXT": ext_tokens = cleaned_section
            elif kw == "INT": int_tokens = cleaned_section
            elif kw == "TOTAL": total_tokens = cleaned_section

        row = [seat_no, spid, gender]
        ext_ptr, int_ptr, tot_ptr = 0, 0, 0
        any_failed = False
        grand_int, grand_ext = 0, 0
        subject_count = 0

        # 1. Processing Practical Subjects
        for _ in range(self.practical_subjects):
            th_int = int_tokens[int_ptr] if int_ptr < len(int_tokens) else "0"
            int_ptr += 1
            pr_int = int_tokens[int_ptr] if int_ptr < len(int_tokens) else "0"
            int_ptr += 1
            int_ptr += 1 # SKIP INT Subject Total

            th_ext = ext_tokens[ext_ptr] if ext_ptr < len(ext_tokens) else "0"
            ext_ptr += 1
            pr_ext = ext_tokens[ext_ptr] if ext_ptr < len(ext_tokens) else "0"
            ext_ptr += 1
            ext_ptr += 1 # SKIP EXT Subject Total
            
            sub_total_val = self.to_numeric(total_tokens[tot_ptr]) if tot_ptr < len(total_tokens) else 0
            tot_ptr += 1
            
            subject_count += 1
            max_marks = 50 if subject_count == 6 else 100
            grade = self.get_grade(sub_total_val, max_marks)
            if grade == "Fail": any_failed = True
                
            th_int_val = self.to_numeric(th_int)
            pr_int_val = self.to_numeric(pr_int)
            th_ext_val = self.to_numeric(th_ext)
            pr_ext_val = self.to_numeric(pr_ext)
            
            subject_sum = th_int_val + pr_int_val + th_ext_val + pr_ext_val
            grand_int += (th_int_val + pr_int_val)
            grand_ext += (th_ext_val + pr_ext_val)
            
            row.extend([th_int, th_ext, pr_int, pr_ext, subject_sum, sub_total_val, grade])

        # 2. Processing Theory-Only Subjects
        for _ in range(self.theory_subjects):
            th_int = int_tokens[int_ptr] if int_ptr < len(int_tokens) else "0"
            int_ptr += 1
            th_ext = ext_tokens[ext_ptr] if ext_ptr < len(ext_tokens) else "0"
            ext_ptr += 1
            sub_total_val = self.to_numeric(total_tokens[tot_ptr]) if tot_ptr < len(total_tokens) else 0
            tot_ptr += 1
            
            subject_count += 1
            max_marks = 50 if subject_count == 6 else 100
            grade = self.get_grade(sub_total_val, max_marks)
            if grade == "Fail": any_failed = True
                
            th_int_val = self.to_numeric(th_int)
            th_ext_val = self.to_numeric(th_ext)
            
            subject_sum = th_int_val + th_ext_val
            grand_int += th_int_val
            grand_ext += th_ext_val
            
            row.extend([th_int, th_ext, subject_sum, grade])

        grand_total = grand_int + grand_ext
        result_status = "Fail" if any_failed else "Pass"
        row.extend([grand_int, grand_ext, grand_total, result_status])

        sem_data = [["", ""] for _ in range(8)]
        historical_matches = re.findall(r"(\d+)\s*-\s*(\d+)\s*-\s*(\d+\.\d{2})", text)
        for sem_num_str, old_set_no, sem_sgpa in historical_matches:
            try:
                sem_idx = int(sem_num_str) - 1
                if 0 <= sem_idx < 8:
                    sem_data[sem_idx] = [old_set_no, sem_sgpa]
            except ValueError:
                continue

        for set_no_val, sgpa_val in sem_data:
            row.append(set_no_val)
            row.append(sgpa_val)
        
        return row

    def build_column_names(self) -> List[str]:
        """Dynamically generates column names based on subject counts."""
        cols = ["SeatNo", "SPID", "Gender"]
        for i in range(1, self.practical_subjects + 1):
            cols += [f"SUB{i}_TH_INT", f"SUB{i}_TH_EXT", f"SUB{i}_PR_INT", f"SUB{i}_PR_EXT", f"SUB{i}_TH_PR_SUM", f"SUB{i}_TOTAL", f"SUB{i}_GRADE"]
        for i in range(self.practical_subjects + 1, self.total_subjects + 1):
            cols += [f"SUB{i}_TH_INT", f"SUB{i}_TH_EXT", f"SUB{i}_TOTAL", f"SUB{i}_GRADE"]
        cols += ["GRAND_INT", "GRAND_EXT", "GRAND_TOTAL", "RESULT"]
        for i in range(1, 9):
            cols.extend([f"SEM{i}_SETNO", f"SEM{i}_SGPA"])
        return cols

    def process_generator(self, pdf_path: str, output_csv: str):
        """Processes the PDF and yields progress updates."""
        rows = []
        logger.info(f"Starting processing of {pdf_path}")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                current_block: List[str] = []
                total_pages = len(pdf.pages)
                
                for page_idx, page in enumerate(pdf.pages):
                    yield f"Processing page {page_idx + 1} of {total_pages}..."
                    text = page.extract_text()
                    if not text: continue
                    
                    lines = text.split("\n")
                    for line in lines:
                        if re.match(r"^\d{4,}\s+\d{10,}", line.strip()):
                            if current_block:
                                try:
                                    data = self.parse_student_block(current_block)
                                    if data: rows.append(data)
                                except Exception as e:
                                    logger.error(f"Error parsing block ending at page {page_idx}: {e}")
                            current_block = [line]
                        else:
                            if current_block: current_block.append(line)
                
                if current_block:
                    try:
                        data = self.parse_student_block(current_block)
                        if data: rows.append(data)
                    except Exception as e:
                        logger.error(f"Error parsing final block: {e}")

            df = pd.DataFrame(rows, columns=self.build_column_names())
            
            if self.metadata_df is not None:
                yield "Merging metadata..."
                try:
                    df['SPID'] = df['SPID'].astype(str).str.strip()
                    self.metadata_df.rename(columns={c: c.upper() for c in self.metadata_df.columns}, inplace=True)
                    self.metadata_df['SPID'] = self.metadata_df['SPID'].astype(str).str.strip()
                    
                    desired_cols = ['SPID', 'ID', 'CAST', 'NAME', 'ENROLLMENT NO', 'APPLICATION ID']
                    existing_cols = [c for c in desired_cols if c in self.metadata_df.columns]
                    
                    if existing_cols:
                        meta_subset = self.metadata_df[existing_cols]
                        df = pd.merge(df, meta_subset, on='SPID', how='left')
                        final_cols = df.columns.tolist()
                        preferred_order = ["SeatNo", "SPID", "ID", "CAST", "NAME"]
                        ordered_cols = [c for c in preferred_order if c in final_cols]
                        remaining_cols = [c for c in final_cols if c not in ordered_cols]
                        df = df[ordered_cols + remaining_cols]
                except Exception as e:
                    logger.error(f"Metadata merge failed: {e}")

            df.to_csv(output_csv, index=False)
            yield f"Saved results for {len(rows)} students to {os.path.basename(output_csv)}"
            return len(rows)
        except Exception as e:
            logger.exception("Conversion failed")
            yield f"Error: {str(e)}"
            raise

def extract_pdf_to_structured_csv(pdf_path, student_detail_path, csv_path):
    """
    Web-friendly generator for PDF extraction.
    Yields progress string updates.
    """
    try:
        meta_df = None
        if student_detail_path and os.path.exists(student_detail_path):
            try:
                if student_detail_path.endswith('.csv'):
                    meta_df = pd.read_csv(student_detail_path)
                else:
                    meta_df = pd.read_excel(student_detail_path)
            except Exception as e:
                yield f"Warning: Could not read metadata: {e}"

        # Default settings: 6 subjects, 4 practicals
        parser = ResultParser(total_subjects=6, practical_subjects=4, metadata_df=meta_df)
        
        for update in parser.process_generator(pdf_path, csv_path):
            yield update
            
    except Exception as e:
        yield f"Error during extraction: {str(e)}"

if __name__ == "__main__":
    if len(sys.argv) == 4:
        pdf_f, meta_f, out_f = sys.argv[1:4]
        print(f"Starting extraction to {out_f}...")
        for p in extract_pdf_to_structured_csv(pdf_f, meta_f, out_f):
            print(p)
    else:
        print("Usage: python pdf_to_structured_csv.py <pdf_file> <detail_file> <output_csv>")
