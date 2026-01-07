import logging
import os
import re
import sys
import tkinter as tk
from threading import Thread
from tkinter import filedialog, messagebox, ttk
from typing import List, Optional, Tuple, Dict, Any

# Third-party imports with check
try:
    import pandas as pd
    import pdfplumber
except ImportError as e:
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror("Missing Dependency", f"Error importing required libraries: {e}\nPlease ensure 'pandas' and 'pdfplumber' are installed.\n\nTry running: pip install pandas pdfplumber openpyxl")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ResultParser:
    """
    Parses student result PDFs and converts them to structured data.
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
        # Handle "20 + 10" or "20+10" formats
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
        """
        Merges tokens that are split by spaces around special characters like '+'.
        Example: ['28', '+', '12'] -> ['28+12']
        """
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

        # Name extraction (skipped for row)
        header_keywords = ["EXT", "INT", "TOTAL", "VNSGU"] 
        
        # Locate EXT, INT, TOTAL sections
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

        # Build Row
        row = [seat_no, spid, gender]
        ext_ptr, int_ptr, tot_ptr = 0, 0, 0
        any_failed = False
        grand_int = 0
        grand_ext = 0
        
        subject_count = 0

        # 1. Processing Practical Subjects
        # Pattern in INT/EXT arrays: [Th, Pr, Total, Th, Pr, Total...]
        # We MUST skip the 'Total' token after reading the components.
        for _ in range(self.practical_subjects):
            # Theory INT
            th_int = int_tokens[int_ptr] if int_ptr < len(int_tokens) else "0"
            int_ptr += 1
            # Practical INT
            pr_int = int_tokens[int_ptr] if int_ptr < len(int_tokens) else "0"
            int_ptr += 1
            # SKIP INT Subject Total
            int_ptr += 1

            # Theory EXT
            th_ext = ext_tokens[ext_ptr] if ext_ptr < len(ext_tokens) else "0"
            ext_ptr += 1
            # Practical EXT
            pr_ext = ext_tokens[ext_ptr] if ext_ptr < len(ext_tokens) else "0"
            ext_ptr += 1
            # SKIP EXT Subject Total
            ext_ptr += 1
            
            # Subject Total (From TOTAL row which has just one value per subject)
            sub_total_val = self.to_numeric(total_tokens[tot_ptr]) if tot_ptr < len(total_tokens) else 0
            tot_ptr += 1
            
            subject_count += 1
            max_marks = 50 if subject_count == 6 else 100
            grade = self.get_grade(sub_total_val, max_marks)

            if grade == "Fail": any_failed = True
                
            # Internal Sum
            th_int_val = self.to_numeric(th_int)
            pr_int_val = self.to_numeric(pr_int)
            th_ext_val = self.to_numeric(th_ext)
            pr_ext_val = self.to_numeric(pr_ext)
            
            subject_sum = th_int_val + pr_int_val + th_ext_val + pr_ext_val
            grand_int += (th_int_val + pr_int_val)
            grand_ext += (th_ext_val + pr_ext_val)
            
            row.extend([th_int, th_ext, pr_int, pr_ext, subject_sum, sub_total_val, grade])


        # 2. Processing Theory-Only Subjects
        # Pattern in INT/EXT arrays: [Th, Total, Th, Total...]
        for _ in range(self.theory_subjects):
            # Theory INT
            th_int = int_tokens[int_ptr] if int_ptr < len(int_tokens) else "0"
            int_ptr += 1

            # Theory EXT
            th_ext = ext_tokens[ext_ptr] if ext_ptr < len(ext_tokens) else "0"
            ext_ptr += 1
            
            # Subject Total
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

        # Grand Totals
        grand_total = grand_int + grand_ext
        result_status = "Fail" if any_failed else "Pass"
        
        row.extend([grand_int, grand_ext, grand_total, result_status])

        # Extract Historical Semester Data
        # Pattern: [Semester] - [SetNo] - [SGPA]
        # Example: 1 - 10068 - 6.55
        sem_data = [["", ""] for _ in range(8)] # [[set_no, sgpa], ...]
        
        # Use a more specific regex to find historical semester lines
        historical_matches = re.findall(r"(\d+)\s*-\s*(\d+)\s*-\s*(\d+\.\d{2})", text)
        for sem_num_str, old_set_no, sem_sgpa in historical_matches:
            try:
                sem_idx = int(sem_num_str) - 1
                if 0 <= sem_idx < 8:
                    sem_data[sem_idx] = [old_set_no, sem_sgpa]
            except ValueError:
                continue

        # Flatten sem_data for the row
        for set_no_val, sgpa_val in sem_data:
            row.append(set_no_val)
            row.append(sgpa_val)
        
        return row

    def build_column_names(self) -> List[str]:
        """Dynamically generates column names based on subject counts."""
        cols = ["SeatNo", "SPID", "Gender"]
        
        for i in range(1, self.practical_subjects + 1):
            cols += [
                f"SUB{i}_TH_INT", f"SUB{i}_TH_EXT", 
                f"SUB{i}_PR_INT", f"SUB{i}_PR_EXT", 
                f"SUB{i}_TH_PR_SUM", f"SUB{i}_TOTAL", f"SUB{i}_GRADE"
            ]
            
        for i in range(self.practical_subjects + 1, self.total_subjects + 1):
            cols += [
                f"SUB{i}_TH_INT", f"SUB{i}_TH_EXT", 
                f"SUB{i}_TOTAL", f"SUB{i}_GRADE"
            ]
            
        cols += ["GRAND_INT", "GRAND_EXT", "GRAND_TOTAL", "RESULT"]
        # Add Sem-N Set No. and SGPA headers
        for i in range(1, 9):
            cols.append(f"SEM{i}_SETNO")
            cols.append(f"SEM{i}_SGPA")
        return cols

    def process(self, pdf_path: str, output_csv: str, progress_callback=None) -> int:
        """Main processing loop."""
        rows = []
        logger.info(f"Starting processing of {pdf_path}")
        
        with pdfplumber.open(pdf_path) as pdf:
            current_block: List[str] = []
            total_pages = len(pdf.pages)
            
            for page_idx, page in enumerate(pdf.pages):
                text = page.extract_text()
                if progress_callback: progress_callback(page_idx + 1, total_pages)
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
            logger.info("Merging with metadata...")
            try:
                df['SPID'] = df['SPID'].astype(str).str.strip()
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
        return len(rows)

class ConverterUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Student Marks PDF to CSV Converter")
        self.root.geometry("650x600")
        self.root.resizable(False, False)
        
        self.style = ttk.Style()
        self.style.theme_use('clam')
        self.style.configure("TLabel", font=("Segoe UI", 10))
        self.style.configure("TButton", font=("Segoe UI", 10))
        self.style.configure("Header.TLabel", font=("Segoe UI", 16, "bold"), foreground="#333")
        self.style.configure("Status.TLabel", font=("Segoe UI", 9), foreground="#666")

        self.setup_ui()

    def setup_ui(self):
        main_frame = ttk.Frame(self.root, padding="25")
        main_frame.pack(fill=tk.BOTH, expand=True)

        header = ttk.Label(main_frame, text="Marks Converter Pro", style="Header.TLabel")
        header.grid(row=0, column=0, columnspan=3, pady=(0, 25))

        ttk.Label(main_frame, text="Result PDF:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.pdf_path = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.pdf_path, width=50).grid(row=1, column=1, padx=10, pady=5)
        ttk.Button(main_frame, text="Browse...", command=self.browse_pdf).grid(row=1, column=2, pady=5)

        ttk.Label(main_frame, text="Details CSV:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.meta_path = tk.StringVar()
        ttk.Entry(main_frame, textvariable=self.meta_path, width=50).grid(row=2, column=1, padx=10, pady=5)
        ttk.Button(main_frame, text="Browse...", command=self.browse_meta).grid(row=2, column=2, pady=5)

        ttk.Label(main_frame, text="Output Name:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.out_name = tk.StringVar(value="RESULT_FINAL.csv")
        ttk.Entry(main_frame, textvariable=self.out_name, width=50).grid(row=3, column=1, padx=10, pady=5)

        settings_labelframe = ttk.LabelFrame(main_frame, text=" Configuration ", padding="15")
        settings_labelframe.grid(row=4, column=0, columnspan=3, pady=25, sticky=tk.EW)

        ttk.Label(settings_labelframe, text="Total Subjects:").grid(row=0, column=0, padx=5)
        self.total_subs = tk.IntVar(value=6)
        sp1 = ttk.Spinbox(settings_labelframe, from_=1, to=20, textvariable=self.total_subs, width=5)
        sp1.grid(row=0, column=1, padx=10)

        ttk.Label(settings_labelframe, text="Practical Subjects:").grid(row=0, column=2, padx=5)
        self.prac_subs = tk.IntVar(value=4)
        sp2 = ttk.Spinbox(settings_labelframe, from_=0, to=20, textvariable=self.prac_subs, width=5)
        sp2.grid(row=0, column=3, padx=10)


        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress_bar.grid(row=5, column=0, columnspan=3, sticky=tk.EW, pady=(10, 5))
        
        self.status_label = ttk.Label(main_frame, text="Ready to start.", style="Status.TLabel")
        self.status_label.grid(row=6, column=0, columnspan=3, sticky=tk.W)

        self.convert_btn = ttk.Button(main_frame, text="START CONVERSION", command=self.start_conversion)
        self.convert_btn.grid(row=7, column=0, columnspan=3, pady=20, ipady=5)

        ttk.Label(main_frame, text="v2.2 Column Skip Fix (INT/EXT Totals)", font=("Segoe UI", 7), foreground="#aaa").grid(row=8, column=0, columnspan=3, pady=(20, 0))

    def browse_pdf(self):
        f = filedialog.askopenfilename(filetypes=[("PDF Files", "*.pdf"), ("All Files", "*.*")])
        if f: self.pdf_path.set(f)

    def browse_meta(self):
        f = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv"), ("All Files", "*.*")])
        if f: self.meta_path.set(f)

    def update_progress(self, current, total):
        percent = (current / total) * 100
        self.progress_var.set(percent)
        self.status_label.config(text=f"Processing page {current} of {total} ({int(percent)}%)...")

    def start_conversion(self):
        pdf_file = self.pdf_path.get()
        if not pdf_file or not os.path.exists(pdf_file):
            messagebox.showerror("Input Error", "Please select a valid PDF file.")
            return

        self.convert_btn.config(state=tk.DISABLED)
        self.status_label.config(text="Initializing...")
        
        t = Thread(target=self.run_process, daemon=True)
        t.start()

    def run_process(self):
        try:
            pdf = self.pdf_path.get()
            meta = self.meta_path.get()
            out = self.out_name.get()
            
            if not out.lower().endswith('.csv'):
                out += '.csv'

            meta_df = None
            if meta and os.path.exists(meta):
                try:
                    meta_df = pd.read_csv(meta)
                except Exception as e:
                    self.root.after(0, lambda: messagebox.showwarning("Metadata Error", f"Could not read metadata CSV:\n{e}\nProceeding without metadata."))

            parser = ResultParser(
                total_subjects=self.total_subs.get(),
                practical_subjects=self.prac_subs.get(),
                metadata_df=meta_df
            )
            
            def cb(c, t):
                self.root.after(0, lambda: self.update_progress(c, t))

            count = parser.process(pdf, out, cb)
            
            self.root.after(0, lambda: self.status_label.config(text="Done."))
            self.root.after(0, lambda: messagebox.showinfo("Success", f"Conversion completed successfully!\n\nProcessed {count} students.\nSaved to: {out}"))

        except Exception as e:
            logger.exception("Conversion process failed")
            self.root.after(0, lambda: self.status_label.config(text="Error occurred."))
            self.root.after(0, lambda: messagebox.showerror("Critical Error", f"An unexpected error occurred:\n{str(e)}"))
        finally:
            self.root.after(0, lambda: self.convert_btn.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.progress_var.set(0))

if __name__ == "__main__":
    try:
        root = tk.Tk()
        app = ConverterUI(root)
        root.mainloop()
    except Exception as e:
        logger.critical(f"Failed to start GUI: {e}")
