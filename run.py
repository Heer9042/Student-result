import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import sys
import pandas as pd
from pdf_to_structured_csv import extract_pdf_to_structured_csv

class App:
    """
    A GUI application for converting student result PDFs to structured CSV files.
    """
    def __init__(self, root):
        """
        Initializes the application window and widgets.

        Args:
            root (tk.Tk): The root Tkinter window.
        """
        self.root = root
        self.root.title("PDF to CSV Converter")
        self.root.geometry("600x300")
        self.root.resizable(False, False)

        # --- Styling ---
        self.style = ttk.Style(self.root)
        self.style.theme_use("clam")
        self.style.configure("TLabel", font=("Helvetica", 10))
        self.style.configure("TButton", font=("Helvetica", 10, "bold"))
        self.style.configure("TEntry", font=("Helvetica", 10))

        # --- Main Frame ---
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- Input Widgets ---
        input_frame = ttk.LabelFrame(main_frame, text="Input Files", padding="10")
        input_frame.pack(fill=tk.X, pady=5)
        input_frame.columnconfigure(1, weight=1)

        # Result PDF file
        self.pdf_path_label = ttk.Label(input_frame, text="Result PDF:")
        self.pdf_path_label.grid(row=0, column=0, padx=5, pady=5, sticky='w')
        self.pdf_path_entry = ttk.Entry(input_frame, width=50)
        self.pdf_path_entry.grid(row=0, column=1, padx=5, pady=5, sticky='ew')
        self.browse_pdf_button = ttk.Button(input_frame, text="Browse", command=self.browse_pdf)
        self.browse_pdf_button.grid(row=0, column=2, padx=5, pady=5)

        # Student detail file
        self.student_detail_label = ttk.Label(input_frame, text="Student Detail File:")
        self.student_detail_label.grid(row=1, column=0, padx=5, pady=5, sticky='w')
        self.student_detail_entry = ttk.Entry(input_frame, width=50)
        self.student_detail_entry.grid(row=1, column=1, padx=5, pady=5, sticky='ew')
        self.browse_student_detail_button = ttk.Button(input_frame, text="Browse", command=self.browse_student_detail)
        self.browse_student_detail_button.grid(row=1, column=2, padx=5, pady=5)

        # --- Output Widgets ---
        output_frame = ttk.LabelFrame(main_frame, text="Output", padding="10")
        output_frame.pack(fill=tk.X, pady=5)

        self.csv_path_label = ttk.Label(output_frame, text="CSV File:")
        self.csv_path_label.pack(side=tk.LEFT, padx=5)
        self.csv_path_entry = ttk.Entry(output_frame, width=50)
        self.csv_path_entry.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        self.browse_csv_button = ttk.Button(output_frame, text="Save As", command=self.browse_csv)
        self.browse_csv_button.pack(side=tk.LEFT, padx=5)

        # --- Action Widgets ---
        action_frame = ttk.Frame(main_frame, padding="10")
        action_frame.pack(fill=tk.X, pady=5)

        self.convert_button = ttk.Button(action_frame, text="Convert", command=self.start_conversion)
        self.convert_button.pack(side=tk.LEFT, padx=5)

        self.progress_bar = ttk.Progressbar(action_frame, orient=tk.HORIZONTAL, length=200, mode='indeterminate')
        self.progress_bar.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        
        # --- Status Label ---
        self.status_label = ttk.Label(main_frame, text="")
        self.status_label.pack(fill=tk.X, pady=5)

    def browse_pdf(self):
        """Opens a file dialog to select a PDF file."""
        filepath = filedialog.askopenfilename(filetypes=[("PDF files", "*.pdf")])
        if filepath:
            self.pdf_path_entry.delete(0, tk.END)
            self.pdf_path_entry.insert(0, filepath)

    def browse_student_detail(self):
        """Opens a file dialog to select a student detail file (CSV or Excel)."""
        filepath = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xlsx *.xls")])
        if filepath:
            self.student_detail_entry.delete(0, tk.END)
            self.student_detail_entry.insert(0, filepath)

    def browse_csv(self):
        """Opens a file dialog to select a location to save the output CSV file."""
        filepath = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if filepath:
            self.csv_path_entry.delete(0, tk.END)
            self.csv_path_entry.insert(0, filepath)

    def start_conversion(self):
        """
        Starts the PDF to CSV conversion process after validating the input files.
        """
        pdf_path = self.pdf_path_entry.get()
        student_detail_path = self.student_detail_entry.get()
        csv_path = self.csv_path_entry.get()

        if not pdf_path or not student_detail_path or not csv_path:
            messagebox.showerror("Error", "Please select all input and output files.")
            return

        # Validate student detail file
        try:
            if student_detail_path.endswith('.csv'):
                df = pd.read_csv(student_detail_path, nrows=0)
            elif student_detail_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(student_detail_path, nrows=0)
            else:
                messagebox.showerror("Error", "Unsupported student detail file format. Please use a CSV or Excel file.")
                return

            df.columns = [col.lower() for col in df.columns]
            required_columns = ['id', 'cast', 'spid']
            if not all(col in df.columns for col in required_columns):
                messagebox.showerror("Error", f"The student detail file must contain the following columns: {', '.join(required_columns)}")
                return
        except Exception as e:
            messagebox.showerror("Error", f"Failed to read the student detail file: {e}")
            return

        self.convert_button.config(state=tk.DISABLED)
        self.status_label.config(text="Converting...")
        self.progress_bar.start()

        # Run conversion in a separate thread to keep the UI responsive
        thread = threading.Thread(target=self.run_conversion, args=(pdf_path, student_detail_path, csv_path))
        thread.start()

    def run_conversion(self, pdf_path, student_detail_path, csv_path):
        """
        Runs the PDF extraction process in a separate thread.

        Args:
            pdf_path (str): The path to the input PDF file.
            student_detail_path (str): The path to the student detail file.
            csv_path (str): The path to save the output CSV file.
        """
        try:
            # Redirect stdout to capture print statements from the script
            original_stdout = sys.stdout
            
            class RedirectedText:
                def __init__(self, text_widget, progress_bar):
                    self.text_widget = text_widget
                    self.progress_bar = progress_bar
                def write(self, string):
                    self.text_widget.config(text=string.strip())
                    # A simple way to show progress, assuming each page is a step
                    if "Processing page" in string:
                        self.progress_bar.step()
                def flush(self):
                    pass
            
            sys.stdout = RedirectedText(self.status_label, self.progress_bar)

            success = extract_pdf_to_structured_csv(pdf_path, student_detail_path, csv_path)

            sys.stdout = original_stdout # Restore stdout

            if success:
                messagebox.showinfo("Success", f"Successfully converted PDF to {csv_path}")
            else:
                messagebox.showerror("Error", "Failed to convert PDF. Check the status messages for details.")

        except Exception as e:
            sys.stdout = original_stdout # Restore stdout
            messagebox.showerror("Error", f"An error occurred: {e}")
        finally:
            self.root.after(0, self.reset_ui)

    def reset_ui(self):
        """Resets the UI to its initial state after the conversion is complete."""
        self.convert_button.config(state=tk.NORMAL)
        self.status_label.config(text="")
        self.progress_bar.stop()


if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()