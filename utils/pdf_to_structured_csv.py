import pdfplumber
import csv
import re
import sys
import pandas as pd
import logging

logger = logging.getLogger('student_marks_system')

# --- CONFIGURATION ---
# This script is configured for a specific PDF layout. If the PDF format,
# number of subjects, or semester structure changes, the parsing logic in 
# `parse_student_data` may need to be updated.
# You can add more semesters to this list, e.g., 'SEM-6', 'SEM-7', etc.
SEMESTERS_TO_EXTRACT = ['SEM-1', 'SEM-2', 'SEM-3', 'SEM-4', 'SEM-5', 'SEM-6', 'SEM-7', 'SEM-8']
# --- END CONFIGURATION ---

def calculate_grade_from_marks(total_marks):
    """Calculate grade based on total marks"""
    try:
        marks = int(total_marks)
        if marks >= 90:
            return 'DISTINCTION'
        elif marks >= 75:
            return 'FIRST'
        elif marks >= 60:
            return 'SECOND'
        elif marks >= 50:
            return 'PASS'
        else:
            return 'FAIL'
    except:
        return ''

def parse_student_data(lines, index, student_details):
    """
    Parse student data from lines starting at index.
    Returns (student_dict, next_index)
    """
    student = {}
    
    # Find the student info line (starts with seat number)
    student_line = None
    ext_line = None
    int_line = None
    total_line = None
    grade_line = None
    cgpa_values = []
    
    # Look for student info line (pattern: number number M/F name...)
    for i in range(index, min(index + 15, len(lines))):
        line = lines[i].strip()
        if not line:
            continue
            
        # Check if it's a student info line (starts with seat number)
        if re.match(r'^\d{4}\s+\d{10}', line):
            parts = line.split()
            if len(parts) >= 3:
                student['SEAT NO'] = parts[0]
                student['SPID'] = parts[1]
                student['GENDER'] = parts[2]
                # Name is everything after gender until B.C.A.
                name_parts = []
                for j in range(3, len(parts)):
                    if 'B.C.A' in parts[j] or (parts[j].startswith('2023') and '-' in parts[j]):
                        break
                    name_parts.append(parts[j])
                student['NAME'] = ' '.join(name_parts)
                student['CAST'] = 'General'  # Default
                # Try to extract ID - might be in enrollment or need to construct
                student['ID'] = ''  # Will try to extract from context

                # Look up ID and CAST from student_details
                if student_details and student.get('SPID') in student_details:
                    details = student_details[student.get('SPID')]
                    student['ID'] = details.get('id', '')
                    student['CAST'] = details.get('cast', 'General')
                
                student_line = i
                break
    
    if student_line is None:
        return None, index + 1
    
    # Find EXT, INT, TOTAL, GRADE lines, and additional semester data lines
    total_line_idx = None
    for i in range(student_line + 1, min(student_line + 15, len(lines))):
        line = lines[i].strip()
        if line.startswith('EXT') and not ext_line:
            ext_line = line
        elif line.startswith('INT') and not int_line:
            int_line = line
        elif line.startswith('TOTAL') and not line.startswith('TOTAL CR') and not total_line:
            total_line = line
            total_line_idx = i
        elif line.startswith('GL GP CR'):
            grade_line = line
        elif re.match(r'^\d+\.\d+', line):  # CGPA line
            cgpa_values.append(line.split()[0])
        elif re.match(r'^\d{4}\s+\d{10}', line) and i > student_line + 3:
            # Next student found
            break
    
    # NOTE: The following parsing logic is highly specific to the PDF's layout.
    # If subject names or the number of marks change, these hardcoded indices
    # will need to be updated.
    if ext_line:
        ext_parts = ext_line.split()
        if len(ext_parts) >= 15:
            if len(ext_parts) > 15 and ext_parts[15].isdigit():
                student['FINAL TOTAL EXT'] = ext_parts[15]
            else:
                for i in range(10, min(20, len(ext_parts))):
                    if ext_parts[i].isdigit() and 100 <= int(ext_parts[i]) <= 999:
                        student['FINAL TOTAL EXT'] = ext_parts[i]
                        break
            
            student['AWD TH EXT'] = ext_parts[1] if len(ext_parts) > 1 else ''
            student['AWD PR EXT'] = ext_parts[2] if len(ext_parts) > 2 else ''
            student['WFS TH EXT'] = ext_parts[3] if len(ext_parts) > 3 else ''
            student['WFS PR EXT'] = ext_parts[4] if len(ext_parts) > 4 else ''
            student['ASP.NET TH EXT'] = ext_parts[5] if len(ext_parts) > 5 else ''
            student['ASP.NET PR EXT'] = ext_parts[6] if len(ext_parts) > 6 else ''
            student['LOS TH EXT'] = ext_parts[7] if len(ext_parts) > 7 else ''
            student['LOS PR EXT'] = ext_parts[8] if len(ext_parts) > 8 else ''
            student['NT TH EXT'] = ext_parts[9] if len(ext_parts) > 9 else ''
            student['FUNDA.SOFT.ENG EXT'] = ext_parts[11] if len(ext_parts) > 11 else ''
    
    if int_line:
        int_parts = int_line.split()
        if len(int_parts) >= 15:
            if len(int_parts) > 15 and int_parts[15].isdigit():
                student['FINAL TOTAL INT'] = int_parts[15]
            else:
                for i in range(10, min(20, len(int_parts))):
                    if int_parts[i].isdigit() and 100 <= int(int_parts[i]) <= 999:
                        student['FINAL TOTAL INT'] = int_parts[i]
                        break
            
            student['AWD TH INT'] = int_parts[1] if len(int_parts) > 1 else ''
            student['AWD PR INT'] = int_parts[2] if len(int_parts) > 2 else ''
            student['WFS TH INT'] = int_parts[3] if len(int_parts) > 3 else ''
            student['WFS PR INT'] = int_parts[4] if len(int_parts) > 4 else ''
            student['ASP.NET TH INT'] = int_parts[5] if len(int_parts) > 5 else ''
            student['ASP.NET PR INT'] = int_parts[6] if len(int_parts) > 6 else ''
            student['LOS TH INT'] = int_parts[7] if len(int_parts) > 7 else ''
            student['LOS PR INT'] = int_parts[8] if len(int_parts) > 8 else ''
            student['NT INT'] = int_parts[9] if len(int_parts) > 9 else ''
            student['FUNDA.SOFT.ENG INT'] = int_parts[11] if len(int_parts) > 11 else ''
            
            if 'PASS' in int_line:
                student['RESULT'] = 'PASS'
            elif 'FAIL' in int_line:
                student['RESULT'] = 'FAIL'
            else:
                student['RESULT'] = ''
    
    if total_line:
        total_parts = total_line.split()
        if len(total_parts) >= 8:
            student['AWD TOTAL'] = total_parts[1] if len(total_parts) > 1 else ''
            student['WFS TOTAL'] = total_parts[2] if len(total_parts) > 2 else ''
            student['ASP.NET TOTAL'] = total_parts[3] if len(total_parts) > 3 else ''
            student['LOS TOTAL'] = total_parts[4] if len(total_parts) > 4 else ''
            student['NT TOTAL'] = total_parts[5] if len(total_parts) > 5 else ''
            student['FUNDA.SOFT.ENG TOTAL'] = total_parts[6] if len(total_parts) > 6 else ''
            student['FINAL TOTAL'] = total_parts[7] if len(total_parts) > 7 else ''
            
            try:
                ext_total = sum(int(ext_parts[i]) for i in range(1, 11) if len(ext_parts) > i and ext_parts[i].isdigit())
                int_total = sum(int(int_parts[i]) for i in range(1, 11) if len(int_parts) > i and int_parts[i].isdigit())
                if ext_line and int_line:
                    student['FINAL TOTAL EXT'] = str(ext_total)
                    student['FINAL TOTAL INT'] = str(int_total)
            except:
                pass
    
    if grade_line:
        grade_parts = grade_line.split()
        grade_idx = 3
        # NOTE: Update this list if subject names change.
        subjects = ['AWD', 'WFS', 'ASP.NET', 'LOS', 'NT', 'FUNDA.SOFT.ENG']
        for subject in subjects:
            if grade_idx < len(grade_parts):
                grade_letter = grade_parts[grade_idx]
                if grade_letter in ['A+', 'A']:
                    grade_name = 'DISTINCTION'
                elif grade_letter in ['B+', 'B']:
                    grade_name = 'FIRST'
                elif grade_letter == 'C':
                    grade_name = 'SECOND'
                elif grade_letter == 'P':
                    grade_name = 'PASS'
                else:
                    grade_name = calculate_grade_from_marks(student.get(f'{subject} TOTAL', '0'))
                
                student[f'{subject} GRADE'] = grade_name
                grade_idx += 3
    
    # NOTE: Update this list if subject names or their components change.
    subjects_calc = [
        ('AWD', ['AWD TH EXT', 'AWD TH INT', 'AWD PR EXT', 'AWD PR INT']),
        ('WFS', ['WFS TH EXT', 'WFS TH INT', 'WFS PR EXT', 'WFS PR INT']),
        ('ASP.NET', ['ASP.NET TH EXT', 'ASP.NET TH INT', 'ASP.NET PR EXT', 'ASP.NET PR INT']),
        ('LOS', ['LOS TH EXT', 'LOS TH INT', 'LOS PR EXT', 'LOS PR INT']),
        ('NT', ['NT TH EXT', 'NT INT']),
        ('FUNDA.SOFT.ENG', ['FUNDA.SOFT.ENG EXT', 'FUNDA.SOFT.ENG INT'])
    ]
    
    for subject, keys in subjects_calc:
        if f'{subject} TOTAL' not in student or not student[f'{subject} TOTAL']:
            try:
                total = sum(int(student.get(k, '0') or '0') for k in keys)
                student[f'{subject} TOTAL'] = str(total)
            except:
                pass
    
    if 'FINAL TOTAL' not in student or not student['FINAL TOTAL']:
        try:
            final = sum(int(student.get(f'{s} TOTAL', '0') or '0') for s, _ in subjects_calc)
            student['FINAL TOTAL'] = str(final)
        except:
            pass
    
    for subject, _ in subjects_calc:
        if f'{subject} GRADE' not in student or not student[f'{subject} GRADE']:
            student[f'{subject} GRADE'] = calculate_grade_from_marks(student.get(f'{subject} TOTAL', '0'))
    
    sem_data = {}
    
    if ext_line:
        ext_parts = ext_line.split()
        for i in range(len(ext_parts) - 3):
            if ext_parts[i] == '-' and ext_parts[i+1] == '/' and ext_parts[i+2].isdigit():
                sem_num = ext_parts[i+2]
                if i+4 < len(ext_parts) and ext_parts[i+3] == '-' and ext_parts[i+4].isdigit():
                    seat_no = ext_parts[i+4]
                    if i+6 < len(ext_parts) and ext_parts[i+5] == '-' and re.match(r'^\d+\.\d+', ext_parts[i+6]):
                        cgpa = ext_parts[i+6]
                        sem_data[f'SEM-{sem_num}'] = {'seat': seat_no, 'cgpa': cgpa}
    
    if int_line:
        int_parts = int_line.split()
        for i in range(len(int_parts) - 3):
            if int_parts[i] == 'PASS' and int_parts[i+1].isdigit():
                sem_num = int_parts[i+1]
                if i+3 < len(int_parts) and int_parts[i+2] == '-' and int_parts[i+3].isdigit():
                    seat_no = int_parts[i+3]
                    if i+5 < len(int_parts) and int_parts[i+4] == '-' and re.match(r'^\d+\.\d+', int_parts[i+5]):
                        cgpa = int_parts[i+5]
                        sem_data[f'SEM-{sem_num}'] = {'seat': seat_no, 'cgpa': cgpa}
    
    if total_line:
        total_parts = total_line.split()
        for i in range(len(total_parts) - 3):
            if total_parts[i] == '-' and total_parts[i+1].isdigit():
                sem_num = total_parts[i+1]
                if i+3 < len(total_parts) and total_parts[i+2] == '-' and total_parts[i+3].isdigit():
                    seat_no = total_parts[i+3]
                    if i+5 < len(total_parts) and total_parts[i+4] == '-' and re.match(r'^\d+\.\d+', total_parts[i+5]):
                        cgpa = total_parts[i+5]
                        sem_data[f'SEM-{sem_num}'] = {'seat': seat_no, 'cgpa': cgpa}
    
    if total_line_idx is not None:
        for offset in range(1, 6):
            if total_line_idx + offset >= len(lines):
                break
            check_line = lines[total_line_idx + offset].strip()
            if not check_line:
                continue
            
            check_parts = check_line.split()
            if len(check_parts) >= 6 and re.match(r'^\d+\.\d+', check_parts[0]):
                if check_parts[1].isdigit() and check_parts[2] == '-' and check_parts[3].isdigit() and check_parts[4] == '-' and re.match(r'^\d+\.\d+', check_parts[5]):
                    sem_num = check_parts[1]
                    cgpa = check_parts[5]
                    sem_data[f'SEM-{sem_num}'] = {'seat': check_parts[3], 'cgpa': cgpa}
                    continue
            
            if len(check_parts) >= 6 and check_parts[0] == '-':
                if check_parts[1].isdigit() and check_parts[2] == '-' and check_parts[3].isdigit() and check_parts[4] == '-' and re.match(r'^\d+\.\d+', check_parts[5]):
                    sem_num = check_parts[1]
                    cgpa = check_parts[5]
                    sem_data[f'SEM-{sem_num}'] = {'seat': check_parts[3], 'cgpa': cgpa}
    
    for sem in SEMESTERS_TO_EXTRACT:
        if sem in sem_data:
            student[sem] = sem_data[sem].get('cgpa', '')
            student[f'{sem}-SEAT'] = sem_data[sem].get('seat', '')
        else:
            student[sem] = ''
            student[f'{sem}-SEAT'] = ''
    
    if cgpa_values:
        if 'SEM-5' in SEMESTERS_TO_EXTRACT and ('SEM-5' not in sem_data or not student.get('SEM-5', '')):
            student['SEM-5'] = cgpa_values[-1] if cgpa_values else ''
        student['OUT OF 10'] = student.get('SEM-5', '')
    
    next_index = student_line + 1
    for i in range(student_line + 1, min(student_line + 20, len(lines))):
        line = lines[i].strip()
        if re.match(r'^\d{4}\s+\d{10}', line) and i > student_line + 3:
            next_index = i
            break
        if i == len(lines) - 1:
            next_index = len(lines)
    
    return student, next_index

def extract_pdf_to_structured_csv(pdf_path, student_detail_path, csv_path):
    """
    Extract text from PDF and save properly structured data to CSV file.
    This function is a generator that yields progress updates.
    """
    logger.info(f"Starting extraction from {pdf_path} and {student_detail_path}")
    all_students = []
    
    student_details = {}
    try:
        logger.info("Reading student details file.")
        if student_detail_path.endswith('.csv'):
            df = pd.read_csv(student_detail_path)
        elif student_detail_path.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(student_detail_path)
        else:
            raise ValueError("Unsupported file format for student details. Please use a CSV or Excel file.")
        
        df.columns = [col.lower() for col in df.columns]
        # Assuming the columns are named 'SPID', 'ID', and 'CAST'
        for index, row in df.iterrows():
            student_details[str(row['spid'])] = {'id': row['id'], 'cast': row['cast']}
        logger.info("Finished reading student details file.")
    except FileNotFoundError:
        logger.warning("Student details Excel file not found.")
        yield "Student details Excel file not found. 'ID' and 'CAST' will be blank."
    except Exception as e:
        logger.exception("Error reading student details file.")
        yield f"Error reading student details file: {e}"

    try:
        logger.info(f"Opening PDF: {pdf_path}")
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            yield f"Total pages: {total_pages}"
            
            all_lines = []
            for page_num, page in enumerate(pdf.pages, 1):
                yield f"Processing page {page_num}/{total_pages}..."
                text = page.extract_text()
                if text:
                    all_lines.extend(text.split('\n'))
            
            logger.info("Finished processing PDF pages. Parsing student data.")
            index = 0
            while index < len(all_lines):
                student, next_index = parse_student_data(all_lines, index, student_details)
                if student and 'SEAT NO' in student:
                    all_students.append(student)
                index = next_index
                if index <= (student.get('_last_index', 0) if student else 0):
                    index += 1
                if index >= len(all_lines):
                    break
            logger.info(f"Finished parsing student data. Found {len(all_students)} students.")
    
    except ImportError:
        logger.error("pdfplumber is not installed.")
        yield "pdfplumber is not installed. Please install it."
        return
    
    except Exception as e:
        logger.exception("Error during PDF extraction.")
        yield f"Error extracting PDF: {e}"
        return
    
    if all_students:
        logger.info(f"Writing {len(all_students)} students to {csv_path}")
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # NOTE: Update this header if subject names change.
            header_row = [
                'SEAT NO', 'SPID', 'ID', 'GENDER', 'NAME', 'CAST',
                'AWD TH', '', 'AWD PR', '', 'AWD FINAL TOTAL', '', 'TOTAL', 'GRADE',
                'WFS TH', '', 'WFS PR', '', 'WFS FINAL TOTAL', '', 'TOTAL', 'GRADE',
                'ASP.NET TH', '', 'ASP.NET PR', '', 'ASP.NET FINAL TOTAL', '', 'TOTAL', 'GRADE',
                'LOS TH', '', 'LOS PR', '', 'LOS FINAL TOTAL', '', 'TOTAL', 'GRADE',
                'NT TH', 'INT', 'NT FINAL TOTAL', 'TOTAL', 'GRADE',
                'FUNDA.SOFT.ENG', '', 'FUNDA.SOFT.ENG FINAL TOTAL', 'TOTAL', 'GRADE',
                'FINAL TOTAL', '', 'TOTAL', 'RESULT',
            ]
            
            for sem in SEMESTERS_TO_EXTRACT:
                header_row.extend([f'{sem} SEAT', sem])
            header_row.append('OUT OF 10')
            writer.writerow(header_row)
            
            sub_header = [
                '', '', '', '', '', '',
                'EXT', 'INT', 'EXT', 'INT', 'EXT', 'INT', '', '',
                'EXT', 'INT', 'EXT', 'INT', 'EXT', 'INT', '', '',
                'EXT', 'INT', 'EXT', 'INT', 'EXT', 'INT', '', '',
                'EXT', 'INT', 'EXT', 'INT', 'EXT', 'INT', '', '',
                'EXT', 'INT', '', '', '',
                'EXT', 'INT', '', '', '',
                'EXT', 'INT', '', '',
            ]
            sub_header.extend([''] * (len(SEMESTERS_TO_EXTRACT) * 2 + 1))
            writer.writerow(sub_header)
            
            for student in all_students:
                try:
                    awd_final_ext = int(student.get('AWD TH EXT', '0') or '0') + int(student.get('AWD PR EXT', '0') or '0')
                    awd_final_int = int(student.get('AWD TH INT', '0') or '0') + int(student.get('AWD PR INT', '0') or '0')
                except:
                    awd_final_ext = ''
                    awd_final_int = ''
                
                try:
                    wfs_final_ext = int(student.get('WFS TH EXT', '0') or '0') + int(student.get('WFS PR EXT', '0') or '0')
                    wfs_final_int = int(student.get('WFS TH INT', '0') or '0') + int(student.get('WFS PR INT', '0') or '0')
                except:
                    wfs_final_ext = ''
                    wfs_final_int = ''
                
                try:
                    aspnet_final_ext = int(student.get('ASP.NET TH EXT', '0') or '0') + int(student.get('ASP.NET PR EXT', '0') or '0')
                    aspnet_final_int = int(student.get('ASP.NET TH INT', '0') or '0') + int(student.get('ASP.NET PR INT', '0') or '0')
                except:
                    aspnet_final_ext = ''
                    aspnet_final_int = ''
                
                try:
                    los_final_ext = int(student.get('LOS TH EXT', '0') or '0') + int(student.get('LOS PR EXT', '0') or '0')
                    los_final_int = int(student.get('LOS TH INT', '0') or '0') + int(student.get('LOS PR INT', '0') or '0')
                except:
                    los_final_ext = ''
                    los_final_int = ''
                
                try:
                    nt_final_total = int(student.get('NT TH EXT', '0') or '0') + int(student.get('NT INT', '0') or '0')
                except:
                    nt_final_total = ''
                
                try:
                    funda_final_total = int(student.get('FUNDA.SOFT.ENG EXT', '0') or '0') + int(student.get('FUNDA.SOFT.ENG INT', '0') or '0')
                except:
                    funda_final_total = ''
                
                final_total_ext = student.get('FINAL TOTAL EXT', '')
                final_total_int = student.get('FINAL TOTAL INT', '')
                final_total_grand = student.get('FINAL TOTAL', '')
                
                # NOTE: Update this row structure if subject names change.
                row = [
                    student.get('SEAT NO', ''),
                    student.get('SPID', ''),
                    student.get('ID', ''),
                    student.get('GENDER', ''),
                    student.get('NAME', ''),
                    student.get('CAST', ''),
                    student.get('AWD TH EXT', ''), student.get('AWD TH INT', ''),
                    student.get('AWD PR EXT', ''), student.get('AWD PR INT', ''),
                    str(awd_final_ext) if awd_final_ext != '' else '',
                    str(awd_final_int) if awd_final_int != '' else '',
                    student.get('AWD TOTAL', ''), student.get('AWD GRADE', ''),
                    student.get('WFS TH EXT', ''), student.get('WFS TH INT', ''),
                    student.get('WFS PR EXT', ''), student.get('WFS PR INT', ''),
                    str(wfs_final_ext) if wfs_final_ext != '' else '',
                    str(wfs_final_int) if wfs_final_int != '' else '',
                    student.get('WFS TOTAL', ''), student.get('WFS GRADE', ''),
                    student.get('ASP.NET TH EXT', ''), student.get('ASP.NET TH INT', ''),
                    student.get('ASP.NET PR EXT', ''), student.get('ASP.NET PR INT', ''),
                    str(aspnet_final_ext) if aspnet_final_ext != '' else '',
                    str(aspnet_final_int) if aspnet_final_int != '' else '',
                    student.get('ASP.NET TOTAL', ''), student.get('ASP.NET GRADE', ''),
                    student.get('LOS TH EXT', ''), student.get('LOS TH INT', ''),
                    student.get('LOS PR EXT', ''), student.get('LOS PR INT', ''),
                    str(los_final_ext) if los_final_ext != '' else '',
                    str(los_final_int) if los_final_int != '' else '',
                    student.get('LOS TOTAL', ''), student.get('LOS GRADE', ''),
                    student.get('NT TH EXT', ''), student.get('NT INT', ''),
                    str(nt_final_total) if nt_final_total != '' else '',
                    student.get('NT TOTAL', ''), student.get('NT GRADE', ''),
                    student.get('FUNDA.SOFT.ENG EXT', ''), student.get('FUNDA.SOFT.ENG INT', ''),
                    str(funda_final_total) if funda_final_total != '' else '',
                    student.get('FUNDA.SOFT.ENG TOTAL', ''), student.get('FUNDA.SOFT.ENG GRADE', ''),
                    final_total_ext if final_total_ext else '',
                    final_total_int if final_total_int else '',
                    final_total_grand if final_total_grand else '',
                    student.get('RESULT', ''),
                ]
                
                for sem in SEMESTERS_TO_EXTRACT:
                    row.extend([student.get(f'{sem}-SEAT', ''), student.get(sem, '')])
                row.append(student.get('OUT OF 10', ''))
                
                writer.writerow(row)
        
        yield f"\nSuccessfully extracted {len(all_students)} students to {csv_path}"
        logger.info("Finished writing CSV file.")
    else:
        yield "No student data extracted from PDF"

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python pdf_to_structured_csv.py <pdf_file> <student_detail_file> <csv_file>")
        sys.exit(1)
    
    pdf_file = sys.argv[1]
    student_detail_file = sys.argv[2]
    csv_file = sys.argv[3]
    
    print(f"Extracting structured data from {pdf_file}...")
    for progress in extract_pdf_to_structured_csv(pdf_file, student_detail_file, csv_file):
        print(progress, end='')
