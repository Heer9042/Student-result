"""
Main Flask application for Student Marks Management System.

Features:
- Upload CSV/PDF files with student marks
- Filter students by overall pass/fail status
- Filter students by subject-wise pass/fail
- Download filtered results as CSV
- View statistics and summaries
"""

from flask import Flask, render_template, request, send_file, jsonify, session, Response, stream_with_context
import pandas as pd
import os
import shutil
from datetime import datetime
from config import UPLOAD_FOLDER, DOWNLOAD_FOLDER, ALLOWED_EXTENSIONS, MAX_FILE_SIZE, SECRET_KEY
from utils.file_reader import read_student_marks, validate_marks_data
from utils.data_processor import StudentMarksProcessor
from utils.pdf_to_structured_csv import extract_pdf_to_structured_csv
import io
from math import ceil
from io import BytesIO
import logging
from logging.handlers import RotatingFileHandler

# Setup logging first (before it's used)
os.makedirs('logs', exist_ok=True)
logger = logging.getLogger('student_marks_system')
logger.setLevel(logging.INFO)
handler = RotatingFileHandler('logs/error.log', maxBytes=1024*1024, backupCount=3)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)

# PDF generation
try:
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
    from reportlab.lib import colors
    PDF_AVAILABLE = True
except Exception:
    PDF_AVAILABLE = False
    logger.warning('reportlab not available; PDF export disabled')


# Initialize Flask application
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE
app.config['DOWNLOAD_FOLDER'] = DOWNLOAD_FOLDER


def allowed_file(filename: str) -> bool:
    """
    Check if uploaded file has an allowed extension.
    
    Args:
        filename (str): Name of the uploaded file
        
    Returns:
        bool: True if file extension is allowed, False otherwise
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def generate_filename(base_name: str, extension: str = 'csv') -> str:
    """
    Generate a unique filename with timestamp.
    
    Args:
        base_name (str): Base name for the file
        extension (str): File extension (default: csv)
        
    Returns:
        str: Generated filename with timestamp
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{base_name}_{timestamp}.{extension}"


@app.route('/')
def index():
    """
    Display the home page with upload form and options.
    
    Returns:
        Rendered index.html template
    """
    return render_template('index.html')


@app.route('/initiate-conversion', methods=['POST'])
def initiate_conversion():
    """
    Handle file uploads for PDF to CSV conversion.
    """
    logger.info("Starting PDF to CSV conversion upload.")
    try:
        if 'pdf_file' not in request.files or 'student_detail_file' not in request.files:
            logger.error("Files not found in request.")
            return jsonify({'success': False, 'message': 'Both PDF and student detail files are required.'}), 400

        pdf_file = request.files['pdf_file']
        student_detail_file = request.files['student_detail_file']
        logger.info(f"Received files: {pdf_file.filename}, {student_detail_file.filename}")

        if pdf_file.filename == '' or student_detail_file.filename == '':
            logger.error("One or both files have no filename.")
            return jsonify({'success': False, 'message': 'No file selected for one or both inputs.'}), 400

        if not pdf_file.filename.lower().endswith('.pdf'):
            logger.error("Invalid PDF file format.")
            return jsonify({'success': False, 'message': 'Invalid PDF file format.'}), 400
        
        detail_file_ext_parts = student_detail_file.filename.rsplit('.', 1)
        if len(detail_file_ext_parts) < 2 or detail_file_ext_parts[1].lower() not in ['csv', 'xlsx', 'xls']:
            logger.error("Invalid student detail file format.")
            return jsonify({'success': False, 'message': 'Invalid student detail file format. Must be CSV or Excel.'}), 400
        detail_file_ext = detail_file_ext_parts[1].lower()

        pdf_filename = os.path.join(app.config['UPLOAD_FOLDER'], generate_filename('uploaded', 'pdf'))
        detail_filename = os.path.join(app.config['UPLOAD_FOLDER'], generate_filename('details', detail_file_ext))
        csv_output_filename = os.path.join(app.config['DOWNLOAD_FOLDER'], generate_filename('converted', 'csv'))
        logger.info(f"Generated filenames: {pdf_filename}, {detail_filename}, {csv_output_filename}")

        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)

        pdf_file.save(pdf_filename)
        student_detail_file.save(detail_filename)
        logger.info("Files saved.")
        
        session['pdf_filename'] = pdf_filename
        session['detail_filename'] = detail_filename
        session['converted_file'] = csv_output_filename
        
        # New: Store subject configuration
        session['total_subjects'] = int(request.form.get('total_subjects', 6))
        session['practical_subjects'] = int(request.form.get('practical_subjects', 4))
        
        logger.info(f"Filenames and config (Subjects: {session['total_subjects']}, Practical: {session['practical_subjects']}) stored in session.")

        return jsonify({'success': True, 'message': 'Files uploaded successfully. Starting conversion.'})

    except Exception as e:
        logger.exception('Exception in initiate_conversion')
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500

@app.route('/conversion-stream')
def conversion_stream():
    """
    Streams the progress of the PDF to CSV conversion.
    """
    logger.info("Starting conversion stream.")
    pdf_filename = session.get('pdf_filename')
    detail_filename = session.get('detail_filename')
    csv_output_filename = session.get('converted_file')
    logger.info(f"Retrieved from session: {pdf_filename}, {detail_filename}, {csv_output_filename}")

    if not all([pdf_filename, detail_filename, csv_output_filename]):
        def generate_error():
            logger.error("File paths not found in session for conversion stream.")
            yield "data: Error: File paths not found in session. Please upload the files again.\n\n"
        return Response(stream_with_context(generate_error()), mimetype='text/event-stream')

    def generate():
        try:
            total_subjects = session.get('total_subjects', 6)
            practical_subjects = session.get('practical_subjects', 4)
            
            for progress in extract_pdf_to_structured_csv(
                pdf_filename, 
                detail_filename, 
                csv_output_filename, 
                total_subjects=total_subjects, 
                practical_subjects=practical_subjects
            ):
                yield f"data: {progress}\n\n"
            yield f"data: done\n\n"
            logger.info("Conversion stream completed successfully.")
        except Exception as e:
            logger.exception("Exception in conversion stream generation.")
            yield f"data: Error: {str(e)}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream')

@app.route('/download_converted')
def download_converted_file():
    """
    Download the last converted CSV file.
    """
    try:
        if 'converted_file' not in session:
            return jsonify({'success': False, 'message': 'No converted file to download'}), 400
        
        converted_path = session.get('converted_file')
        if not converted_path or not os.path.exists(converted_path):
            return jsonify({'success': False, 'message': 'No converted file available for download.'}), 400

        return send_file(converted_path, mimetype='text/csv', as_attachment=True, download_name='converted_result.csv')
        
    except Exception as e:
        logger.exception('Exception in download_converted_file')
        return jsonify({'success': False, 'message': f'Error downloading file: {str(e)}'}), 500




@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handle file upload and initial data processing.
    
    Returns:
        JSON response with success status and detected subjects.
    """
    try:
        # Check if file was uploaded
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file uploaded'}), 400
        
        file = request.files['file']
        
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'}), 400
        
        # Validate file extension
        if not allowed_file(file.filename):
            return jsonify({'success': False, 'message': 'Invalid file format. Please upload CSV or PDF.'}), 400
        
        # Save uploaded file
        filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
        file.save(filename)
        
        # Read and validate file
        success, df, message = read_student_marks(filename)
        print(f"DEBUG: File read success: {success}, message: {message}")
        print(f"DEBUG: DataFrame shape: {df.shape}")
        print(f"DEBUG: DataFrame columns: {list(df.columns)}")
        print(f"DEBUG: First 5 rows:\n{df.head()}")

        if not success:
            return jsonify({'success': False, 'message': message}), 400

        # Validate marks data
        is_valid, validation_msg = validate_marks_data(df)
        print(f"DEBUG: Validation success: {is_valid}, message: {validation_msg}")
        if not is_valid:
            return jsonify({'success': False, 'message': validation_msg}), 400
        
        # Store uploaded file path in session (avoid storing large data in session cookie)
        session['uploaded_file'] = filename
        session['filename'] = file.filename
        
        processor = StudentMarksProcessor(df)
        subjects = processor.subject_columns
        
        return jsonify({
            'success': True,
            'message': 'File uploaded successfully. Please select subjects to continue.',
            'data': {
                'subjects': subjects,
                'sem_columns': processor.get_sem_columns(),
            }
        }), 200
        
    except Exception as e:
        logger.exception('Exception in upload_file')
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/load_data', methods=['POST'])
def load_data():
    """
    Loads data with selected subjects and returns initial statistics.
    """
    try:
        if 'uploaded_file' not in session:
            return jsonify({'success': False, 'message': 'No data loaded. Please upload a file first.'}), 400

        req_json = request.get_json()
        selected_subjects = req_json.get('subjects', [])
        practical_subjects = req_json.get('practical_subjects', [])
        theory_subjects = req_json.get('theory_subjects', [])
        
        session['selected_subjects'] = selected_subjects
        session['practical_subjects'] = practical_subjects
        session['theory_subjects'] = theory_subjects

        uploaded_file = session.get('uploaded_file')
        if not uploaded_file or not os.path.exists(uploaded_file):
            return jsonify({'success': False, 'message': 'Uploaded file not found on server. Please re-upload.'}), 400

        success, df, msg = read_student_marks(uploaded_file)
        if not success:
            return jsonify({'success': False, 'message': f'Error reading uploaded file: {msg}'}), 500

        # Initialize processor with categorized subjects
        processor = StudentMarksProcessor(
            df, 
            selected_subjects=selected_subjects,
            practical_subjects=practical_subjects,
            theory_subjects=theory_subjects
        )
        
        stats = processor.get_overall_statistics()

        return jsonify({
            'success': True,
            'message': 'Data loaded successfully.',
            'data': {
                'statistics': stats
            }
        }), 200

    except Exception as e:
        logger.exception('Exception in load_data')
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500



@app.route('/get_semesters', methods=['GET'])
def get_semesters():
    """
    Get the available semester columns from the uploaded data.
    
    Returns:
        JSON response with a list of semester columns
    """
    try:
        if 'uploaded_file' not in session:
            return jsonify({'success': False, 'message': 'No data loaded. Please upload a file first.'}), 400
        
        uploaded_file = session.get('uploaded_file')
        if not uploaded_file or not os.path.exists(uploaded_file):
            return jsonify({'success': False, 'message': 'Uploaded file not found on server. Please re-upload.'}), 400

        success, df, msg = read_student_marks(uploaded_file)
        if not success:
            return jsonify({'success': False, 'message': f'Error reading uploaded file: {msg}'}), 500

        processor = StudentMarksProcessor(df)
        sem_columns = processor.get_sem_columns()
        
        return jsonify({'success': True, 'semesters': sem_columns}), 200
        
    except Exception as e:
        logger.exception('Exception in get_semesters')
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/filter', methods=['POST'])

def filter_data():
    """
    Apply filters to student data based on request parameters.
    
    Returns:
        JSON response with filtered data
    """
    try:
        # Check if uploaded file path exists in session
        if 'uploaded_file' not in session:
            return jsonify({'success': False, 'message': 'No data loaded. Please upload a file first.'}), 400

        uploaded_file = session.get('uploaded_file')
        if not uploaded_file or not os.path.exists(uploaded_file):
            return jsonify({'success': False, 'message': 'Uploaded file not found on server. Please re-upload.'}), 400

        success, df, msg = read_student_marks(uploaded_file)
        if not success:
            return jsonify({'success': False, 'message': f'Error reading uploaded file: {msg}'}), 500

        selected_subjects = session.get('selected_subjects', None)
        practical_subjects = session.get('practical_subjects', None)
        theory_subjects = session.get('theory_subjects', None)
        
        processor = StudentMarksProcessor(
            df, 
            selected_subjects=selected_subjects,
            practical_subjects=practical_subjects,
            theory_subjects=theory_subjects
        )
        
        # Parse JSON body safely (avoid exceptions on invalid JSON)
        req_json = request.get_json(silent=True)
        if not req_json:
            logger.warning('Empty or invalid JSON received for /filter from %s', request.remote_addr)
            return jsonify({'success': False, 'message': 'Invalid request payload. Expected JSON.'}), 400

        # Get filter type and apply appropriate filter
        filter_type = req_json.get('filter_type')
        filtered_df = None
        filter_description = ""
        
        if filter_type == 'overall_pass':
            upto_semester = req_json.get('upto_semester')
            filtered_df = processor.filter_passed_students(upto_semester)
            if upto_semester:
                filter_description = f"Students who passed all semesters up to {upto_semester.replace('-', ' ').title()}"
            else:
                filter_description = "Students who passed all semesters"
            
        elif filter_type == 'overall_fail':
            upto_semester = req_json.get('upto_semester')
            filtered_df = processor.filter_failed_students(upto_semester)
            if upto_semester:
                filter_description = f"Students who failed in at least one semester up to {upto_semester.replace('-', ' ').title()}"
            else:
                filter_description = "Students who failed at least one semester"
            
        elif filter_type == 'subject_pass':
            subject = req_json.get('subject')
            if not subject:
                return jsonify({'success': False, 'message': 'Subject not specified'}), 400
            filtered_df = processor.filter_subject_wise_pass(subject)
            filter_description = f"Students who passed {subject}"

        elif filter_type == 'subject_fail':
            subject = req_json.get('subject')
            if not subject:
                return jsonify({'success': False, 'message': 'Subject not specified'}), 400
            filtered_df = processor.filter_subject_wise_fail(subject)
            filter_description = f"Students who failed {subject}"
            
        elif filter_type == 'summary':
            summary_df = processor.get_subject_wise_summary()
            # save summary as CSV on server for download
            os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)
            summary_filename = generate_filename('summary', extension='csv')
            summary_path = os.path.join(app.config['DOWNLOAD_FOLDER'], summary_filename)
            summary_df.to_csv(summary_path, index=False)
            session['filtered_file'] = summary_path
            session['filter_type'] = 'summary'
            return jsonify({
                'success': True,
                'message': 'Summary generated successfully',
                'filter_description': 'Subject-wise Pass/Fail Summary',
                'data': summary_df.fillna('').to_dict('records'),
                'rows': len(summary_df)
            }), 200
            
        elif filter_type == 'statistics':
            stats = processor.get_overall_statistics()
            return jsonify({
                'success': True,
                'message': 'Statistics generated successfully',
                'filter_description': 'Overall Class Statistics',
                'statistics': stats
            }), 200
        
        elif filter_type == 'sem_pass':
            sem_column = req_json.get('sem_column')
            if not sem_column:
                return jsonify({'success': False, 'message': 'Semester column not specified'}), 400
            filtered_df = processor.filter_sem_pass(sem_column)
            # Extract semester number from sem-X
            sem_num = sem_column.replace('sem-', 'Sem ').title()
            filter_description = f"Students who passed {sem_num}"

        elif filter_type == 'sem_fail':
            sem_column = req_json.get('sem_column')
            if not sem_column:
                return jsonify({'success': False, 'message': 'Semester column not specified'}), 400
            filtered_df = processor.filter_sem_fail(sem_column)
            # Extract semester number from sem-X
            sem_num = sem_column.replace('sem-', 'Sem ').title()
            filter_description = f"Students who failed {sem_num}"
        
        elif filter_type == 'all_practical_pass':
            filtered_df = processor.filter_by_type('practical', 'pass')
            filter_description = "Students who passed ALL Practical subjects"
            
        elif filter_type == 'all_practical_fail':
            filtered_df = processor.filter_by_type('practical', 'fail')
            filter_description = "Students who failed in at least one Practical subject"
            
        elif filter_type == 'all_theory_pass':
            filtered_df = processor.filter_by_type('theory', 'pass')
            filter_description = "Students who passed ALL Theory subjects"
            
        elif filter_type == 'all_theory_fail':
            filtered_df = processor.filter_by_type('theory', 'fail')
            filter_description = "Students who failed in at least one Theory subject"
        
        else:
            return jsonify({'success': False, 'message': 'Invalid filter type'}), 400
        
        if filtered_df is None or filtered_df.empty:
            # Store empty filtered data as CSV on server for download consistency
            os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)
            # use columns from the uploaded dataframe (df is in scope above)
            empty_df = pd.DataFrame(columns=list(df.columns) if 'df' in locals() else [])
            empty_filename = generate_filename(filter_type, extension='csv')
            empty_path = os.path.join(app.config['DOWNLOAD_FOLDER'], empty_filename)
            empty_df.to_csv(empty_path, index=False)
            session['filtered_file'] = empty_path
            session['filter_type'] = filter_type
            session['filter_description'] = filter_description
            return jsonify({
                'success': True,
                'message': 'No records found matching the filter criteria',
                'data': [],
                'rows': 0,
                'filter_description': filter_description,
                'page': 1,
                'per_page': 10,
                'total_pages': 0,
                'total_rows': 0
            }), 200

        # Pagination support
        page = int(req_json.get('page', 1) or 1)
        per_page = int(req_json.get('per_page', 10) or 10)
        total_rows = len(filtered_df)
        total_pages = ceil(total_rows / per_page) if total_rows > 0 else 0
        if page < 1:
            page = 1
        if per_page < 1:
            per_page = 10

        start = (page - 1) * per_page
        end = start + per_page
        page_df = filtered_df.iloc[start:end]

        # Store full filtered data as CSV on server for downloads (avoid large session cookies)
        os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)
        filtered_filename = generate_filename(filter_type, extension='csv')
        filtered_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filtered_filename)
        filtered_df.to_csv(filtered_path, index=False)
        session['filtered_file'] = filtered_path
        session['filter_type'] = filter_type
        session['filter_description'] = filter_description

        return jsonify({
            'success': True,
            'message': f'Filter applied successfully',
            'filter_description': filter_description,
            'data': page_df.fillna('').to_dict('records'),
            'rows': len(page_df),
            'page': page,
            'per_page': per_page,
            'total_pages': total_pages,
            'total_rows': total_rows
        }), 200
        
    except ValueError as e:
        logger.exception('ValueError in filter_data')
        return jsonify({'success': False, 'message': str(e)}), 400
    except Exception as e:
        logger.exception('Exception in filter_data')
        return jsonify({'success': False, 'message': f'Error applying filter: {str(e)}'}), 500


@app.route('/download')
def download_filtered_data():
    """
    Download filtered data as CSV file.
    
    Returns:
        CSV file download response
    """
    try:
        # Check if filtered file path exists in session
        if 'filtered_file' not in session:
            return jsonify({'success': False, 'message': 'No filtered data to download'}), 400
        
        # Read filtered file path from session
        filtered_path = session.get('filtered_file')
        if not filtered_path or not os.path.exists(filtered_path):
            return jsonify({'success': False, 'message': 'No filtered file available for download. Apply a filter first.'}), 400

        fmt = request.args.get('format', 'csv').lower()

        if fmt == 'pdf':
            if not PDF_AVAILABLE:
                return jsonify({'success': False, 'message': 'PDF export is not available (reportlab not installed).'}), 400
            # Read CSV and generate PDF in-memory
            df = pd.read_csv(filtered_path)
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
            data = [list(df.columns)] + df.fillna('').astype(str).values.tolist()
            table = Table(data)
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#667eea')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
            ]))
            elems = [table]
            doc.build(elems)
            buffer.seek(0)

            download_name = generate_filename(session.get('filter_type', 'results'), extension='pdf')
            return send_file(buffer, mimetype='application/pdf', as_attachment=True, download_name=download_name)
        else:
            # Serve existing CSV filtered file
            return send_file(filtered_path, mimetype='text/csv', as_attachment=True, download_name=os.path.basename(filtered_path))
        
    except Exception as e:
        logger.exception('Exception in download_filtered_data')
        return jsonify({'success': False, 'message': f'Error downloading file: {str(e)}'}), 500


@app.route('/clear-session', methods=['POST'])
def clear_session():
    """
    Clear session data to upload a new file.
    
    Returns:
        JSON response with success status
    """
    try:
        session.clear()
        return jsonify({'success': True, 'message': 'Session cleared. Ready for new upload.'}), 200
    except Exception as e:
        logger.exception('Exception in clear_session')
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/clear-cache', methods=['POST'])
def clear_cache():
    """
    Clear all uploaded and downloaded files and reset session.
    """
    try:
        # Clear uploads
        if os.path.exists(app.config['UPLOAD_FOLDER']):
            for filename in os.listdir(app.config['UPLOAD_FOLDER']):
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f'Failed to delete {file_path}. Reason: {e}')

        # Clear downloads
        if os.path.exists(app.config['DOWNLOAD_FOLDER']):
            for filename in os.listdir(app.config['DOWNLOAD_FOLDER']):
                file_path = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f'Failed to delete {file_path}. Reason: {e}')
        
        # Also clear session
        session.clear()
        
        return jsonify({'success': True, 'message': 'Cache files and session cleared successfully.'}), 200
    except Exception as e:
        logger.exception('Exception in clear_cache')
        return jsonify({'success': False, 'message': f'Error clearing cache: {str(e)}'}), 500


@app.errorhandler(413)
def request_entity_too_large(error):
    """Handle file size exceeded error."""
    return jsonify({'success': False, 'message': 'File size exceeds maximum limit (16MB)'}), 413


@app.errorhandler(404)
def not_found(error):
    """Handle page not found error."""
    return render_template('404.html'), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle internal server error."""
    return jsonify({'success': False, 'message': 'Internal server error'}), 500


if __name__ == '__main__':
    # Create necessary directories
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
    
    # Run Flask app
    app.run(debug=True, host='127.0.0.1', port=5000)


