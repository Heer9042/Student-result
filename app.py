"""
Main Flask application for Student Marks Management System.

Features:
- Upload CSV/PDF files with student marks
- Filter students by overall pass/fail status
- Filter students by subject-wise pass/fail
- Download filtered results as CSV
- View statistics and summaries
"""

from flask import Flask, render_template, request, send_file, jsonify, session
import pandas as pd
import os
from datetime import datetime
from config import UPLOAD_FOLDER, DOWNLOAD_FOLDER, ALLOWED_EXTENSIONS, MAX_FILE_SIZE, SECRET_KEY
from utils.file_reader import read_student_marks, validate_marks_data
from utils.data_processor import StudentMarksProcessor
import io


# Initialize Flask application
app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE


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


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handle file upload and initial data processing.
    
    Returns:
        JSON response with success status and data summary
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
        
        if not success:
            return jsonify({'success': False, 'message': message}), 400
        
        # Validate marks data
        is_valid, validation_msg = validate_marks_data(df)
        if not is_valid:
            return jsonify({'success': False, 'message': validation_msg}), 400
        
        # Store dataframe in session
        session['student_data'] = df.to_json()
        session['filename'] = file.filename
        
        # Get basic statistics
        processor = StudentMarksProcessor(df)
        stats = processor.get_overall_statistics()
        subjects = processor.subject_columns
        
        return jsonify({
            'success': True,
            'message': 'File uploaded successfully',
            'data': {
                'rows': len(df),
                'subjects': subjects,
                'statistics': stats
            }
        }), 200
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/filter', methods=['POST'])
def filter_data():
    """
    Apply filters to student data based on request parameters.
    
    Returns:
        JSON response with filtered data
    """
    try:
        # Check if data exists in session
        if 'student_data' not in session:
            return jsonify({'success': False, 'message': 'No data loaded. Please upload a file first.'}), 400
        
        # Reconstruct dataframe from session
        df = pd.read_json(session['student_data'])
        processor = StudentMarksProcessor(df)
        
        # Get filter type and apply appropriate filter
        filter_type = request.json.get('filter_type')
        filtered_df = None
        filter_description = ""
        
        if filter_type == 'overall_pass':
            filtered_df = processor.filter_passed_students()
            filter_description = "Students who passed all subjects"
            
        elif filter_type == 'overall_fail':
            filtered_df = processor.filter_failed_students()
            filter_description = "Students who failed at least one subject"
            
        elif filter_type == 'subject_pass':
            subject = request.json.get('subject')
            if not subject:
                return jsonify({'success': False, 'message': 'Subject not specified'}), 400
            filtered_df = processor.filter_subject_wise_pass(subject)
            filter_description = f"Students who passed {subject}"
            
        elif filter_type == 'subject_fail':
            subject = request.json.get('subject')
            if not subject:
                return jsonify({'success': False, 'message': 'Subject not specified'}), 400
            filtered_df = processor.filter_subject_wise_fail(subject)
            filter_description = f"Students who failed {subject}"
            
        elif filter_type == 'summary':
            summary_df = processor.get_subject_wise_summary()
            session['filtered_data'] = summary_df.to_json()
            session['filter_type'] = 'summary'
            return jsonify({
                'success': True,
                'message': 'Summary generated successfully',
                'filter_description': 'Subject-wise Pass/Fail Summary',
                'data': summary_df.to_dict('records'),
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
        
        else:
            return jsonify({'success': False, 'message': 'Invalid filter type'}), 400
        
        if filtered_df is None or filtered_df.empty:
            return jsonify({
                'success': True,
                'message': 'No records found matching the filter criteria',
                'data': [],
                'rows': 0,
                'filter_description': filter_description
            }), 200
        
        # Store filtered data in session for download
        session['filtered_data'] = filtered_df.to_json()
        session['filter_type'] = filter_type
        session['filter_description'] = filter_description
        
        return jsonify({
            'success': True,
            'message': f'Filter applied successfully',
            'filter_description': filter_description,
            'data': filtered_df.to_dict('records'),
            'rows': len(filtered_df)
        }), 200
        
    except ValueError as e:
        return jsonify({'success': False, 'message': str(e)}), 400
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error applying filter: {str(e)}'}), 500


@app.route('/download')
def download_filtered_data():
    """
    Download filtered data as CSV file.
    
    Returns:
        CSV file download response
    """
    try:
        # Check if filtered data exists in session
        if 'filtered_data' not in session:
            return jsonify({'success': False, 'message': 'No filtered data to download'}), 400
        
        # Reconstruct dataframe from session
        df = pd.read_json(session['filtered_data'])
        
        # Generate filename based on filter type
        filter_type = session.get('filter_type', 'results')
        filename = generate_filename(filter_type)
        filepath = os.path.join(app.config['DOWNLOAD_FOLDER'], filename)
        
        # Create downloads folder if it doesn't exist
        os.makedirs(app.config['DOWNLOAD_FOLDER'], exist_ok=True)
        
        # Save filtered data to CSV
        df.to_csv(filepath, index=False)
        
        # Send file for download
        return send_file(
            filepath,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
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
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


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
