"""
Configuration settings for the Student Marks System
"""
import os

# Flask configuration
FLASK_ENV = os.getenv('FLASK_ENV', 'development')
DEBUG = FLASK_ENV == 'development'
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')

# File upload configuration
UPLOAD_FOLDER = 'uploads'
DOWNLOAD_FOLDER = 'downloads'
ALLOWED_EXTENSIONS = {'csv', 'pdf'}
MAX_FILE_SIZE = 16 * 1024 * 1024  # 16MB

# Marking configuration
PASS_THRESHOLD = 40  # Marks required to pass a subject (out of 100)

# Create folders if they don't exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
