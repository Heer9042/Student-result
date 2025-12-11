# Quick Start Guide

## Installation (Windows PowerShell)

```powershell
# Navigate to project directory
cd t:\python\student_marks_system

# Create virtual environment (optional but recommended)
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
```

## Access the Application

1. Open your web browser
2. Go to: **http://127.0.0.1:5000**
3. You should see the Student Marks Management interface

## First Time Setup

### Option A: Use Sample Data
1. In the upload section, click "Choose File"
2. Select `sample_data.csv` from the project folder
3. Click upload
4. Try different filters to explore features

### Option B: Upload Your Own Data
1. Prepare a CSV file with format:
   ```
   Student Name, Mathematics, English, Science, History
   John Smith, 85, 78, 92, 88
   Emma Johnson, 92, 95, 88, 91
   ```
2. Upload the file using the web interface
3. Apply filters and download results

## Common Tasks

### Find all students who passed
1. Upload CSV file
2. Click "✅ Passed Students (All Subjects)"
3. Click "⬇️ Download as CSV"

### Find students failing a specific subject
1. Upload CSV file
2. Click "🚫 Fail in Subject"
3. Select the subject from dropdown
4. Click "⬇️ Download as CSV"

### Get class statistics
1. Upload CSV file
2. Click "🔢 Statistics"
3. View the statistics displayed

## Stopping the Application

Press `Ctrl + C` in the terminal running the Flask app

## Deactivating Virtual Environment (if used)

```powershell
deactivate
```

## Tips

- Use CSV format for best results
- Ensure marks are between 0-100
- First column should be "Student Name"
- File size should not exceed 16MB
- Pass threshold is 40 marks by default (configurable)

---

For more details, see README.md
