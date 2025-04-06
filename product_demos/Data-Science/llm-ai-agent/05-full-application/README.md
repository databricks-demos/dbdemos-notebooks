# Databricks Intelligent Document

A web application for extracting and validating entities from PDF documents using React, FastAPI, and PostgreSQL.

## Prerequisites

- Python 3.11+ (with Conda)
- Node.js 18+
- PostgreSQL 14+
- Tesseract OCR 5.0+

## Initial Setup

### Conda Environment Setup

1. Install Miniconda or Anaconda if not already installed:
   - [Miniconda](https://docs.conda.io/en/latest/miniconda.html)
   - [Anaconda](https://www.anaconda.com/products/distribution)

2. Create and activate the conda environment:
   ```
   conda create -n databricks-intelligent-document python=3.11
   conda activate databricks-intelligent-document
   ```

3. Clone the repository:
   ```
   git clone https://github.com/databricks-solutions/databricks-intelligent-document.git
   cd databricks-intelligent-document
   ```

### Tesseract OCR Setup

1. Install Tesseract OCR:
   - **Windows**: Download and install from [UB Mannheim](https://github.com/UB-Mannheim/tesseract/wiki)
   - **macOS**: `brew install tesseract`
   - **Linux**: `sudo apt install tesseract-ocr`

2. Verify the installation:
   ```
   tesseract --version
   ```

3. Make sure the Tesseract executable is in your PATH or set the path in your code:
   ```python
   pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'  # Windows example
   ```

## Project Structure

databricks-intelligent-document/
├── backend/                 # FastAPI backend
│   ├── app/                # Application code
│   │   ├── models/        # Database models
│   │   ├── routes/        # API endpoints
│   │   ├── services/      # Business logic
│   │   ├── config.py      # Configuration
│   │   ├── database.py    # Database connection
│   │   └── main.py        # Application entry point
│   └── requirements.txt    # Python dependencies
├── frontend/               # React frontend
│   ├── public/            # Static files
│   ├── src/               # Source code
│   │   ├── components/    # React components
│   │   ├── services/      # API services
│   │   └── App.tsx        # Main application
│   └── package.json       # Node.js dependencies
└── pdf_storage/            # PDF storage directory

## Setup Instructions

### Database Setup

1. Install PostgreSQL if not already installed:
   - [PostgreSQL Downloads](https://www.postgresql.org/download/)

2. Create a database:
   ```
   CREATE DATABASE intelligent_document;
   ```

### Backend Setup

1. Install backend dependencies:
   ```
   cd backend
   pip install -r requirements.txt
   ```

2. Create a `.env` file in the `backend/` directory with the following variables (get the value from Databricks managed Database Instances):
   - `DB_HOST`: PostgreSQL host (default: localhost)
   - `DB_PORT`: PostgreSQL port (default: 5432) 
   - `DB_USER`: PostgreSQL username
   - `DB_PASSWORD`: PostgreSQL password
   - `DB_NAME`: Database name (default: intelligent_document)
   - `PDF_STORAGE_PATH`: Path to PDF storage directory (default: ../pdf_storage)

   Example `.env` file:
   ```
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=your_password
   DB_NAME=intelligent_document
   PDF_STORAGE_PATH=../pdf_storage
   ```

3. Start the FastAPI server:
   ```
   cd backend
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

   Note: The application will automatically create database tables if they don't exist.
   
   To reset the database (drop and recreate all tables):
   ```
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 --reset-db
   ```

### Frontend Setup

1. Install dependencies:
   ```
   cd frontend
   npm install
   ```

2. Create a `.env` file in the `frontend/` directory:
   ```
   REACT_APP_API_URL=http://localhost:8000/api
   ```

3. Start the development server:
   ```
   cd frontend
   npm start
   ```

### Frontend Development

The frontend is built with React, TypeScript, and Material-UI. Here are some key components:

1. **Main Views**:
   - `DocumentView.tsx`: Main document viewing and entity extraction interface
   - `Performance.tsx`: Dashboard for viewing extraction performance metrics

2. **API Services**:
   - `api.ts`: Contains all API calls to the backend

3. **State Management**:
   - React's built-in state management with hooks
   - Context API for global state where needed

4. **Styling**:
   - Material-UI components and theming
   - Responsive design for different screen sizes

The application will be available at:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

## Usage

1. Configure Folder:
   - Click "Configure Schema" in the top bar
   - Define the entities you want to extract
   - Save the configuration

2. Process PDFs:
   - Place PDFs in the configured folder
   - Extracted entities appear in the left panel
   - Validate each entity by clicking the checkmark
   - Add missing entities using the "Add Entity" button

3. View Performance:
   - Click "View Performance" to see extraction metrics
   - Review precision, recall, and F1 scores
   - Analyze common errors and entity-specific metrics

## Development Notes

### Adding New Entity Types

1. Update the schema configuration:
   - Use the UI to add new entity types
   - Or modify the database directly using SQL

2. Update the extraction service:
   - Modify `backend/app/services/extraction_service.py`
   - Add new entity extraction logic

### Database Management

The application uses a simple approach to database management with SQL scripts:

1. **Automatic Table Creation**: When the application starts, it checks if the required tables exist and creates them if needed using SQLAlchemy models.

2. **Resetting the Database**: You can reset the database (drop and recreate all tables) in two ways:

   a. When starting the application:
   ```
   cd backend
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 --reset-db
   ```

   b. Using the reset script:
   ```
   cd backend
   python scripts/reset_db.py
   ```

   ⚠️ **WARNING**: Both methods will delete all data in the database. Use with caution.

3. **Manual Database Operations**: You can also run database operations directly:

   ```python
   # From the Python shell or scripts
   from app.db_init import create_tables, drop_tables, reset_database
   
   # Create tables if they don't exist
   create_tables()
   
   # Drop all tables
   drop_tables()
   
   # Reset the database (drop and recreate)
   reset_database()
   ```