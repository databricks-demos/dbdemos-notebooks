# Databricks GenAI Agent Demo

A web application demonstrating Databricks GenAI capabilities with a chat interface powered by FastAPI and React.

## Local Development Setup

### Python Environment Setup

1. Install Miniconda (recommended) or Anaconda if not already installed:
   - Download Miniconda from [here](https://docs.conda.io/en/latest/miniconda.html)
   - Or Anaconda from [here](https://www.anaconda.com/download)

2. Create and activate a new conda environment:
```bash
# Create a new environment with Python 3.11
conda create -n dbdemos-agent python=3.11

# Activate the environment
conda activate dbdemos-agent
```

### Python Backend Setup

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Create `app_local.yaml` from the example file and configure your Databricks credentials:
```bash
cp app_local.yaml.example app_local.yaml
# Edit app_local.yaml with your Databricks credentials
```

3. Start the FastAPI server:
```bash
python -m uvicorn app.main:app --reload
```

The backend will be available at:
- API: http://localhost:8000
- API Documentation: http://localhost:8000/docs

### Frontend Setup

1. Install Node.js (v18+ recommended) if not already installed:
   - Download from [Node.js website](https://nodejs.org/)
   - Or use nvm (Node Version Manager)

2. Install frontend dependencies:
```bash
cd frontend
npm install
```

3. Start the development server:
```bash
npm run dev
```

The frontend will be available at http://localhost:5173

### Local Development Tips

1. **IDE Setup**: We recommend using CursorAI for development:
   - Provides intelligent code completion
   - Built-in AI assistance for development
   - Download from [cursor.sh](https://cursor.sh)

2. **Hot Reload**:
   - Backend: The `--reload` flag enables auto-reload on code changes
   - Frontend: Vite provides fast hot module replacement

3. **Testing**:
   - Backend tests: `pytest`
   - Frontend tests: `cd frontend && npm test`

## Deployment

The application can be deployed to Databricks using the provided `deploy.sh` script:

```bash
# Deploy to a specific workspace folder and app name
./deploy.sh "/Workspace/your/path" "your-app-name"

# Use defaults
./deploy.sh
```

The deployment process:
1. Builds the frontend (`npm run build`)
2. Packages the Python code
3. Uploads both to your Databricks workspace
4. Deploys as a Databricks Application

### Manual Deployment Steps

If you prefer to deploy manually:

1. Build the frontend:
```bash
cd frontend
npm run build
```

2. Copy the frontend build to the static folder:
```bash
cp -r frontend/dist/* static/
```

3. Package and upload the Python code:
```bash
# Create a clean build directory
rm -rf build
mkdir -p build

# Copy Python files (excluding dev files)
rsync -av \
  --exclude='**/__pycache__/' \
  --exclude='**/app_local.yaml' \
  --exclude='**/*.pyc' \
  --exclude='.*' \
  --exclude='build' \
  --exclude='frontend' \
  app/ build/

# Upload to Databricks
databricks workspace import-dir build "/Workspace/your/path" --overwrite
```

4. Deploy the application in your Databricks workspace

## Project Structure

```
├── app/                    # Python FastAPI application
│   ├── routes/            # API endpoints
│   ├── services/          # Business logic
│   └── main.py           # Application entry
├── frontend/              # React frontend
│   ├── src/
│   │   ├── components/   # React components
│   │   └── App.tsx      # Main application
│   └── package.json
├── static/                # Static files served by FastAPI
├── requirements.txt       # Python dependencies
└── deploy.sh             # Deployment script
```