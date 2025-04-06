from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles 
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from .routes import agent
import os
import traceback
import time
import logging
import yaml
from pathlib import Path
from typing import Dict

# Development environment settings

# Set up application logging
logger = logging.getLogger("app")

def load_config() -> Dict:
    """Load configuration from YAML files with fallback to default config."""
    config_dir = Path(__file__).parent.parent
    local_config = config_dir / 'app_local.yaml'
    default_config = config_dir / 'app.yaml'
    
    try:
        config_file = local_config if local_config.exists() else default_config
        with open(config_file) as f:
            return yaml.safe_load(f).get('env', {})
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        return {'ENV': 'prod'}  # Safe default

def get_config_value(config: list, key: str, default_value: str = '') -> str:
    """Get a configuration value from the config list of dictionaries"""
    return next((item['value'] for item in config if item['name'] == key), default_value)

# Load configuration
config = load_config()
environment = get_config_value(config, 'ENV', 'prod')


app = FastAPI(title="AI Agent demo")

# Request-Response logging middleware
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = id(request)
        method = request.method
        endpoint = request.url.path
        
        # Log request
        logger.info(f"Request {request_id}: {method} {endpoint} started")
        
        # Process request and measure time
        start_time = time.time()
        
        try:
            response = await call_next(request)
            process_time = time.time() - start_time
            
            # Log successful response
            logger.info(f"{method} {endpoint} completed with status {response.status_code} in {process_time:.3f}s")
            
            return response
        except Exception as e:
            process_time = time.time() - start_time
            logger.error(f"{method} {endpoint} failed in {process_time:.3f}s: {str(e)}\n{traceback.format_exc()}")
            raise

# Add the request logging middleware
app.add_middleware(RequestLoggingMiddleware)

# Add CORS middleware only in development environment
if environment == 'dev':
    print("STARTING IN DEV MODE - This won't work in a deployed environment on Databricks. If you see this message in your databricks logs, change the ENV to prod in the app.yaml file.")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://127.0.0.1:5173", "http://localhost:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    @app.get("/")
    async def root():
        return {"message": "Databricks GenAI API"} 
else:
    print("STARTING IN PROD MODE - will serve the /dist folder. This will work in a deployed environment on Databricks.")

# Initialize API routes first
app.include_router(agent.router, prefix="/api/agent")

# Then mount static files in prod mode
if environment == 'prod':
    try:
        target_dir = "dist"
        app.mount("/", StaticFiles(directory=target_dir, html=True), name="site")
    except:
        print('ERROR - dist folder not found')

# Global exception handler for all unhandled exceptions
@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    method = request.method
    endpoint = request.url.path
    logger.error(f"{method} {endpoint} error: {str(exc)}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error : {str(exc)} Check logs for details."}
    )

# Handle validation errors
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    method = request.method
    endpoint = request.url.path
    logger.error(f"{method} {endpoint} validation error: {str(exc)}")
    return JSONResponse(
        status_code=422,
        content={"detail": f"Validation error: {str(exc)}"}
    )

# Handle HTTP exceptions
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    method = request.method
    endpoint = request.url.path
    logger.error(f"{method} {endpoint} HTTP {exc.status_code}: {exc.detail}")
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )
