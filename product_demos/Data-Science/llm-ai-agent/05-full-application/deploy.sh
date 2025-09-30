#!/bin/bash

# Accept parameters
APP_FOLDER_IN_WORKSPACE=${1:-"/Workspace/dbdemos/dbdemos-genai/dbdemos-genai-agent-support"}
LAKEHOUSE_APP_NAME=${2:-"dbdemos-genai-agent-support"}

# Frontend build and import
(
  cd frontend
  npm run build
  rm -rf ../static/
  mv dist ../static
  databricks workspace delete "$APP_FOLDER_IN_WORKSPACE/static" --recursive --profile WEST
  databricks workspace import-dir ../static "$APP_FOLDER_IN_WORKSPACE/static" --overwrite --profile WEST
) &

# Backend packaging
(
  rm -rf build
  mkdir -p build
  rsync -av \
    --exclude='**/__pycache__/' \
    --exclude='**/app_local.yaml' \
    --exclude='frontend' \
    --exclude='**/app_local.yaml.example' \
    --exclude='**/*.pyc' \
    --exclude='.*' \
    --exclude='tests' \
    --exclude='deploy.sh' \
    --exclude='test' \
    --exclude='build' \
    --exclude='local_conf*' \
    ./ build/
  if [ -f app_prod.py ]; then
    cp app_prod.py build/app.py
  fi
  databricks workspace delete "$APP_FOLDER_IN_WORKSPACE/app" --recursive --profile WEST
  databricks workspace import-dir build "$APP_FOLDER_IN_WORKSPACE" --overwrite --profile WEST
  rm -rf build
) &

# Wait for both background processes to finish
wait

# Deploy the application
databricks apps deploy "$LAKEHOUSE_APP_NAME" --profile WEST

# Print the app page URL
echo "Open the app page for details and permission: https://e2-demo-west.cloud.databricks.com/apps/$LAKEHOUSE_APP_NAME"
