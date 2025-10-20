# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains all the notebooks for [dbdemos](https://github.com/databricks-demos/dbdemos), which are designed to be packaged and distributed as interactive Databricks demos. These are Databricks notebooks (not standard Jupyter notebooks) that run on the Databricks platform and demonstrate end-to-end solutions across various industries and use cases.

**Key Point**: This repository contains source notebooks only. The actual packaging and distribution is handled by the separate dbdemos repository. Only fork and modify this content if you need direct access to the notebook source code - otherwise, prefer using the dbdemos package.

## Critical: Independent Demo Structure

**⚠️ MOST IMPORTANT: Each folder containing a `_resources/` subdirectory is an independent, self-contained demo with NO links between demos.**

- There are approximately **40 independent demos** in this repository
- Each demo can be installed and run completely independently via dbdemos
- Demos do NOT share code, data, or dependencies with each other (except the root `_resources/00-global-setup-v2.py`)
- When making changes, only modify files within a single demo's folder structure
- Each demo has its own:
  - `_resources/` folder with setup and bundle configuration
  - `config.py` with catalog/schema/volume settings
  - Introduction notebook (`00-*.py` or `00-*.sql`)
  - Numbered folders for workflow stages (01-Data-Ingestion, 02-Data-Science-ML, etc.)

**Example independent demos**:
- `demo-FSI/lakehouse-fsi-smart-claims/` ← Independent demo
- `demo-FSI/lakehouse-fsi-fraud-detection/` ← Independent demo (no relation to smart-claims)
- `product_demos/Data-Science/chatbot-rag-llm/` ← Independent demo
- `product_demos/Data-Science/ai-agent/` ← Independent demo (no relation to chatbot-rag-llm)

**How to identify which demo you're working in**:
- Look for the nearest parent directory containing a `_resources/` folder
- That directory is the demo boundary - all work should stay within it
- To list all independent demos: `find . -type d -name "_resources" | grep -v "^./_resources$"`

## Architecture & Organization

### Directory Structure

The repository is organized into three main categories:

1. **Industry-Specific Demos** (`demo-*/`)
   - `demo-FSI/`: Financial Services Industry demos (fraud detection, credit decisioning, smart claims)
   - `demo-HLS/`: Healthcare & Life Sciences (patient readmission)
   - `demo-manufacturing/`: Manufacturing demos (IoT platform, wind turbine predictive maintenance)
   - `demo-retail/`: Retail demos (customer 360)
   - `aibi/`: AI Business Intelligence demos for various verticals (customer support, marketing, sales, etc.)

2. **Product Demos** (`product_demos/`)
   - Technical product capabilities organized by feature area
   - Categories: Data Science, Delta Lake, Spark Declarative Pipelines, Unity Catalog, DBSQL, streaming, etc.

3. **Shared Resources** (root `_resources/` only)
   - Root `_resources/00-global-setup-v2.py` contains the `DBDemos` class with common setup functions
   - This is the ONLY shared code across demos
   - Every demo also has its own `_resources/` folder - these are NOT shared and are demo-specific

### Notebook Architecture Patterns

Each demo follows a consistent structure:

1. **Introduction Notebook** (`00-*-Introduction.*`): Overview and architecture explanation
2. **Data Ingestion** (`01-Data-Ingestion/` or `01-Data-ingestion/`): Spark Declarative Pipelines (SDP), streaming ingestion
3. **Data Governance** (`02-Data-Governance/` or `02-Data-governance/`): Unity Catalog, data quality, ACLs
4. **BI/Analytics** (`03-BI-*`): DBSQL dashboards and queries
5. **Data Science/ML** (`02-Data-Science-ML/` or `04-Data-Science-ML/`): Model training, batch scoring, MLflow
6. **Generative AI** (`05-Generative-AI/`): RAG applications, AI agents, LLM integration
7. **Workflow Orchestration** (`05-Workflow-Orchestration/` or `06-Workflow-orchestration/`): End-to-end job orchestration
8. **Config File** (`config.py`): Catalog, schema, and volume configuration for each demo

### Key Technical Components

**Databricks Notebooks Format**:
- These are `.py` and `.sql` files with special `# MAGIC` comments that define Markdown cells
- Format: `# MAGIC %md` for markdown cells, regular code for Python/SQL cells
- `# COMMAND ----------` separates cells
- These are NOT standard Jupyter notebooks (though some .ipynb files exist for data generation)

**Unity Catalog Integration**:
- All demos use Unity Catalog (UC) for governance
- Configured via `catalog` and `schema` variables in `config.py`
- Volumes are used for file storage: `/Volumes/{catalog}/{schema}/{volume_name}/`
- Common pattern: `catalog = "main__build"` for build/test environments

**Spark Declarative Pipelines (SDP)**:
- Primary ETL framework used across demos (formerly known as Delta Live Tables/DLT and Lakeflow Declarative Pipelines/LDP)
- Python SDP: `@dlt.table()` decorator with `spark.readStream` and `cloudFiles`
- SQL SDP: `CREATE OR REFRESH STREAMING TABLE` statements
- Supports both batch and streaming data ingestion
- Common sources: JSON, CSV, Parquet files loaded via Auto Loader (`cloudFiles`)

**Data Setup Pattern**:
- `DBDemos.setup_schema(catalog, db, reset_all_data, volume_name)` - Primary initialization function
- Creates catalog, schema, and volumes if they don't exist
- Handles permissions for shared demos (`account users`)
- Reset capability to clear all data for fresh runs

**ML & AI Patterns**:
- MLflow for experiment tracking and model registry
- Model serving via Databricks Model Serving endpoints
- Vector Search for RAG applications (`VECTOR_SEARCH_ENDPOINT_NAME`)
- Mosaic AI Agent Framework for building AI agents with tools
- Feature Store for ML feature management

**Bundle Configuration**:
- Each demo has `_resources/bundle_config.py` with metadata
- Defines demo name, category, description, notebooks, dependencies
- Used by dbdemos packaging system to create installable demos

## Common Development Tasks

### Running Notebooks Locally

These notebooks are designed to run on Databricks. You cannot execute them directly in a local Python environment. To work with them:

1. **Databricks Workspace**: Upload to a Databricks workspace and run there
2. **Databricks CLI**: Use `databricks workspace import` to sync notebooks
3. **VS Code with Databricks Extension**: Edit locally and sync to workspace

### Testing Changes

Before submitting changes:

1. **Identify the demo boundary**: Find the folder with `_resources/` that contains your changes
2. Run all notebooks **within that demo only** in a clean Databricks environment
3. Verify notebooks execute without errors end-to-end within that demo
4. Test with `reset_all_data=true` to ensure fresh installation works
5. Verify Unity Catalog resources are created correctly
6. **Do NOT test across multiple demos** - changes to one demo should never affect another

### Configuration Changes

Each demo's configuration is in its `config.py`:
- `catalog`: Unity Catalog name (typically `"main__build"` for development)
- `schema` or `dbName` or `db`: Schema/database name
- `volume_name`: Volume for storing files
- **Important**: Some SDP notebooks have hardcoded catalog/schema that must match `config.py`

### Common Patterns in Code

**Setup Pattern**:
```python
%run ./_resources/00-setup $catalog=<catalog> $db=<schema>
```

**Unity Catalog Table Reference**:
```python
spark.table(f"`{catalog}`.`{schema}`.`{table_name}`")
```

**SDP Python**:
```python
@dlt.table(comment="Description")
def table_name():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load(f"/Volumes/{catalog}/{db}/{volume_name}/path")
```

**SDP SQL**:
```sql
CREATE OR REFRESH STREAMING TABLE table_name
COMMENT "Description"
AS SELECT * FROM cloud_files("/Volumes/catalog/schema/volume/path", "json")
```

**Model Permissions**:
```python
DBDemos.set_model_permission(model_name, "EXECUTE", "account users")
DBDemos.set_model_endpoint_permission(endpoint_name, "CAN_QUERY", "account users")
```

## Git Workflow

From the README:
1. Open a bug/feature request first
2. If at Databricks, discuss on Slack before proceeding
3. Fork and create a feature branch: `git checkout -b FEATURENAME`
4. Make changes and test thoroughly in a clean environment
5. Commit with meaningful message: `git commit -a`
6. Push: `git push origin FEATURENAME`
7. Create Pull Request
8. Optional: Package with dbdemos (may be automated in future)

## Important Notes

- **Unity Catalog Only**: All demos require Unity Catalog - `hive_metastore` and `spark_catalog` are not supported
- **Databricks Runtime**: Minimum DBR version specified in `dbutils.widgets.text("min_dbr_version", "X.X")`
- **Analytics Tracking**: Many notebooks include 1px tracking image for usage analytics - can be removed to disable
- **External Libraries**: Check `config.py` or notebook headers for license information on external dependencies
- **Databricks SDK**: Used for programmatic access to Databricks APIs (grants, serving endpoints, vector search)
- **File Paths**: All file references use Unity Catalog Volumes (`/Volumes/...`) not legacy DBFS paths
- **Shared Demos**: When `catalog == 'dbdemos'`, special grants are applied for `account users` group

## Data Science & AI Specific

**RAG Applications**:
- Vector Search endpoints defined in config
- Knowledge base ingestion via chunking and embedding
- LangChain integration for retrieval chains
- Agent Framework for evaluation and deployment

**MLOps**:
- Model training notebooks create MLflow experiments
- Batch scoring notebooks load models from registry
- Model serving deployment via REST API or SDK
- Feature engineering stored in Feature Store tables

**Generative AI**:
- Foundation models accessed via Databricks Model Serving
- Custom tools and functions for AI agents
- Evaluation notebooks with LLM-as-judge patterns
- Lakehouse Apps for deploying chatbot UIs
