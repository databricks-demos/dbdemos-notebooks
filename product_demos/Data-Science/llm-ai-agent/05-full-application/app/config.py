import yaml
import os
from pathlib import Path
from typing import Dict, Optional

class Config:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._config = self._load_config()
            self._initialized = True

    def _load_config(self) -> Dict:
        """Load configuration from YAML files with fallback to default config."""
        config_dir = Path(__file__).parent.parent
        local_config = config_dir / 'app_local.yaml'
        default_config = config_dir / 'app.yaml'
        
        try:
            config_file = local_config if local_config.exists() else default_config
            with open(config_file) as f:
                return yaml.safe_load(f).get('env', {})
        except Exception as e:
            print(f"Failed to load configuration: {e}")
            return {'ENV': 'prod'}  # Safe default

    def get_value(self, key: str, default_value: str = '') -> str:
        """Get a configuration value from the config list of dictionaries"""
        return next((item['value'] for item in self._config if item['name'] == key), default_value)

    def setup_databricks_env(self):
        """Set up Databricks environment variables"""
        os.environ['DATABRICKS_HOST'] = self.get_value('DATABRICKS_HOST')
        os.environ['DATABRICKS_TOKEN'] = self.get_value('DATABRICKS_TOKEN')

    @property
    def environment(self) -> str:
        """Get the current environment (dev/prod)"""
        return self.get_value('ENV', 'prod') 