import os
import sys
import pytest

# Add the backend directory to the Python path
backend_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, backend_dir)

from app.services.agent_service import AgentService
from app.config import Config

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    """Setup environment variables for all tests"""
    config = Config()
    config.setup_databricks_env()

def test_call_endpoint():
    """Test real MLflow endpoint call with Databricks configuration"""
    # Test data
    history = [
            {
                "role": "user",
                "content": "What is 5+5"
            }
        ]
    
    # Make the actual call
    agent_service = AgentService()
    response = agent_service.call_endpoint(history)
    print("--------------------------------")
    print(response)
    assert response == 'test'
    print("--------------------------------")

if __name__ == "__main__":
    pytest.main([__file__])