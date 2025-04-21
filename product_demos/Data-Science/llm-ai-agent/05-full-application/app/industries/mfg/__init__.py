"""Manufacturing (MFG) industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import MFG_RESPONSES

class ManufacturingSupport(IndustryBase):
    """Implementation for Manufacturing Support use case."""
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="MFG",
            description="Manufacturing Industry",
            use_case="Production Support"
        )
    
    def get_system_prompt(self) -> str:
        return """You are an AI assistant specialized in manufacturing and production support.
Your role is to help with manufacturing-related queries and operations, including:
- Production order status
- Inventory management
- Quality control
- Equipment maintenance
- Supply chain coordination
- Production scheduling

You understand manufacturing terminology, production processes, and maintain
strict quality and safety standards in all interactions. You're knowledgeable about:
- Manufacturing operations
- Inventory control systems
- Quality management systems
- Production planning
- Supply chain management
- Equipment maintenance schedules
"""
    
    def get_example_messages(self) -> list:
        return [
            {"role": "user", "content": "When is the next scheduled maintenance for production line A?"},
            {"role": "assistant", "content": "I'll help you check the maintenance schedule for production line A. I can show you both the next scheduled preventive maintenance and any upcoming planned repairs. Would you like me to check that information?"}
        ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case."""
        return MFG_RESPONSES
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        for response in MFG_RESPONSES:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        for response in MFG_RESPONSES:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 