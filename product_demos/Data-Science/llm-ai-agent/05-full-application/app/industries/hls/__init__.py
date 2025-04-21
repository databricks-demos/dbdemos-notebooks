"""Healthcare and Life Sciences (HLS) industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import HLS_RESPONSES

class HealthcareSupport(IndustryBase):
    """Implementation for Healthcare Support use case."""
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="HLS",
            description="Healthcare and Life Sciences Industry",
            use_case="Member Support"
        )
    
    def get_system_prompt(self) -> str:
        return """You are an AI assistant specialized in healthcare member support.
Your role is to help members with their healthcare-related queries, including:
- Claims status and processing
- Benefits verification and explanation
- Provider network information
- Prescription drug coverage
- Prior authorization requirements
- Deductible and out-of-pocket tracking

You understand healthcare terminology, insurance concepts, and maintain strict HIPAA compliance
in all interactions. You're knowledgeable about:
- Health insurance plans and benefits
- Medical billing and claims processing
- Healthcare provider networks
- Prescription drug formularies
- Member rights and responsibilities
"""
    
    def get_example_messages(self) -> list:
        return [
            {"role": "user", "content": "How do I find out if my doctor is in-network?"},
            {"role": "assistant", "content": "I can help you check if your doctor is in our network. Could you please provide the doctor's name and location? I'll search our provider network database to confirm their participation status and any network-specific details."}
        ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case."""
        return HLS_RESPONSES
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        for response in HLS_RESPONSES:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        for response in HLS_RESPONSES:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 