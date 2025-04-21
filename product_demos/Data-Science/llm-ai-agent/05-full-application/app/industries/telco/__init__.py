"""TELCO industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import TELCO_RESPONSES

class TelcoSubscriptionSupport(IndustryBase):
    """Implementation for Telco Subscription Support use case."""
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="TELCO",
            description="Telecommunications Industry",
            use_case="Subscription Support"
        )
    
    def get_system_prompt(self) -> str:
        return """You are an AI assistant specialized in telecommunications subscription support.
Your role is to help customers with their subscription-related queries, troubleshooting,
and service modifications. You have expertise in:
- Plan changes and upgrades
- Billing inquiries
- Service activation and deactivation
- Technical support for telecommunications services
"""
    
    def get_example_messages(self) -> list:
        return [
            {"role": "user", "content": "I want to upgrade my current plan to include more data."},
            {"role": "assistant", "content": "I'll help you upgrade your plan. First, I'll need to check your current plan details and available upgrade options. Could you please confirm your current plan name or monthly data allowance?"}
        ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case."""
        return TELCO_RESPONSES
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        for response in TELCO_RESPONSES:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        for response in TELCO_RESPONSES:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 