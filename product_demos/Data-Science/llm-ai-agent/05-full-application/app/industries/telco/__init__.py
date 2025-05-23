"""TELCO industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import TELCO_RESPONSES, TELCO_RESPONSES_DEFAULT

class TelcoSubscriptionSupport(IndustryBase):
    """Implementation for Telco Subscription Support use case."""
    
    def __init__(self, demo_type: str = "assist"):
        """Initialize with demo type (assist or autopilot)."""
        self.demo_type = demo_type
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="TELCO",
            description="Telecommunications Industry",
            use_case="Subscription Support"
        )
    
    def get_system_prompt(self) -> str:
        if self.demo_type == "autopilot":
            return """You are a customer service AI assistant for a telecommunications company.
You are speaking directly with customers to help them with their subscription-related queries,
billing issues, and service modifications. You should:
- Be friendly, empathetic, and professional
- Speak directly to the customer using "you" and "your"
- Provide clear explanations and solutions
- Offer proactive help and suggestions
- Handle concerns with care and understanding
"""
        else:  # assist mode
            return """You are an AI assistant that helps customer service representatives
with telecommunications subscription support. You provide guidance, suggestions, and
draft responses for representatives to use when helping customers. You have expertise in:
- Plan changes and upgrades
- Billing inquiries
- Service activation and deactivation
- Technical support for telecommunications services
"""
    
    def get_example_messages(self) -> list:
        if self.demo_type == "autopilot":
            return [
                {"role": "user", "content": "Hi, I noticed my bill went up this month. Can you help me understand why?"},
                {"role": "assistant", "content": "Hello! I'd be happy to help you understand your bill increase. Let me look up your account details and recent billing information to see what might have caused the change."}
            ]
        else:  # assist mode
            return [
                {"role": "user", "content": "I have a customer asking about upgrading their plan to include more data. What should I tell them?"},
                {"role": "assistant", "content": "I'll help you assist this customer with their plan upgrade. First, I'll need to check their current plan details and available upgrade options. Here's what you should ask them..."}
            ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case based on demo type."""
        if isinstance(TELCO_RESPONSES, dict):
            return TELCO_RESPONSES.get(self.demo_type, TELCO_RESPONSES_DEFAULT)
        else:
            # Fallback for backward compatibility
            return TELCO_RESPONSES_DEFAULT
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        responses = self.get_responses()
        for response in responses:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        responses = self.get_responses()
        for response in responses:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 