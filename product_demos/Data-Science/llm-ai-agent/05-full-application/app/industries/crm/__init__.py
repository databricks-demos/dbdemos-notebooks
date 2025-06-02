"""CRM industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import CRM_RESPONSES, CRM_RESPONSES_DEFAULT

class CrmSupport(IndustryBase):
    """Implementation for CRM Support use case."""
    
    def __init__(self, demo_type: str = "assist"):
        """Initialize with demo type (assist or autopilot)."""
        self.demo_type = demo_type
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="CRM",
            description="Customer Relationship Management",
            use_case="CRM Support & Optimization"
        )
    
    def get_system_prompt(self) -> str:
        if self.demo_type == "autopilot":
            return """You are an AI CRM consultant and sales optimization expert.
You are speaking directly with business professionals to help them with CRM implementation,
sales process optimization, lead management, and customer relationship strategies. You should:
- Be professional, knowledgeable, and consultative
- Speak directly to the business user using "you" and "your"
- Provide actionable recommendations and best practices
- Focus on ROI, efficiency improvements, and business growth
- Offer specific, implementable solutions and strategies
"""
        else:  # assist mode
            return """You are an AI assistant that helps sales professionals, account managers,
and customer success teams with CRM-related tasks and strategies. You provide guidance,
recommendations, and actionable insights for teams managing customer relationships. You have expertise in:
- Lead qualification and scoring
- Sales pipeline management
- Customer retention strategies
- CRM system optimization
- Sales performance analytics
- Account management best practices
"""
    
    def get_example_messages(self) -> list:
        if self.demo_type == "autopilot":
            return [
                {"role": "user", "content": "Our sales team is struggling with lead qualification. What CRM features would help us identify better prospects?"},
                {"role": "assistant", "content": "I can help you implement an effective lead qualification system! Let me analyze your current process and recommend specific CRM features and scoring models that will help your team focus on high-quality prospects."}
            ]
        else:  # assist mode
            return [
                {"role": "user", "content": "I have a client threatening to cancel due to service issues. What's the best approach to handle this escalation?"},
                {"role": "assistant", "content": "This is a critical situation that requires immediate action. Let me analyze the client's history and provide you with a data-driven retention strategy and specific steps to address their concerns."}
            ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case based on demo type."""
        if isinstance(CRM_RESPONSES, dict):
            return CRM_RESPONSES.get(self.demo_type, CRM_RESPONSES_DEFAULT)
        else:
            # Fallback for backward compatibility
            return CRM_RESPONSES_DEFAULT
    
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