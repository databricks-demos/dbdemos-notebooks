"""Retail and Consumer Goods (RCG) industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import RCG_RESPONSES

class RetailOrderSupport(IndustryBase):
    """Implementation for Retail Order Support use case."""
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="RCG",
            description="Retail and Consumer Goods Industry",
            use_case="Order Support"
        )
    
    def get_system_prompt(self) -> str:
        return """You are an AI assistant specialized in retail order support.
Your role is to help customer service representatives with order-related queries and issues, including:
- Order status and tracking
- Product returns and exchanges
- Technical product support
- Customer account management
- Loyalty program benefits
- Special offers and promotions

You understand retail operations, customer service best practices, and maintain
strict customer privacy in all interactions. You're knowledgeable about:
- Order processing systems
- Product documentation and troubleshooting
- Return policies and procedures
- Customer loyalty programs
- Promotional campaigns and discounts
- Customer satisfaction metrics
"""
    
    def get_example_messages(self) -> list:
        return [
            {"role": "user", "content": "Can you help me look up an order status for a customer?"},
            {"role": "assistant", "content": "I'll help you check the order status. Could you please provide the order number? Once you do, I can look up the details including items ordered, shipping status, and any special notes or issues."}
        ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case."""
        return RCG_RESPONSES
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        for response in RCG_RESPONSES:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        for response in RCG_RESPONSES:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 