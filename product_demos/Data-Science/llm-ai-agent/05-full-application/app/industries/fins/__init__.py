"""Financial Services (FINS) industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import FINS_RESPONSES

class FinancialServicesSupport(IndustryBase):
    """Implementation for Financial Services Support use case."""
    
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="FINS",
            description="Financial Services Industry",
            use_case="Banking Support"
        )
    
    def get_system_prompt(self) -> str:
        return """You are an AI assistant specialized in financial services and banking support.
Your role is to help customers with their banking and financial needs, including:
- Account balance inquiries
- Transaction history
- Fund transfers and payments
- Bill pay setup and management
- Account maintenance
- Card services

You understand banking terminology, financial products, and maintain strict security
and privacy compliance in all interactions. You're knowledgeable about:
- Banking products and services
- Payment processing and transfers
- Account security and fraud prevention
- Online and mobile banking features
- Financial regulations and compliance
"""
    
    def get_example_messages(self) -> list:
        return [
            {"role": "user", "content": "How do I dispute a transaction I don't recognize?"},
            {"role": "assistant", "content": "I'll help you with disputing the unrecognized transaction. Could you please identify the specific transaction date and amount? I'll guide you through our dispute process and help you submit the necessary documentation to investigate the charge."}
        ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case."""
        return FINS_RESPONSES
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        for response in FINS_RESPONSES:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        for response in FINS_RESPONSES:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 