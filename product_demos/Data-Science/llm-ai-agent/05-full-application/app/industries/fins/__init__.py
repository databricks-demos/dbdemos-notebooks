"""Financial Services (FINS) industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import FINS_RESPONSES
from .sap_responses import SAP_FINS_RESPONSES

class FinancialServicesSupport(IndustryBase):
    """Implementation for Financial Services Support use case."""
    
    def __init__(self, sap_mode: bool = False):
        """Initialize with SAP mode flag."""
        super().__init__()
        self.sap_mode = sap_mode
    
    def get_config(self) -> IndustryConfig:
        """Get the configuration for this industry.
        
        The name changes when in SAP mode to indicate the different use case.
        """
        if self.sap_mode:
            return IndustryConfig(
                name="FINS-SAP",
                description="Financial Services Industry with SAP Integration",
                use_case="Banking Support (SAP)"
            )
        return IndustryConfig(
            name="FINS",
            description="Financial Services Industry",
            use_case="Banking Support"
        )
    
    def get_system_prompt(self) -> str:
        """Get the system prompt for this industry.
        
        The prompt is enhanced with SAP-specific information when in SAP mode.
        """
        base_prompt = """You are an AI assistant specialized in financial services and banking support.
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
        if self.sap_mode:
            sap_additions = """
You are working specifically with a bank that uses SAP systems for their core infrastructure.
In addition to general banking knowledge, you understand:
- SAP Banking modules and functionality
- SAP CRM and how it's used for customer management
- SAP FSCM (Financial Supply Chain Management)
- SAP GRC (Governance, Risk, and Compliance)
- Integration between SAP and banking systems
- SAP workflows for banking operations
"""
            return base_prompt + sap_additions
        return base_prompt
    
    def get_example_messages(self) -> list:
        if self.sap_mode:
            return [
                {"role": "user", "content": "How do I dispute a transaction I don't recognize?"},
                {"role": "assistant", "content": "I'll help you with disputing the unrecognized transaction through our SAP-integrated system. Could you please identify the specific transaction date and amount? I'll guide you through our SAP-powered dispute process and help you submit the necessary documentation to investigate the charge."}
            ]
        return [
            {"role": "user", "content": "How do I dispute a transaction I don't recognize?"},
            {"role": "assistant", "content": "I'll help you with disputing the unrecognized transaction. Could you please identify the specific transaction date and amount? I'll guide you through our dispute process and help you submit the necessary documentation to investigate the charge."}
        ]
    
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case.
        
        When in SAP mode, return the SAP-specific responses.
        """
        if self.sap_mode:
            return SAP_FINS_RESPONSES
        return FINS_RESPONSES
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        responses = SAP_FINS_RESPONSES if self.sap_mode else FINS_RESPONSES
        for response in responses:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        responses = SAP_FINS_RESPONSES if self.sap_mode else FINS_RESPONSES
        tools = set()
        for response in responses:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 