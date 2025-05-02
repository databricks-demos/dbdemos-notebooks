"""Pharmaceutical (PHARMA) industry implementations."""

from typing import List, Dict, Any
from ..base import IndustryBase, IndustryConfig
from .responses import PHARMA_RESPONSES

class PharmaSupport(IndustryBase):
    """Implementation for Pharmaceutical Support use case."""
    def get_config(self) -> IndustryConfig:
        return IndustryConfig(
            name="PHARMA",
            description="Pharmaceutical Industry",
            use_case="Pharma Support"
        )

    def get_system_prompt(self) -> str:
        return """You are an AI assistant specialized in pharmaceutical industry support.\nYour role is to help with pharma-related queries and operations, including:\n- Clinical trial monitoring\n- Manufacturing quality control\n- Regulatory compliance\n- R&D pipeline optimization\n- Safety and pharmacovigilance\n\nYou understand pharmaceutical terminology, clinical processes, and maintain\nstrict regulatory and data privacy standards in all interactions. You're knowledgeable about:\n- Clinical trial operations\n- GMP manufacturing\n- Quality management systems\n- Regulatory requirements\n- R&D workflows\n- Safety monitoring\n"""

    def get_example_messages(self) -> list:
        return [
            {"role": "user", "content": "Can you analyze the latest adverse events in our clinical trial?"},
            {"role": "assistant", "content": "I'll review the safety monitoring data and provide a summary of adverse events, including any significant trends or regulatory concerns."}
        ]

    def get_responses(self) -> List[Dict[str, Any]]:
        return PHARMA_RESPONSES

    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        for response in PHARMA_RESPONSES:
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")

    def get_available_tools(self) -> List[Dict[str, str]]:
        tools = set()
        for response in PHARMA_RESPONSES:
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 