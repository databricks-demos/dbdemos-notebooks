"""Base classes for industry implementations."""

from typing import List, Dict, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod

@dataclass
class IndustryConfig:
    """Base configuration for industry-specific settings."""
    name: str
    description: str
    use_case: str

class IndustryBase(ABC):
    """Base class for industry-specific implementations."""
    
    @abstractmethod
    def get_config(self) -> IndustryConfig:
        """Return the configuration for this industry implementation."""
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Return the system prompt for this industry."""
        pass
    
    @abstractmethod
    def get_example_messages(self) -> list:
        """Return example messages for this industry use case."""
        pass
    
    @abstractmethod
    def get_responses(self) -> List[Dict[str, Any]]:
        """Get all example responses for this use case."""
        pass
    
    def get_response_by_question(self, question: str) -> Dict[str, Any]:
        """Get a specific response by its question."""
        for response in self.get_responses():
            if response["question"] == question:
                return response
        raise ValueError(f"No response found for question: {question}")
    
    def get_available_tools(self) -> List[Dict[str, str]]:
        """Get a list of all available tools used in the responses."""
        tools = set()
        for response in self.get_responses():
            for tool in response.get("tools", []):
                tools.add((tool["tool_name"], tool["description"]))
        return [{"name": name, "description": desc} for name, desc in sorted(tools)] 