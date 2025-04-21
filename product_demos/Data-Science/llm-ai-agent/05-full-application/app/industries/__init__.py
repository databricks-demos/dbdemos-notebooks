"""
This module contains industry-specific implementations for different use cases.
Each industry has its own submodule with specific prompts, configurations, and implementations.
"""

from typing import Dict, Type
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

"""Industry-specific implementations package."""

from .base import IndustryBase, IndustryConfig 