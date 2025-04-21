"""Registry for all industry implementations."""

from typing import Dict, Type
from .base import IndustryBase

# Import all industry implementations
from .telco import TelcoSubscriptionSupport

class IndustryRegistry:
    """Registry for managing industry implementations."""
    
    _implementations: Dict[str, Type[IndustryBase]] = {
        "TELCO_SUBSCRIPTION": TelcoSubscriptionSupport,
        # Add other implementations as they are created:
        # "HLS_CLAIMS": HlsClaimsSupport,
        # "FINS_INSURANCE": FinsInsuranceSupport,
        # "MFG_MACHINE": MfgMachineSupport,
        # "CMEG_PLAYER": CmegPlayerSupport,
        # "PUBS_CIVIL": PubsCivilSupport,
        # "RCG_RETAIL": RcgRetailSupport,
    }
    
    @classmethod
    def get_implementation(cls, industry_code: str) -> Type[IndustryBase]:
        """Get the implementation class for a specific industry code."""
        if industry_code not in cls._implementations:
            raise ValueError(f"No implementation found for industry code: {industry_code}")
        return cls._implementations[industry_code]
    
    @classmethod
    def register_implementation(cls, code: str, implementation: Type[IndustryBase]):
        """Register a new industry implementation."""
        cls._implementations[code] = implementation
    
    @classmethod
    def list_implementations(cls) -> Dict[str, str]:
        """List all available implementations with their descriptions."""
        return {
            code: impl().get_config().description
            for code, impl in cls._implementations.items()
        } 