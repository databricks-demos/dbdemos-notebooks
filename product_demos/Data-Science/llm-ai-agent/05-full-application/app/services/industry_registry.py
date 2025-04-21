"""Industry registry for managing different industry implementations."""

from ..industries.telco import TelcoSubscriptionSupport
from ..industries.hls import HealthcareSupport
from ..industries.fins import FinancialServicesSupport
from ..industries.mfg import ManufacturingSupport
from ..industries.rcg import RetailOrderSupport

class IndustryRegistry:
    """Registry for managing different industry implementations."""
    
    _implementations = {
        "telco": TelcoSubscriptionSupport,
        "hls": HealthcareSupport,
        "fins": FinancialServicesSupport,
        "mfg": ManufacturingSupport,
        "retail": RetailOrderSupport
    }
    
    @classmethod
    def get_implementation(cls, industry_code: str):
        """Get an instance of the implementation class for a given industry code."""
        if industry_code not in cls._implementations:
            raise ValueError(f"No implementation found for industry code: {industry_code}")
        implementation_class = cls._implementations[industry_code]
        return implementation_class()  # Return an instance of the class
    
    @classmethod
    def list_industries(cls) -> list:
        """List all available industry implementations."""
        return list(cls._implementations.keys()) 