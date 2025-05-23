"""Industry registry for managing different industry implementations."""

from ..industries.telco import TelcoSubscriptionSupport
from ..industries.hls import HealthcareSupport
from ..industries.fins import FinancialServicesSupport
from ..industries.mfg import ManufacturingSupport
from ..industries.rcg import RetailOrderSupport
from ..industries.pharma import PharmaSupport

class IndustryRegistry:
    """Registry for managing different industry implementations."""
    
    _implementations = {
        "telco": TelcoSubscriptionSupport,
        "hls": HealthcareSupport,
        "fins": FinancialServicesSupport,
        "mfg": ManufacturingSupport,
        "retail": RetailOrderSupport,
        "pharma": PharmaSupport,
        "sap": FinancialServicesSupport  # Special implementation with SAP mode=True
    }
    
    @classmethod
    def get_implementation(cls, industry_code: str, demo_type: str = "assist"):
        """Get an instance of the implementation class for a given industry code."""
        if industry_code not in cls._implementations:
            raise ValueError(f"No implementation found for industry code: {industry_code}")
        
        implementation_class = cls._implementations[industry_code]
        
        # Special case for SAP mode
        if industry_code == "sap":
            return implementation_class(sap_mode=True)
        
        # Check if the implementation supports demo_type parameter
        try:
            # Try to pass demo_type parameter for implementations that support it (like telco)
            return implementation_class(demo_type=demo_type)
        except TypeError:
            # Fallback for implementations that don't support demo_type parameter
            return implementation_class()
    
    @classmethod
    def list_industries(cls) -> list:
        """List all available industry implementations."""
        # Exclude "sap" from the list as it's a hidden implementation
        return [k for k in cls._implementations.keys() if k != "sap"] 