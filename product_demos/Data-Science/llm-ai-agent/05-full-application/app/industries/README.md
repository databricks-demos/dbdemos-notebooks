# Industry Use Cases Documentation

This directory contains industry-specific implementations for different AI agent use cases. Each industry has its own dedicated module with specific prompts, configurations, and example responses.

## Directory Structure

```
app/industries/
├── base.py                # Base classes and interfaces
├── __init__.py           # Package initialization
├── industry_name/        # Industry-specific directory (e.g., telco, hls, fins)
│   ├── __init__.py      # Industry implementation
│   └── responses.py      # Example responses and tools
└── README.md            # This documentation
```

## Adding a New Industry

1. Create a new directory under `app/industries/` with your industry code (e.g., `hls`, `fins`, `mfg`):
   ```bash
   mkdir app/industries/your_industry_code
   ```

2. Create `responses.py` in your industry directory with example responses:
   ```python
   """Your industry use case responses and examples."""

   YOUR_INDUSTRY_RESPONSES = [
       {
           "question": "Example customer question?",
           "non_intelligent_answer": "Basic response without AI tools",
           "tools": [
               {
                   "tool_name": "tool_name",
                   "description": "What the tool does",
                   "type": "TOOL_TYPE",  # e.g., DATABASE, FORECASTING_MODEL, EXTERNAL_API
                   "reasoning": "Why this tool is being called",
                   "informations": [
                       "Additional information about tool usage"
                   ]
               }
           ],
           "final_answer": "Detailed response using tool results",
           "final_informations": [
               "Additional context or information"
           ]
       }
   ]
   ```

3. Create `__init__.py` in your industry directory with your implementation:
   ```python
   from typing import List, Dict, Any
   from ..base import IndustryBase, IndustryConfig
   from .responses import YOUR_INDUSTRY_RESPONSES

   class YourIndustrySupport(IndustryBase):
       def get_config(self) -> IndustryConfig:
           return IndustryConfig(
               name="YOUR_INDUSTRY",
               description="Your Industry Description",
               use_case="Your Use Case"
           )
       
       def get_system_prompt(self) -> str:
           return """Your industry-specific system prompt.
           Describe the AI assistant's role and expertise."""
       
       def get_example_messages(self) -> list:
           return [
               {"role": "user", "content": "Example user message"},
               {"role": "assistant", "content": "Example assistant response"}
           ]
       
       def get_responses(self) -> List[Dict[str, Any]]:
           return YOUR_INDUSTRY_RESPONSES
   ```

4. Register your implementation in `app/services/industry_registry.py`:
   ```python
   from ..industries.your_industry_code import YourIndustrySupport

   class IndustryRegistry:
       _implementations = {
           "your_industry_code": YourIndustrySupport,
           # ... other implementations ...
       }
   ```

## Modifying an Existing Industry

### Updating Responses

1. Open the industry's `responses.py` file
2. Modify existing responses or add new ones following the response structure:
   ```python
   {
       "question": str,            # The customer question
       "non_intelligent_answer": str,  # Basic response without AI
       "tools": List[Dict],       # Tools used in the response
       "final_answer": str,       # AI-generated response
       "final_informations": List[str]  # Additional context
   }
   ```

### Updating Implementation

1. Open the industry's `__init__.py` file
2. Modify the implementation class methods:
   - `get_config()`: Update industry metadata
   - `get_system_prompt()`: Modify AI behavior
   - `get_example_messages()`: Update example interactions

## Best Practices

1. **System Prompts**
   - Be specific about the AI's role and expertise
   - List key capabilities and knowledge areas
   - Include any industry-specific terminology or guidelines

2. **Example Messages**
   - Provide realistic conversation examples
   - Cover common use cases
   - Show proper interaction patterns

3. **Responses**
   - Include a variety of scenarios
   - Demonstrate proper tool usage
   - Provide both simple and complex interactions
   - Include error handling cases

4. **Tools**
   - Document each tool's purpose clearly
   - Include proper reasoning for tool usage
   - Provide informative feedback messages

## Testing Your Implementation

1. Ensure your implementation follows the `IndustryBase` interface
2. Test all responses with both intelligent and non-intelligent modes
3. Verify tool integrations work as expected
4. Check that system prompts generate appropriate responses

## Example Implementation

See the TELCO implementation (`app/industries/telco/`) for a complete example of:
- Industry configuration
- System prompts
- Example messages
- Response structure
- Tool usage

## Need Help?

- Check the TELCO implementation for reference
- Review the `IndustryBase` class for required methods
- Test your implementation thoroughly
- Follow the established patterns for consistency 