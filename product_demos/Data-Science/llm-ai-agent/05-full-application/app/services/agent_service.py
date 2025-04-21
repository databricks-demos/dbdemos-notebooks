"""Service for handling agent interactions."""

from typing import List, Dict, Any
import json
import mlflow.deployments
import mlflow
import asyncio
from .industry_registry import IndustryRegistry

class AgentService:
    def __init__(self, use_mockup: bool = True):
        self.use_mockup = use_mockup
        self.registry = IndustryRegistry()

    async def get_questions(self, use_case: str = "telco"):
        """Return the list of predefined questions for the given use case"""
        implementation = self.registry.get_implementation(use_case)
        if not implementation:
            # Fallback to telco if implementation not found
            implementation = self.registry.get_implementation("telco")
        
        responses = implementation.get_responses()
        return [{"preview": q["question"][:50] + "..." if len(q["question"]) > 50 else q["question"], 
                 "text": q["question"]} for q in responses]

    def _cleanup_message_sequence(self, messages: List[dict]) -> List[dict]:
        """Clean up message sequence to ensure proper alternation and remove redundant messages.
        
        Rules:
        1. Remove first message if it's from assistant
        2. For consecutive user messages, only keep the last one
        """
        cleaned_messages = []
        prev_role = None
        
        for msg in messages:
            # Skip first message if it's from assistant
            if not cleaned_messages and msg["role"] == "assistant":
                continue
                
            # If we have consecutive user messages, remove the previous one
            if msg["role"] == "user" and prev_role == "user":
                cleaned_messages.pop()
                
            cleaned_messages.append(msg)
            prev_role = msg["role"]
            
        return cleaned_messages

    async def process_message(self, messages, intelligence_enabled: bool = True, use_case: str = "telco"):
        """Process a message and yield streaming responses"""
        # First emit thinking start
        yield f"data: {json.dumps({'type': 'thinking-start', 'data': None})}\n\n"

        if self.use_mockup:
            async for response in self._process_message_mock(messages, intelligence_enabled, use_case):
                yield response
        else:
            async for response in self._process_message_real(messages, intelligence_enabled, use_case):
                yield response

    async def _process_message_real(self, messages, intelligence_enabled: bool, use_case: str):
        """Process messages using real Databricks endpoints"""
        implementation = self.registry.get_implementation(use_case)
        if not implementation:
            implementation = self.registry.get_implementation("telco")
        
        # Add system prompt based on use case
        system_messages = [{"role": "system", "content": implementation.get_system_prompt()}]
        
        # Clean up message sequence
        cleaned_messages = self._cleanup_message_sequence(messages)
        full_messages = system_messages + cleaned_messages
        
        # Call the appropriate endpoint based on intelligence mode
        endpoint_name = "agents_main-dbdemos_agent_tools-agent_tools_test" if intelligence_enabled else "databricks-llama3"
        
        # Get streaming response
        response_stream = self.call_endpoint(full_messages, endpoint_name)
        
        current_tool = None
        for chunk in response_stream:
            if isinstance(chunk, str):
                chunk = json.loads(chunk)
            
            delta = chunk.get('delta', {})
            
            if delta.get('tool_calls') and intelligence_enabled:
                # Tool call started
                for tool in delta['tool_calls']:
                    tool_data = {
                        "tool_name": tool['function']['name'].replace('main__dbdemos_agent_tools__', ''),
                        "description": tool['function']['arguments'],
                        "type": "TOOL_CALL",
                        "reasoning": f"Calling tool {tool['function']['name']}"
                    }
                    current_tool = tool['id']
                    yield f"data: {json.dumps({'type': 'tool', 'data': tool_data})}\n\n"
            
            elif delta.get('role') == 'tool' and intelligence_enabled:
                # Tool response
                if delta.get('tool_call_id') == current_tool:
                    tool_response = json.loads(delta.get('content', '{}'))
                    yield f"data: {json.dumps({'type': 'tool', 'data': {'informations': [tool_response.get('value', '')]}})}\n\n"
            
            elif delta.get('content'):
                # Final answer
                yield f"data: {json.dumps({'type': 'final-answer', 'data': {'final_answer': delta['content']}})}\n\n"

    def call_endpoint(self, messages: List[dict], endpoint_name: str = "agents_main-dbdemos_agent_tools-agent_tools_test"):
        """Call MLflow endpoint with proper Databricks configuration"""
        # Create client and call endpoint
        client = mlflow.deployments.get_deploy_client("databricks")
        response_stream = client.predict_stream(
            endpoint=endpoint_name,
            inputs={"messages": messages, "stream": True}
        )
        return response_stream

    def get_agent(self, agent_id: int):
        pass

    ###########################################
    # MOCK IMPLEMENTATION
    ###########################################

    def _calculate_word_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity score between two texts based on word overlap"""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        return intersection / union if union > 0 else 0

    async def find_best_response(self, question: str, use_case: str = "telco") -> dict:
        """Find the most relevant response by matching the question with predefined questions"""
        implementation = self.registry.get_implementation(use_case)
        if not implementation:
            implementation = self.registry.get_implementation("telco")
        
        responses = implementation.get_responses()
        if not question:
            return responses[0]
        
        # Find most similar question
        best_match = max(responses, key=lambda r: self._calculate_word_similarity(question, r["question"]))
        return best_match

    async def _process_message_mock(self, messages, intelligence_enabled: bool, use_case: str):
        """Process messages using mock data and simulated delays"""
        # Get the last user message
        user_messages = [m for m in messages if m["role"] == "user"]
        last_question = user_messages[-1]["content"] if user_messages else ""
        
        response = await self.find_best_response(last_question, use_case)
        
        # Use shorter thinking time for non-intelligent mode
        await asyncio.sleep(2 if intelligence_enabled else 1)

        if intelligence_enabled:
            # Emit each tool one by one with delays
            for tool in response["tools"]:
                yield f"data: {json.dumps({'type': 'tool', 'data': tool})}\n\n"
                await asyncio.sleep(2)

            # Wait before emitting final answer
            await asyncio.sleep(1.5)

            # Emit final answer and informations
            yield f"data: {json.dumps({'type': 'final-answer', 'data': {'final_answer': response['final_answer'], 'final_informations': response.get('final_informations', [])}})}\n\n"
        else:
            # Emit completion event
            yield f"data: {json.dumps({'type': 'final-answer', 'data': {'final_answer': response['non_intelligent_answer']}})}\n\n"