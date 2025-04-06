from .mockup_agent import USE_CASES
import json

class AgentService:
    def __init__(self):
        self.use_cases = USE_CASES

    async def get_questions(self, use_case: str = "telco"):
        """Return the list of predefined questions for the given use case"""
        responses = self.use_cases.get(use_case, self.use_cases["telco"])["responses"]
        return [{"preview": q["question"][:50] + "..." if len(q["question"]) > 50 else q["question"], 
                 "text": q["question"]} for q in responses]

    async def get_response_by_history_size(self, history_size: int, use_case: str = "telco"):
        """Get response based on history size and use case"""
        responses = self.use_cases.get(use_case, self.use_cases["telco"])["responses"]
        user_message_count = history_size - 1
        index = user_message_count % len(responses) if user_message_count >= 0 else 0
        return responses[index]
    

    async def process_message(self, messages, intelligence_enabled: bool = True, use_case: str = "telco"):
        """Process a message and yield streaming responses"""
        import asyncio
        history_size = len([m for m in messages if m["role"] == "user"])
        response = await self.get_response_by_history_size(history_size, use_case)

        # First emit thinking start
        yield f"data: {json.dumps({'type': 'thinking-start', 'data': None})}\n\n"
        
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

    def get_agent(self, agent_id: int):
        pass