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

    def _calculate_word_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity score between two texts based on word overlap"""
        words1 = set(text1.lower().split())
        words2 = set(text2.lower().split())
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        return intersection / union if union > 0 else 0

    async def find_best_response(self, question: str, use_case: str = "telco") -> dict:
        """Find the most relevant response by matching the question with predefined questions"""
        responses = self.use_cases.get(use_case, self.use_cases["telco"])["responses"]
        
        if not question:
            return responses[0]
        
        # Find most similar question
        best_match = max(responses, key=lambda r: self._calculate_word_similarity(question, r["question"]))
        return best_match

    async def process_message(self, messages, intelligence_enabled: bool = True, use_case: str = "telco"):
        """Process a message and yield streaming responses"""
        import asyncio
        
        # Get the last user message
        user_messages = [m for m in messages if m["role"] == "user"]
        last_question = user_messages[-1]["content"] if user_messages else ""
        
        response = await self.find_best_response(last_question, use_case)

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