from fastapi import APIRouter, Depends, HTTPException, Body, Query
from fastapi.responses import StreamingResponse
from typing import List
from ..services.agent_service import AgentService
import mlflow.deployments
import mlflow

router = APIRouter()
agent_service = AgentService()


@router.get("/questions", response_model=List[dict])
async def get_questions(
    use_case: str = Query("telco", description="The use case to get questions for"),
    demo_type: str = Query("assist", description="The demo type: 'assist' or 'autopilot'")
):
    """Get the list of predefined questions"""
    return await agent_service.get_questions(use_case, demo_type)

@router.post("/chat")
async def process_message(
    messages: List[dict] = Body(...),
    intelligence_enabled: bool = Body(True),
    use_case: str = Body("telco", description="The use case to process messages for"),
    demo_type: str = Body("assist", description="The demo type: 'assist' or 'autopilot'")
):
    """Process a chat message and return streaming response"""
    return StreamingResponse(
        agent_service.process_message(messages, intelligence_enabled, use_case, demo_type),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream"
        }
    )