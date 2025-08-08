from fastapi import FastAPI, APIRouter
from pydantic import BaseModel
from enum import Enum
from typing import Optional
import gradio as gr
import os
from gradio.themes.utils import sizes
from databricks.sdk import WorkspaceClient
import mlflow
from mlflow.deployments import get_deploy_client
import json

mlflow.set_tracking_uri("databricks")

# --- FastAPI setup ---
app = FastAPI()
router = APIRouter()
app.include_router(router)

# --- Databricks Model Serving setup ---
w = WorkspaceClient()
available_endpoints = [x.name for x in w.serving_endpoints.list()]

# --- Gradio Chatbot Logic ---
def respond(message, history, dropdown):
    if len(message.strip()) == 0:
        return [
            *history,
            gr.ChatMessage(
                content="ERROR the question should not be empty.",
                role="assistant"
            )
        ]

    try:
        client = get_deploy_client("databricks")
        input_message = [{
            "content": message,
            "role": "user",
            "type": "message"
        }]

        response = client.predict(
            endpoint=dropdown,
            inputs={'input': input_message, "databricks_options": {
                "return_trace": True
            }}
        )
        print(f"Response: {response}")
        output_messages = response['output']
        trace_id = response.get("databricks_output", {}).get("trace", {}).get("info", {}).get("trace_id")
        print(f"Trace ID: {trace_id}")

        thoughts = []
        final_msg = ""

        # First pass: collect function calls and outputs as thoughts
        for msg in output_messages:
            if msg.get('type') == 'function_call':
                function_name = msg.get('name', 'Unknown function')
                thoughts.append(f"🔧 Calling function: {function_name}")

            elif msg.get('type') == 'function_call_output':
                try:
                    output_data = json.loads(msg.get('output', '{}'))
                    if 'value' in output_data:
                        thoughts.append(f"📊 Function returned data")
                except Exception:
                    thoughts.append("📊 Function completed")

        # Second pass: find the final assistant message
        for msg in output_messages:
            if msg.get('role') == 'assistant' and msg.get('content'):
                for content in msg['content']:
                    if content['type'] == 'output_text':
                        text = content['text'].strip()
                        if text and text != message.strip():
                            final_msg = text
                            break
                if final_msg:
                    break

        chat_msgs = [
            *history,
            gr.ChatMessage(
                content=message,
                role="user"
            )
        ]

        # Updated: Use the metadata={'title': ...} for thoughts, for Gradio built-in accordion styling
        if thoughts:
            thoughts_text = "\n\n".join(thoughts)
            chat_msgs.append(
                gr.ChatMessage(
                    content=thoughts_text,
                    role="assistant",
                    metadata={"title": "🤔 Agent Thoughts"}
                )
            )

        if final_msg:
            chat_msgs.append(
                gr.ChatMessage(
                    content=final_msg,
                    role="assistant",
                    options=[{"value": trace_id, "label": "trace_id"}] if trace_id else []
                )
            )
        print(f"Chat messages: {chat_msgs}")
        return chat_msgs

    except Exception as error:
        return [
            *history,
            gr.ChatMessage(content=message, role="user"),
            gr.ChatMessage(
                content=f"ERROR requesting endpoint {dropdown}: {error}",
                role="assistant"
            )
        ]

# --- Gradio UI with Feedback Buttons ---
theme = gr.themes.Soft(
    text_size=sizes.text_sm,
    radius_size=sizes.radius_sm,
    spacing_size=sizes.spacing_sm,
)

with gr.Blocks(theme=theme) as demo:
    gr.Markdown(
        """
        # Databricks AI Agent Demo - Customer Data Assistant

        This AI agent can help you query customer data and provide insights.  
        It uses tools to access databases and can answer questions about customers, their segments, and business metrics.  
        This is a demo example showing AI agent capabilities with Databricks.
        """
    )

    endpoint_dropdown = gr.Dropdown(
        choices=available_endpoints,
        value=os.environ["MODEL_SERVING_ENDPOINT"],
        label="Serving Endpoint"
    )

    chatbot = gr.Chatbot(
        show_label=False,
        container=False,
        show_copy_button=True,
        type="messages"
    )
    
    with gr.Row():
        textbox = gr.Textbox(
            placeholder="Ask about customer data...",
            container=False,
            scale=7
        )
        send_button = gr.Button("Send", variant="primary")

    feedback_output = gr.Markdown(visible=False)

    # --- Feedback Handler: Extract trace_id from options ---
    def handle_feedback(history, like_data: gr.LikeData):
        idx = like_data.index

        print(f"History in handle_feedback: {history}")

        # Extract trace_id from options
        trace_id = next((v.get('value') for m in history if m['role'] == 'assistant' for v in m['options'] if v.get('label') == 'trace_id'), None)

        if not trace_id:
            return gr.Markdown("No trace ID found for this message.", visible=True)
        try:
            mlflow.log_feedback(
                trace_id=trace_id,
                name='user_feedback',
                value=True if like_data.liked else False,
                rationale=None,
                source=mlflow.entities.AssessmentSource(
                    source_type='HUMAN',
                    source_id='user',
                ),
            )
            return gr.Markdown(f"Thank you for your feedback - sent to MLflow: {trace_id}", visible=True)
        except Exception as e:
            return gr.Markdown(f"Error submitting feedback: {str(e)}", visible=True)

    def chat_submit(message, history, endpoint):
        return respond(message, history, endpoint)

    with gr.Row():
        chatbot
        feedback_output

    textbox.submit(
        chat_submit,
        [textbox, chatbot, endpoint_dropdown],
        chatbot
    )
    
    send_button.click(
        chat_submit,
        [textbox, chatbot, endpoint_dropdown],
        chatbot
    )

    # Attach feedback handler (history, like_data)
    chatbot.like(handle_feedback, chatbot, feedback_output)

    gr.Examples(
        examples=[
            ["Give me the information about john21@example.net"],
            ["What are the step-by-step instructions for updating the firmware on my SAT-SURVEY-2024 system?"],
            ["Summarize all subscriptions held by john21@example.net"]
        ],
        inputs=[textbox]
    )

demo.queue(default_concurrency_limit=100)
demo.launch(server_name="0.0.0.0", server_port=8000)
