from fastapi import FastAPI, APIRouter
from pydantic import BaseModel
from enum import Enum
from typing import Optional
import gradio as gr
import os
from gradio.themes.utils import sizes
from databricks.sdk import WorkspaceClient
import mlflow
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
        input_message = [{
            "content": message,
            "role": "user",
            "type": "message"
        }]
        """
        TODO IRVIN
        replace w.api_client.do( by: 
        import mlflow
import mlflow.deployments

client = mlflow.deployments.get_deploy_client("databricks")

input_message = [{
            "content": "test",
            "role": "user",
            "type": "message"
        }]

response = client.predict(
  # Endpoint from above.
  endpoint=ENDPOINT_NAME,
  inputs={'input': input_message, "databricks_options": {
      # Return the trace so we can get the trace_id for logging feedback.
      "return_trace": True
    }}
)
response
and also the trace id shouldn't be in         trace_id = response.get('custom_outputs').get('trace_id') anymore (we are removing custom_output)

        """
        response = w.api_client.do(
            'POST',
            f'/serving-endpoints/{dropdown}/invocations',
            body={'input': input_message}
        )
        print(f"Response: {response}")
        
        output_messages = response['output']
        """TODO IRVIN: change here 
        trace_id = response.get('custom_outputs').get('trace_id') """
        print(f"Trace ID: {trace_id}")
        thoughts = []
        for msg in output_messages[:-1]:
            if msg['role'] == 'assistant' and msg['content']:
                for content in msg['content']:
                    if content['type'] == 'output_text':
                        text = content['text']
                        if text.strip() and text.strip() != message.strip():
                            thoughts.append(text.strip())

        final_msg = ""
        if output_messages:
            last_msg = output_messages[-1]
            if last_msg['role'] == 'assistant' and last_msg['content']:
                for content in last_msg['content']:
                    if content['type'] == 'output_text':
                        final_msg = content['text'].strip()

        chat_msgs = [
            *history,
            gr.ChatMessage(
                content=message,
                role="user"
            )
        ]
        if thoughts:
            chat_msgs.append(
                gr.ChatMessage(content="\n\n".join(thoughts),role="assistant")
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
    textbox = gr.Textbox(
        placeholder="Ask about customer data...",
        container=False,
        scale=7
    )

    feedback_output = gr.Markdown(visible=False)

    # --- Feedback Handler: Extract trace_id from options ---
    def handle_feedback(history, like_data: gr.LikeData):
        idx = like_data.index
        msg = history[idx]
        trace_id = None

        # Extract trace_id from options
        if 'options' in msg:
            for option in msg["options"]:
                if option.get("label") == "trace_id":
                    trace_id = option.get("value")
                break

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
