import itertools
from fastapi import FastAPI
import gradio as gr
import requests
import os
from gradio.themes.utils import sizes
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

app = FastAPI()

#your endpoint will directly be setup with proper permissions when you deploy your app
w = WorkspaceClient()

model_serving_endpoint_name = os.environ['MODEL_SERVING_ENDPOINT']

def respond(message, history):
    if len(message.strip()) == 0:
        return "ERROR the question should not be empty"
    try:
        response = w.serving_endpoints.query(
            name=model_serving_endpoint_name,
            messages=[ChatMessage(content=message, role=ChatMessageRole.USER)],
            temperature=1.0,
            stream=False
        )
    except Exception as error:
        response = f"ERROR status_code: {type(error).__name__}"       
    return response.choices[0].message.content

theme = gr.themes.Soft(
    text_size=sizes.text_sm,radius_size=sizes.radius_sm, spacing_size=sizes.spacing_sm,
)

demo = gr.ChatInterface(
    respond,
    chatbot=gr.Chatbot(show_label=False, container=False, show_copy_button=True, bubble_full_width=True),
    textbox=gr.Textbox(placeholder="Ask me a question",
                       container=False, scale=7),
    title="Databricks App RAG demo - Chat with your Databricks assistant",
    description="This chatbot is a demo example for the dbdemos llm chatbot. <br>This content is provided as a LLM RAG educational example, without support. It is using DBRX, can hallucinate and should not be used as production content.<br>Please review our dbdemos license and terms for more details.",
    examples=[["What is DBRX?"],
              ["How can I start a Databricks cluster?"],
              ["What is a Databricks Cluster Policy?"],
              ["How can I track billing usage on my workspaces?"],],
    cache_examples=False,
    theme=theme,
    retry_btn=None,
    undo_btn=None,
    clear_btn="Clear",
)

demo.queue(default_concurrency_limit=100)
app = gr.mount_gradio_app(app, demo, path="/")
