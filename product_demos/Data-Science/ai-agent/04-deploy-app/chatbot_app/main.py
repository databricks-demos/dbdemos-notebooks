from fastapi import FastAPI
import gradio as gr
import os
from gradio.themes.utils import sizes
from databricks.sdk import WorkspaceClient

app = FastAPI()

print(gr.__version__)
w = WorkspaceClient()
available_endpoints = [x.name for x in w.serving_endpoints.list()]
#available_endpoints = [os.environ["MODEL_SERVING_ENDPOINT"]]

def respond(message, history, dropdown):
    if len(message.strip()) == 0:
        yield [
            *history,
            gr.ChatMessage(
                content="ERROR the question should not be empty.",
                role="assistant",
                metadata={"title": "⚠️ Error"}
            )
        ]
        return

    try:
        input_message = [{
            "content": message,
            "role": "user",
            "type": "message"
        }]
        # Show "thinking" step
        yield [
            *history,
            gr.ChatMessage(content=message, role="user"),
            gr.ChatMessage(
                content="Agent is reasoning and using tools...",
                role="assistant",
                metadata={"title": "🛠️ Agent Reasoning", "status": "pending"}
            )
        ]

        response = w.api_client.do(
            'POST',
            f'/serving-endpoints/{dropdown}/invocations',
            body={'input': input_message}
        )
        output_messages = response['output']

        # Collect all reasoning/tool steps for the tooltip
        thoughts = []
        for msg in output_messages[:-1]:
            if msg['role'] == 'assistant' and msg['content']:
                for content in msg['content']:
                    if content['type'] == 'output_text':
                        text = content['text']
                        # Skip empty or duplicate user prompts
                        if text.strip() and text.strip() != message.strip():
                            thoughts.append(text.strip())

        # Get only the last assistant message as the final answer
        final_msg = ""
        if output_messages:
            last_msg = output_messages[-1]
            if last_msg['role'] == 'assistant' and last_msg['content']:
                for content in last_msg['content']:
                    if content['type'] == 'output_text':
                        final_msg = content['text'].strip()

        # Build the chat messages: show only the answer, with a collapsible tooltip for reasoning
        chat_msgs = [
            *history,
            gr.ChatMessage(content=message, role="user"),
        ]
        if thoughts:
            chat_msgs.append(
                gr.ChatMessage(
                    content="\n\n".join(thoughts),
                    role="assistant",
                    metadata={
                        "title": "🧠 Agent Reasoning & Tool Usage",
                        "status": "done"
                    }
                )
            )
        if final_msg:
            chat_msgs.append(
                gr.ChatMessage(
                    content=final_msg,
                    role="assistant"
                )
            )
        yield chat_msgs

    except Exception as error:
        yield [
            *history,
            gr.ChatMessage(content=message, role="user"),
            gr.ChatMessage(
                content=f"ERROR requesting endpoint {dropdown}: {error}",
                role="assistant",
                metadata={"title": "⚠️ Error"}
            )
        ]

theme = gr.themes.Soft(
    text_size=sizes.text_sm,
    radius_size=sizes.radius_sm,
    spacing_size=sizes.spacing_sm,
)

demo = gr.ChatInterface(
    fn=respond,
    chatbot=gr.Chatbot(
        show_label=False,
        container=False,
        show_copy_button=True,
        bubble_full_width=True,
        type="messages"  # Enables metadata/thoughts display
    ),
    textbox=gr.Textbox(placeholder="Ask about customer data...", container=False, scale=7),
    title="Databricks AI Agent Demo - Customer Data Assistant",
    description=(
        "This AI agent can help you query customer data and provide insights. <br>"
        "It uses tools to access databases and can answer questions about customers, their segments, and business metrics.<br/>"
        "This is a demo example showing AI agent capabilities with Databricks."
    ),
    examples=[
        ["Give me the information about john21@example.net"],
        ["What are the step-by-step instructions for updating the firmware on my SAT-SURVEY-2024 system?"],
        ["Summarize all subscriptions held by john21@example.net"]
    ],
    cache_examples=False,
    theme=theme,
    additional_inputs=gr.Dropdown(
        choices=available_endpoints,
        value=os.environ["MODEL_SERVING_ENDPOINT"],
        label="Serving Endpoint",
    ),
    additional_inputs_accordion="Settings",
)

demo.queue(default_concurrency_limit=100)
demo.launch(server_name="0.0.0.0", server_port=8000)
