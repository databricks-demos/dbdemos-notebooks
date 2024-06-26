# Databricks notebook source
# MAGIC %pip install gradio==3.38.0 fastapi==0.104 uvicorn
# MAGIC %pip install typing-extensions --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

endpoint_name = "dbdemos_endpoint_advanced_amine_elhelou_rag_chatbot"

DESCRIPTION = f"""
# Chatbot powered by Databricks
Sample bot that can answer databricks questions.
"""

# COMMAND ----------

# MAGIC %md ### Helper functions

# COMMAND ----------

import requests
import json


workspaceUrl = spark.conf.get('spark.databricks.workspaceUrl')
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

chatbot_model_serving_endpoint = f'https://{workspaceUrl}/serving-endpoints/{endpoint_name}/invocations'

message_thread = []
def reset_thread():
  global message_thread
  message_thread = []
reset_thread()

# COMMAND ----------

def submit_prompt(prompt):
  global message_thread 
  
  headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}

  message_thread.append(
    {
      "role": "user",
      "content": prompt
    }
  )

  payload = {
    "messages": message_thread
  }

  response = requests.post(chatbot_model_serving_endpoint, headers=headers, json=payload)

  if response.status_code == 200:
      resp = response.json()

      try:
        bot_message = resp['choices'][0]['message']['content']
        
        message_thread = resp['thread']
      
      except Exception as e:
        bot_message = resp[0]['result']+". Relevant sources:"+",".join(resp[0]['sources'])

      return bot_message
    
  else:
      error = response.json()
      raise ValueError(f'Error submitting job: {error}')

# COMMAND ----------

def generate_output(message: str,
        chat_history: list[tuple[str, str]],
        # system_prompt: str,
        max_new_tokens: int = 300,
        temperature: float = 0.8,
        top_p: float = 0.95,
        top_k: int = 50):
    
    output = submit_prompt(message)
    return output

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Let's host it in gradio

# COMMAND ----------

import json
from dataclasses import dataclass

import uvicorn
from fastapi import FastAPI

# COMMAND ----------

@dataclass
class ProxySettings:
    proxy_url: str
    port: str
    url_base_path: str


class DatabricksApp:

    def __init__(self, port):
        # self._app = data_app
        self._port = port
        import IPython
        self._dbutils = IPython.get_ipython().user_ns["dbutils"]
        self._display_html = IPython.get_ipython().user_ns["displayHTML"]
        self._context = json.loads(self._dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
        # need to do this after the context is set
        self._cloud = self.get_cloud()
        # create proxy settings after determining the cloud
        self._ps = self.get_proxy_settings()
        self._fastapi_app = self._make_fastapi_app(root_path=self._ps.url_base_path.rstrip("/"))
        self._streamlit_script = None
        # after everything is set print out the url

    def _make_fastapi_app(self, root_path) -> FastAPI:
        fast_api_app = FastAPI(root_path=root_path)

        @fast_api_app.get("/")
        def read_main():
            return {
                "routes": [
                    {"method": "GET", "path": "/", "summary": "Landing"},
                    {"method": "GET", "path": "/status", "summary": "App status"},
                    {"method": "GET", "path": "/dash", "summary": "Sub-mounted Dash application"},
                ]
            }

        @fast_api_app.get("/status")
        def get_status():
            return {"status": "ok"}

        return fast_api_app

    def get_proxy_settings(self) -> ProxySettings:
        if self._cloud.lower() not in ["aws", "azure"]:
            raise Exception("only supported in aws or azure")

        org_id = self._context["tags"]["orgId"]
        org_shard = ""
        # org_shard doesnt need a suffix of "." for dnsname its handled in building the url
        if self._cloud.lower() == "azure":
            org_shard_id = int(org_id) % 20
            org_shard = f".{org_shard_id}"
        cluster_id = self._context["tags"]["clusterId"]
        url_base_path = f"/driver-proxy/o/{org_id}/{cluster_id}/{self._port}"

        from dbruntime.databricks_repl_context import get_context
        host_name = get_context().browserHostName
        proxy_url = f"https://{host_name}/driver-proxy/o/{org_id}/{cluster_id}/{self._port}/"

        return ProxySettings(
            proxy_url=proxy_url,
            port=self._port,
            url_base_path=url_base_path
        )

    @property
    def app_url_base_path(self):
        return self._ps.url_base_path

    def mount_gradio_app(self, gradio_app):
        import gradio as gr
        # gradio_app.queue()
        gr.mount_gradio_app(self._fastapi_app, gradio_app, f"/gradio")
        # self._fastapi_app.mount("/gradio", gradio_app)
        self.display_url(self.get_gradio_url())

    def get_cloud(self):
        if self._context["extraContext"]["api_url"].endswith("azuredatabricks.net"):
            return "azure"
        return "aws"

    def get_gradio_url(self):
        # must end with a "/" for it to not redirect
        return f'<a href="{self._ps.proxy_url}gradio/">Click to go to Gradio App!</a>'

    def display_url(self, url):
        self._display_html(url)

    def run(self):
        print(self.app_url_base_path)
        uvicorn.run(self._fastapi_app, host="0.0.0.0", port=self._port)

# COMMAND ----------

import gradio as gr
import random
import time



def process_example(message: str, history: str):
    # system_prompt, max_new_tokens, temperature, top_p, top_k
    output = generate_output(message, history)
    return output

chatbot = gr.Chatbot(height=500)

details = gr.JSON()

with gr.Blocks(theme=gr.themes.Soft()) as demo:
    with gr.Row():
        gr.HTML(
            show_label=False,
            value="<img src='https://databricks.gallerycdn.vsassets.io/extensions/databricks/databricks/0.3.15/1686753455931/Microsoft.VisualStudio.Services.Icons.Default' height='40' width='40'/><div font size='1'></div>",
        )
    gr.Markdown(DESCRIPTION)

    tabbed = gr.TabbedInterface([chatbot, details], ["Chat", "JSON"])

    #chatbot = gr.Chatbot(height=500)
    msg = gr.Textbox(label='User Question'
                    #  , value='Ask your question'
                     )
    clear = gr.ClearButton([msg, chatbot, details])
    clear.click(reset_thread)

    def respond(message, chat_history):
        bot_message = process_example(message, chat_history)
        chat_history.append((message, bot_message))
        return "", chat_history, message_thread

    msg.submit(fn=respond,
        inputs=[msg, chatbot],
        outputs=[msg, chatbot, details])

# COMMAND ----------

app_port = 8766

# COMMAND ----------

print(spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId"))

# COMMAND ----------

cluster_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().clusterId().getOrElse(None)
workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")

print(f"Use this URL to access the chatbot app: ")
print(f"https://dbc-dp-{workspace_id}.cloud.databricks.com/driver-proxy/o/{workspace_id}/{cluster_id}/{app_port}/gradio/")

# COMMAND ----------

dbx_app = DatabricksApp(app_port)

# demo.queue()
dbx_app.mount_gradio_app(demo)

import nest_asyncio
nest_asyncio.apply()
dbx_app.run()

# COMMAND ----------


