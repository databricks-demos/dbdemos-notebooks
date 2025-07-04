from typing import Any, Generator, Optional, Sequence, List, Dict
import mlflow
from mlflow.entities import SpanType
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse, ResponsesAgentStreamEvent

from databricks_langchain import (
    ChatDatabricks,
    VectorSearchRetrieverTool,
    DatabricksFunctionClient,
    UCFunctionToolkit,
    set_uc_function_client
)

from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from unitycatalog.ai.langchain.toolkit import UnityCatalogTool

from langchain_core.messages.ai import AIMessage
from langchain_core.messages.tool import ToolMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.language_models import LanguageModelLike
from langchain_core.tools import BaseTool

from langgraph.graph import END, StateGraph
from langgraph.graph.state import CompiledStateGraph
from langgraph.prebuilt.tool_node import ToolNode

# Enable autologging and UC tool setup
mlflow.langchain.autolog()
set_uc_function_client(DatabricksFunctionClient())

class ToolCallingAgent(ResponsesAgent):
    def __init__(
        self,
        uc_tool_names: Sequence[str] = ("main_build.dbdemos_ai_agent.*",),
        llm_endpoint_name: str = "databricks-claude-sonnet-4",
        system_prompt: Optional[str] = None,
        retriever_config: Optional[dict] = None,
        max_history_messages: int = 20,
    ):
        self.llm_endpoint_name = llm_endpoint_name
        self.llm = ChatDatabricks(endpoint=llm_endpoint_name)
        self.tools: List[BaseTool] = UCFunctionToolkit(function_names=list(uc_tool_names)).tools

        if retriever_config:
            self.tools.append(
                VectorSearchRetrieverTool(
                    index_name=retriever_config.get("index_name"),
                    name=retriever_config.get("tool_name"),
                    description=retriever_config.get("description"),
                    num_results=retriever_config.get("num_results"),
                )
            )

        self.max_history_messages = max_history_messages
        self.system_prompt = system_prompt
        self.graph: CompiledStateGraph = self._build_graph(system_prompt)

    def _build_graph(self, system_prompt: Optional[str]) -> CompiledStateGraph:
        model = self.llm.bind_tools(self.tools)

        def should_continue(state: dict):
            last = state["messages"][-1]
            tc = (last.get("tool_calls") if isinstance(last, dict)
                  else last.tool_calls if isinstance(last, AIMessage)
                  else None)
            return "continue" if tc else "end"

        pre = RunnableLambda(lambda s: [{"role": "system", "content": system_prompt}] + s["messages"]) if system_prompt \
              else RunnableLambda(lambda s: s["messages"])
        runnable = pre | model

        def call_agent(state, config):
            ai_msg = runnable.invoke(state, config)
            return {"messages": state["messages"] + [ai_msg]}

        def call_tool(state, config):
            tool_node = ToolNode(self.tools)
            result = tool_node.invoke(state, config)
            tool_messages = result.get("messages", [])
            if not all(isinstance(m, ToolMessage) for m in tool_messages):
                raise ValueError("Expected ToolMessage list in tool node result")
            return {"messages": state["messages"] + tool_messages}

        graph = StateGraph(dict)
        graph.add_node("agent", RunnableLambda(call_agent))
        graph.add_node("tools", RunnableLambda(call_tool))
        graph.set_entry_point("agent")
        graph.add_conditional_edges("agent", should_continue, {"continue": "tools", "end": END})
        graph.add_edge("tools", "agent")
        return graph.compile()

    def _mlflow_messages_to_dicts(self, messages: Sequence[Any]) -> List[Dict[str, str]]:
        return [{"role": m.role, "content": m.content,} for m in messages]

    def _summarize_history(self, messages: List[Dict[str, str]]) -> str:
        llm = ChatDatabricks(endpoint=self.llm_endpoint_name)
        prompt = "Summarize the following conversation between a user and an assistant:\n\n"
        for m in messages:
            prompt += f"{m['role'].capitalize()}: {m['content']}\n"
        prompt += "\nSummary:"
        response = llm.invoke([{"role": "user", "content": prompt}])
        return response.content if hasattr(response, "content") else str(response)

    def _truncate_and_summarize_history(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        if len(messages) <= self.max_history_messages:
            return messages
        to_summarize = messages[:-self.max_history_messages]
        recent = messages[-self.max_history_messages:]
        summary = self._summarize_history(to_summarize)
        summarized_history = [{"role": "system", "content": f"Summary of earlier conversation: {summary}"}] + recent
        return summarized_history

    def _stream_events(self, request: ResponsesAgentRequest):
        full_history = self._mlflow_messages_to_dicts(request.input)
        processed_history = self._truncate_and_summarize_history(full_history)
        state = {"messages": processed_history}
        for event in self.graph.stream(state, stream_mode="updates"):
            for node_out in event.values():
                for msg in node_out.get("messages", []):
                    if isinstance(msg, AIMessage):
                        content, msg_id = msg.content, getattr(msg, "id", "")
                    elif isinstance(msg, ToolMessage):
                        content, msg_id = msg.content, getattr(msg, "tool_call_id", "")
                    else:
                        content, msg_id = msg.get("content"), msg.get("id", "")
                    yield content, msg_id

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        items = []
        for content, msg_id in self._stream_events(request):
            items.append(self.create_text_output_item(text=content, id=msg_id or ""))
        return ResponsesAgentResponse(output=items)

    def predict_stream(self, request: ResponsesAgentRequest):
        for content, msg_id in self._stream_events(request):
            yield ResponsesAgentStreamEvent(**self.create_text_delta(delta=content, item_id=msg_id or ""))
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done",
                item=self.create_text_output_item(text=content, id=msg_id or "")
            )

    def get_resources(self):
        res = [DatabricksServingEndpoint(endpoint_name=self.llm.endpoint)]
        for t in self.tools:
            if isinstance(t, VectorSearchRetrieverTool):
                res.extend(t.resources)
            elif isinstance(t, UnityCatalogTool):
                res.append(DatabricksFunction(function_name=t.uc_function_name))
        return res

# Example config loading
model_config = mlflow.models.ModelConfig(development_config='../02_agent_eval/agent_config.yaml')

AGENT = ToolCallingAgent(
    uc_tool_names=model_config.get('uc_tool_names'),
    llm_endpoint_name=model_config.get('llm_endpoint_name'),
    system_prompt=model_config.get('system_prompt'),
    retriever_config=model_config.get('retriever_config'),
    max_history_messages=20
)

mlflow.models.set_model(AGENT)
