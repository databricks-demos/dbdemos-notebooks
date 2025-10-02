from databricks_mcp import DatabricksMCPClient
from databricks.sdk import WorkspaceClient
from langchain_core.tools import BaseTool

class MCPToolWrapper(BaseTool):
    """Wrap a Databricks MCP tool as a LangChain BaseTool"""

    def __init__(self, name: str, description: str, server_url: str, ws_client: WorkspaceClient):
        super().__init__(name=name, description=description, args_schema=dict)
        self.server_url = server_url
        self.ws_client = ws_client

    def _run(self, **kwargs) -> str:
        client = DatabricksMCPClient(server_url=self.server_url, workspace_client=self.ws_client)
        response = client.call_tool(self.name, kwargs)  # synchronous call
        return "".join([c.text for c in response.content])


class MinimalMCPAgent:
    def __init__(self, mcp_server_urls):
        self.tools = []
        self.ws_client = WorkspaceClient()

        for server_url in mcp_server_urls:
            try:
                print(f"Connecting to MCP server: {server_url}")
                client = DatabricksMCPClient(server_url=server_url, workspace_client=self.ws_client)
                tool_defs = client.list_tools()  # ✅ synchronous
                print(f" -> Found {len(tool_defs)} tools on {server_url}")
                for t in tool_defs:
                    print(f"    - {t.name}: {t.description}")
                    self.tools.append(MCPToolWrapper(t.name, t.description or t.name, server_url, self.ws_client))
            except Exception as e:
                print(f"Error loading MCP tools from {server_url}: {e}")

    def list_tools(self):
        return [t.name for t in self.tools]

    def call_tool(self, tool_name, **kwargs):
        for t in self.tools:
            if t.name == tool_name:
                return t.run(**kwargs)
        raise ValueError(f"Tool {tool_name} not found")