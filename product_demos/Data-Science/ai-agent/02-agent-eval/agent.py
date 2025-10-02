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
        response = client.call_tool(self.name, kwargs)
        return "".join([c.text for c in response.content])


class MCPAgent:
    def __init__(self, mcp_servers=None):
        self.tools = []
        self.ws_client = WorkspaceClient()

        host = self.ws_client.config.host
        server_urls = []

        # Normalize servers: custom URLs vs managed short names
        for s in mcp_servers or []:
            if isinstance(s, str) and s.startswith("http"):
                server_urls.append(s)  # custom MCP
            else:
                server_urls.append(f"{host}/api/2.0/mcp/functions/system/{s}")  # managed MCP

        # Connect to each MCP server and load tools
        for url in server_urls:
            try:
                print(f"Connecting to MCP server: {url}")
                client = DatabricksMCPClient(server_url=url, workspace_client=self.ws_client)
                tool_defs = client.list_tools()
                print(f" -> Found {len(tool_defs)} tools on {url}")
                for t in tool_defs:
                    print(f"    - {t.name}: {t.description}")
                    self.tools.append(MCPToolWrapper(t.name, t.description or t.name, url, self.ws_client))
            except Exception as e:
                print(f"Error loading MCP tools from {url}: {e}")

    def list_tools(self):
        return [t.name for t in self.tools]

    def call_tool(self, tool_name, **kwargs):
        for t in self.tools:
            if t.name == tool_name:
                return t.run(**kwargs)
        raise ValueError(f"Tool {tool_name} not found")