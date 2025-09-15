from typing import Literal
from pydantic_settings import BaseSettings


class MCPSettings(BaseSettings):
    mcp_server_type: Literal["sse", "stdio"] = "sse"
    mcp_host: str = "0.0.0.0"
    mcp_port: int = 5000

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    @property
    def get_mcp_uri (self) -> str:
        if self.mcp_server_type == "stdio":
            return "stdio://"
        # SSE
        return f"http://{self.mcp_host}:{self.mcp_port}"


mcpsettings = MCPSettings()