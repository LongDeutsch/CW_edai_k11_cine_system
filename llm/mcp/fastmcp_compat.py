"""FastMCP compatibility and tool management layer.

Cung cấp interface tương thích FastMCP (Model Context Protocol).
Hỗ trợ đăng ký tools, sinh JSON schema, và gọi hàm tự động cho LLM Tool Calling.
"""

from __future__ import annotations

import inspect
import json
from typing import Any, Callable


class FastMCP:
    """FastMCP server implementation tương thích chuẩn Model Context Protocol."""

    def __init__(self, name: str = "cinestream-concierge", description: str = "") -> None:
        self.name = name
        self.description = description
        self._tools: dict[str, dict[str, Any]] = {}

    def tool(self, name: str | None = None, description: str | None = None) -> Callable:
        """Decorator đăng ký một function thành MCP Tool."""

        def decorator(func: Callable) -> Callable:
            tool_name = name or func.__name__
            tool_doc = description or inspect.getdoc(func) or f"Tool {tool_name}"

            # Phân tích chữ ký hàm để sinh JSON Schema
            sig = inspect.signature(func)
            properties: dict[str, Any] = {}
            required: list[str] = []

            for param_name, param in sig.parameters.items():
                param_type = "string"
                if param.annotation == int:
                    param_type = "integer"
                elif param.annotation == float:
                    param_type = "number"
                elif param.annotation == bool:
                    param_type = "boolean"
                elif param.annotation in (list, list[int], list[str]):
                    param_type = "array"
                elif param.annotation == dict:
                    param_type = "object"

                properties[param_name] = {
                    "type": param_type,
                    "description": f"Tham số {param_name}",
                }
                if param.default == inspect.Parameter.empty:
                    required.append(param_name)

            self._tools[tool_name] = {
                "name": tool_name,
                "description": tool_doc,
                "func": func,
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                },
            }
            return func

        return decorator

    def list_tools(self) -> list[dict[str, Any]]:
        """Trả về danh sách tools chuẩn MCP format."""
        result = []
        for t in self._tools.values():
            result.append(
                {
                    "name": t["name"],
                    "description": t["description"],
                    "inputSchema": t["parameters"],
                }
            )
        return result

    def call_tool(self, name: str, arguments: dict[str, Any] | None = None) -> Any:
        """Gọi thực thi một tool đã đăng ký."""
        if name not in self._tools:
            raise KeyError(f"Tool '{name}' không tồn tại trong MCP server.")
        func = self._tools[name]["func"]
        args = arguments or {}
        return func(**args)
