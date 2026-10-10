"""MCP1.29.0 adapter. No model sampling or external provider."""
from __future__ import annotations

import asyncio
import sys
from dataclasses import dataclass

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


@dataclass(frozen=True, slots=True)
class Lookup:
    sku: str
    available: int
    tenant: str


async def lookup(expected_tenant: str = "teaching") -> Lookup:
    params = StdioServerParameters(command=sys.executable,
                                  args=["-m", "protocol.mcp_inventory_server"])
    async with asyncio.timeout(10):
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool("inventory_lookup", {"sku": "BOOK"})
                body = result.structuredContent
                if result.isError or not isinstance(body, dict):
                    raise ValueError("no accepted structured lookup")
                if set(body) != {"sku", "available", "tenant"} or (
                    body["sku"] != "BOOK" or body["tenant"] != expected_tenant or
                    type(body["available"]) is not int or body["available"] < 0
                ):
                    raise ValueError("lookup violates application boundary")
                return Lookup("BOOK", body["available"], expected_tenant)


if __name__ == "__main__":
    print(asyncio.run(lookup()))
