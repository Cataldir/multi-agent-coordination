"""Real MCP stdio server over an authored read-only inventory fixture."""
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("coordination-teaching-inventory")


@mcp.tool()
def inventory_lookup(sku: str) -> dict[str, str | int]:
    """Read only; this operation does not reserve inventory."""
    if sku != "BOOK":
        raise ValueError("unknown teaching SKU")
    return {"sku": sku, "available": 8, "tenant": "teaching"}


if __name__ == "__main__":
    mcp.run(transport="stdio")
