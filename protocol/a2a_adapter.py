"""Actual legacy A2A0.3 SDK client boundary, with bounded loopback smoke.

This API is pinned to504148064d01b5cbd64b4cbe11a17d477acebc9e. Do not
upgrade to1.x without reviewing protocol/types. Scripted core is separate.
"""
from __future__ import annotations

import asyncio
import uuid

import httpx
from a2a.client.card_resolver import A2ACardResolver
from a2a.client.client import Client, ClientConfig
from a2a.client.client_factory import ClientFactory
from a2a.types import Message, Part, Task, TaskQueryParams, TextPart


async def negotiate(client: Client, context: str, request_id: str) -> Task:
    """Send a real SDK Message and retain task identity for later query."""
    message = Message(role="user", message_id=request_id, context_id=context,
                      parts=[Part(root=TextPart(text="Offer Friday for order-11"))])
    latest: Task | None = None
    async with asyncio.timeout(10):
        async for event in client.send_message(message):
            if isinstance(event, Message):
                raise ValueError("this boundary requires a durable task")
            task, _update = event
            if task.context_id != context:
                raise ValueError("A2A context mismatch")
            latest = task
        if latest is None:
            raise ValueError("no task returned")
        observed = await client.get_task(TaskQueryParams(id=latest.id))
        if observed.id != latest.id or observed.context_id != context:
            raise ValueError("queried task identity mismatch")
        return observed


async def smoke() -> None:
    """Only the included loopback server; no arbitrary URL or credential path."""
    async with httpx.AsyncClient(timeout=5, follow_redirects=False) as http:
        card = await A2ACardResolver(http, "http://127.0.0.1:8011").get_agent_card()
        if card.url != "http://127.0.0.1:8011/" or card.version != "0.1.0":
            raise ValueError("unexpected peer identity or edition")
        # Refuse alternate routes; remote card data cannot redirect this smoke.
        if card.additional_interfaces:
            raise ValueError("unexpected alternate peer routes")
        client = ClientFactory(ClientConfig(httpx_client=http, streaming=False,
                                           polling=True)).create(card)
        task = await negotiate(client, "teaching-context", str(uuid.uuid4()))
        print(task.model_dump_json(exclude_none=True))


if __name__ == "__main__":
    asyncio.run(smoke())
