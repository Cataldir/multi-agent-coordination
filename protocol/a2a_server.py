"""Real A2A0.3 JSON-RPC loopback server; deterministic peer, no LLM.

In-memory SDK task storage intentionally demonstrates protocol only. It does
not replace the durable learner business store or establish restart recovery.
"""
from __future__ import annotations

import asyncio

import uvicorn
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.apps import A2AStarletteApplication
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import AgentCapabilities, AgentCard, AgentSkill, Part, TextPart
from a2a.utils import new_task


class Supplier(AgentExecutor):
    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        if context.message is None:
            raise ValueError("message required")
        task = context.current_task or new_task(context.message)
        if context.current_task is None:
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(event_queue, task.id, task.context_id)
        await updater.start_work()
        await asyncio.sleep(0.05)
        await updater.add_artifact([Part(root=TextPart(
            text="Synthetic offer: Friday, quantity2, USD2400; proposal only"))],
            name="teaching-offer")
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        if context.current_task is None:
            raise ValueError("known task required")
        updater = TaskUpdater(event_queue, context.current_task.id,
                              context.current_task.context_id)
        await updater.cancel()


def main() -> None:
    card = AgentCard(name="Teaching supplier", description="No-effect fixture peer",
                     url="http://127.0.0.1:8011/", version="0.1.0",
                     capabilities=AgentCapabilities(streaming=False),
                     default_input_modes=["text"], default_output_modes=["text"],
                     skills=[AgentSkill(id="offer", name="Offer", description=
                     "Synthetic delivery proposal", tags=["teaching"])])
    handler = DefaultRequestHandler(agent_executor=Supplier(),
                                    task_store=InMemoryTaskStore())
    app = A2AStarletteApplication(agent_card=card, http_handler=handler).build()
    uvicorn.run(app, host="127.0.0.1", port=8011, log_level="warning")


if __name__ == "__main__":
    main()
