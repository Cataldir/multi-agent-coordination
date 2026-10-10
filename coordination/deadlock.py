"""Actual asyncio lock cycle with bounded detection and an ordered fix."""
from __future__ import annotations

import asyncio


async def reproduce(timeout: float = 0.2) -> str:
    """Hold opposite locks, synchronize, then prove neither can complete."""
    left, right = asyncio.Lock(), asyncio.Lock()
    both_hold_one = asyncio.Barrier(2)

    async def peer(first: asyncio.Lock, second: asyncio.Lock) -> None:
        async with first:
            await both_hold_one.wait()
            async with second:
                return

    tasks = [asyncio.create_task(peer(left, right)),
             asyncio.create_task(peer(right, left))]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks), timeout)
        raise AssertionError("unexpected completion: intended cycle absent")
    except TimeoutError:
        return "cycle A -> right held by B -> left held by A; timeout aborted"
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        assert not left.locked() and not right.locked()


async def ordered(timeout: float = 1.0) -> tuple[str, str]:
    """Global left-before-right ordering removes circular wait."""
    left, right = asyncio.Lock(), asyncio.Lock()

    async def peer(name: str) -> str:
        async with left:
            await asyncio.sleep(0)
            async with right:
                return name

    async with asyncio.timeout(timeout):
        async with asyncio.TaskGroup() as group:
            a = group.create_task(peer("A completed"))
            b = group.create_task(peer("B completed"))
    return a.result(), b.result()


async def main() -> None:
    print(await reproduce())
    print(await ordered())


if __name__ == "__main__":
    asyncio.run(main())
