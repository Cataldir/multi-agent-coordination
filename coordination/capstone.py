"""One deterministic learner integration run, no model/provider use."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .envelope import Operation
from .saga import Coordinator, ScriptedPeer
from .store import Principal, Store


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--fail-at", choices=[o.value for o in Operation], default="")
    parser.add_argument("--unknown-at", choices=[o.value for o in Operation], default="")
    args = parser.parse_args()
    store = Store(args.database)
    try:
        coordinator = Coordinator(store, Principal("teaching", "coordinator"),
                                  args.run)
        state = coordinator.forward(ScriptedPeer(), fail_at=args.fail_at,
                                    unknown_at=args.unknown_at)
        print(json.dumps({"fixture": "deterministic-scripted-peer",
                          "run": args.run, "state": state,
                          "projection": store.counts("teaching", args.run),
                          "replayed": store.replay("teaching", args.run),
                          "pending_outbox": len(store.pending())}))
    finally:
        store.close()


if __name__ == "__main__":
    main()
