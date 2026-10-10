"""Versioned commands. Correlation fields carry no authorization authority."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping


class Refusal(ValueError):
    """The command does not satisfy the declared boundary."""


class Operation(StrEnum):
    RESERVE = "reserve"
    CHARGE = "charge"
    PROVISION = "provision"
    NOTIFY = "notify"
    RELEASE = "release"
    REFUND = "refund"
    DEPROVISION = "deprovision"


@dataclass(frozen=True, slots=True)
class Payload:
    order_id: str
    quantity: int
    cents: int
    currency: str = "USD"

    def __post_init__(self) -> None:
        if not isinstance(self.order_id, str) or not 1 <= len(self.order_id) <= 80:
            raise Refusal("bounded order id required")
        if type(self.quantity) is not int or not 1 <= self.quantity <= 100:
            raise Refusal("quantity must be an integer in 1..100")
        if type(self.cents) is not int or not 1 <= self.cents <= 1_000_000:
            raise Refusal("cents must be an integer in 1..1000000")
        if self.currency != "USD":
            raise Refusal("this teaching edition supports USD only")

    def wire(self) -> dict[str, str | int]:
        return {"order_id": self.order_id, "quantity": self.quantity,
                "cents": self.cents, "currency": self.currency}


@dataclass(frozen=True, slots=True)
class Envelope:
    message_id: str
    run_id: str
    correlation_id: str
    causation_id: str | None
    tenant: str
    owner: str
    fence: int
    key: str
    operation: Operation
    payload: Payload
    schema_version: int = 2

    def __post_init__(self) -> None:
        for value in (self.message_id, self.run_id, self.correlation_id,
                      self.tenant, self.owner, self.key):
            if not isinstance(value, str) or not 1 <= len(value) <= 120:
                raise Refusal("bounded non-empty identifiers required")
        if self.causation_id is not None and (
            not isinstance(self.causation_id, str) or
            not 1 <= len(self.causation_id) <= 120
        ):
            raise Refusal("invalid causation id")
        if type(self.fence) is not int or self.fence <= 0:
            raise Refusal("positive integer fence required")
        if self.schema_version != 2:
            raise Refusal("internal envelope must be normalized v2")
        if not isinstance(self.operation, Operation):
            raise Refusal("typed operation required")
        if not isinstance(self.payload, Payload):
            raise Refusal("typed payload required")

    def intent(self) -> str:
        """Exclude attempt/correlation fields from stable business intent."""
        value = {"tenant": self.tenant, "run": self.run_id,
                 "operation": self.operation.value, "payload": self.payload.wire()}
        return hashlib.sha256(json.dumps(value, sort_keys=True,
                                        separators=(",", ":")).encode()).hexdigest()


def decode(raw: Mapping[str, object]) -> Envelope:
    """Strict v1 compatibility adapter, followed by one internal v2 contract."""
    required = {"message_id", "run_id", "correlation_id", "causation_id",
                "tenant", "owner", "fence", "key", "operation",
                "payload", "schema_version"}
    if set(raw) != required:
        raise Refusal("missing or unknown envelope fields")
    version = raw["schema_version"]
    if type(version) is not int or version not in (1, 2):
        raise Refusal("unsupported schema version")
    body = raw["payload"]
    if not isinstance(body, dict):
        raise Refusal("payload must be an object")
    keys = {"order_id", "quantity", "cents"}
    if set(body) != (keys if version == 1 else keys | {"currency"}):
        raise Refusal("payload schema mismatch")
    if not isinstance(body["order_id"], str):
        raise Refusal("order id must be text")
    payload = Payload(body["order_id"], body["quantity"], body["cents"],
                      body.get("currency", "USD"))  # type: ignore[arg-type]
    try:
        operation = Operation(raw["operation"])
    except (ValueError, TypeError) as exc:
        raise Refusal("unsupported operation") from exc
    return Envelope(**{k: raw[k] for k in required - {
        "payload", "schema_version", "operation"}},
        payload=payload, operation=operation)  # type: ignore[arg-type]
