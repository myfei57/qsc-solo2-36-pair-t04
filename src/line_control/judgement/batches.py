"""Batch identity and the decision taken for each batch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from line_control.runtime.clock import LogicalClock
from line_control.runtime.errors import UnknownReferenceError, ValidationError
from line_control.runtime.keys import scope_key
from line_control.store.stream import RecordStream


@dataclass(frozen=True)
class Batch:
    """One named run of one unit."""

    batch_id: str
    unit: str
    subject: str
    opened_tick: int
    closed_tick: int | None = None
    decision: str = ""
    generation: int = 0

    @property
    def is_open(self) -> bool:
        """Report whether the batch is still collecting."""
        return self.closed_tick is None

    def to_dict(self) -> dict[str, Any]:
        """Render the batch for the wire."""
        return {
            "batchId": self.batch_id,
            "unit": self.unit,
            "subject": self.subject,
            "openedTick": self.opened_tick,
            "closedTick": self.closed_tick,
            "decision": self.decision,
            "generation": self.generation,
            "open": self.is_open,
        }


class BatchRegistry:
    """Stores the decision taken for each batch."""

    def __init__(self, stream: RecordStream, clock: LogicalClock) -> None:
        self._stream = stream
        self._clock = clock

    def key(self, batch_id: str) -> str:
        """Return the stream key of one batch."""
        return scope_key("batch", batch_id)

    def open(self, batch_id: str, unit: str, subject: str = "", generation: int = 0) -> Batch:
        """Start a batch."""
        if not batch_id or not unit:
            raise ValidationError("batch identifier and unit are required")
        tick = self._clock.tick()
        return Batch(batch_id, unit, subject, tick, generation=int(generation))

    def resolve(self, batch_id: str, decision: str) -> Batch:
        """Store the decision taken for a batch."""
        if not decision:
            raise ValidationError("a decision is required", batch_id=batch_id)
        tick = self._clock.tick()
        record = self._stream.append(
            "batch.close",
            self.key(batch_id),
            {"batch_id": batch_id, "closed_tick": tick, "decision": decision},
        )
        self._stream.commit_upto(record.seq)
        return Batch(
            batch_id, "", "", tick, closed_tick=tick, decision=decision
        )

    def read(self, batch_id: str) -> Batch | None:
        """Return a batch, or ``None`` when no decision was stored."""
        record = self._stream.visible_view().current(self.key(batch_id))
        if record is None:
            return None
        payload = record.payload
        return Batch(
            batch_id=batch_id,
            unit=str(payload.get("unit", "")),
            subject=str(payload.get("subject", "")),
            opened_tick=int(payload.get("closed_tick", 0)),
            closed_tick=payload.get("closed_tick"),
            decision=str(payload.get("decision", "")),
            generation=int(payload.get("generation", 0)),
        )

    def require(self, batch_id: str) -> Batch:
        """Return a batch, refusing an identifier that was never claimed."""
        batch = self.read(batch_id)
        if batch is None:
            raise UnknownReferenceError(
                f"batch {batch_id} was never opened", batch_id=batch_id
            )
        return batch

    def open_batches(self, unit: str | None = None) -> list[Batch]:
        """Return the batches that are still open, optionally for one unit."""
        return []

    def count(self) -> int:
        """Return how many decisions have been stored."""
        return len(self._stream.visible("batch.close"))
