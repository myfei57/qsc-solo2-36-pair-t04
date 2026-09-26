"""Batch identity and the decision taken for each batch.

A batch identifier is claimed exactly once by a ``batch.open`` record and
closed at most once by a ``batch.close`` record.  Both records are committed
before the call returns, so a second open with the same identifier or a second
decision for the same batch is refused as a duplicate rather than silently
overwriting what the operator already recorded.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from line_control.runtime.clock import LogicalClock
from line_control.runtime.errors import (
    DuplicateRecordError,
    UnknownReferenceError,
    ValidationError,
)
from line_control.runtime.keys import scope_key
from line_control.store.records import Record
from line_control.store.stream import RecordStream

OPEN_KIND = "batch.open"
CLOSE_KIND = "batch.close"


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
    """Stores one opening and, at most, one decision for each batch."""

    def __init__(self, stream: RecordStream, clock: LogicalClock) -> None:
        self._stream = stream
        self._clock = clock

    def key(self, batch_id: str) -> str:
        """Return the stream key of one batch."""
        return scope_key("batch", batch_id)

    def open(self, batch_id: str, unit: str, subject: str = "", generation: int = 0) -> Batch:
        """Claim a batch identifier exactly once."""
        if not batch_id or not unit:
            raise ValidationError("batch identifier and unit are required")
        existing = self.read(batch_id)
        if existing is not None:
            raise DuplicateRecordError(
                f"batch {batch_id} was already opened",
                batch_id=batch_id,
                opened_tick=existing.opened_tick,
            )
        tick = self._clock.tick()
        record = self._stream.append(
            OPEN_KIND,
            self.key(batch_id),
            {
                "batch_id": batch_id,
                "unit": unit,
                "subject": subject,
                "opened_tick": tick,
            },
            generation=int(generation),
        )
        self._stream.commit_upto(record.seq)
        return Batch(batch_id, unit, subject, tick, generation=int(generation))

    def resolve(self, batch_id: str, decision: str) -> Batch:
        """Store the decision taken for a batch, refusing a second one."""
        if not decision:
            raise ValidationError("a decision is required", batch_id=batch_id)
        batch = self.require(batch_id)
        if not batch.is_open:
            raise DuplicateRecordError(
                f"batch {batch_id} was already closed",
                batch_id=batch_id,
                decision=batch.decision,
                closed_tick=batch.closed_tick,
            )
        tick = self._clock.tick()
        record = self._stream.append(
            CLOSE_KIND,
            self.key(batch_id),
            {"batch_id": batch_id, "closed_tick": tick, "decision": decision},
            generation=batch.generation,
        )
        self._stream.commit_upto(record.seq)
        return Batch(
            batch.batch_id,
            batch.unit,
            batch.subject,
            batch.opened_tick,
            closed_tick=tick,
            decision=decision,
            generation=batch.generation,
        )

    def read(self, batch_id: str) -> Batch | None:
        """Return a batch, or ``None`` when its identifier was never claimed."""
        history = self._stream.trace(self.key(batch_id))
        if not history:
            return None
        opened = self._record_of_kind(history, OPEN_KIND)
        if opened is None:
            return None
        payload = opened.payload
        closed = self._record_of_kind(history, CLOSE_KIND)
        return Batch(
            batch_id=batch_id,
            unit=str(payload.get("unit", "")),
            subject=str(payload.get("subject", "")),
            opened_tick=int(payload.get("opened_tick", opened.tick)),
            closed_tick=None if closed is None else int(closed.payload["closed_tick"]),
            decision="" if closed is None else str(closed.payload.get("decision", "")),
            generation=opened.generation,
        )

    @staticmethod
    def _record_of_kind(history: list[Record], kind: str) -> Record | None:
        """Return the last committed record of one kind in a key history."""
        for record in reversed(history):
            if record.kind == kind:
                return record
        return None

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
        open_batches: list[Batch] = []
        for record in self._stream.visible(OPEN_KIND):
            batch = self.read(str(record.payload.get("batch_id", "")))
            if batch is None or not batch.is_open:
                continue
            if unit is not None and batch.unit != unit:
                continue
            open_batches.append(batch)
        open_batches.sort(key=lambda batch: (batch.opened_tick, batch.batch_id))
        return open_batches

    def count(self) -> int:
        """Return how many batch identifiers have been claimed."""
        return len(self._stream.visible(OPEN_KIND))
