"""Normalized event schemas shared across exchanges."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True, slots=True)
class Trade:
    exchange: str
    symbol: str  # normalized, e.g. "BTC-USD"
    trade_id: int
    price: str  # decimal string; never round-trip prices through float
    size: str
    taker_side: str  # "buy" | "sell"
    event_ts: str  # exchange timestamp, ISO-8601 UTC
    ingest_ts: str  # when our ingestor received it

    @property
    def key(self) -> str:
        return f"{self.exchange}:{self.symbol}"

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, raw: bytes) -> Trade:
        return cls(**json.loads(raw))


@dataclass(frozen=True, slots=True)
class Gap:
    """A hole in a contiguous trade_id stream, to be backfilled from REST."""

    exchange: str
    symbol: str
    first_missing: int
    last_missing: int
    detected_ts: str

    @property
    def size(self) -> int:
        return self.last_missing - self.first_missing + 1

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()
