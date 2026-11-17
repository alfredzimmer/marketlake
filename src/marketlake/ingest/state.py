"""Durable per-symbol trade-id high-water marks, carried across ingestor runs.

The file holds the highest trade_id per symbol that is *durable*: acknowledged
by Kafka or written to the spill journal. Ids the process saw but hadn't
secured when it died are above the mark, so the next run reports them as part
of the downtime gap and they get backfilled instead of silently lost (#8).
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path


class TradeIdState:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        # Delivery callbacks fire from whichever thread polls the producer.
        self._lock = threading.Lock()
        self._last: dict[str, int] = {}
        if self.path.exists():
            self._last = {k: int(v) for k, v in json.loads(self.path.read_text()).items()}
        self._dirty = False

    def last(self) -> dict[str, int]:
        with self._lock:
            return dict(self._last)

    def advance(self, symbol: str, trade_id: int) -> None:
        with self._lock:
            if trade_id > self._last.get(symbol, -1):
                self._last[symbol] = trade_id
                self._dirty = True

    def save(self) -> None:
        """Atomically replaces the state file (write temp, fsync, rename)."""
        with self._lock:
            if not self._dirty:
                return
            data = json.dumps(self._last, sort_keys=True).encode()
            self._dirty = False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_name(self.path.name + ".tmp")
            with open(tmp, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        except OSError:
            with self._lock:
                self._dirty = True
            raise
