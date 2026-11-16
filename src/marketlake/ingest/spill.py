"""Local append-only journal for records Kafka didn't accept.

When the broker is unreachable longer than the producer's delivery timeout, or
the producer queue is full, records would otherwise be dropped. They are
appended here (fsynced) and replayed into Kafka once deliveries succeed again.
Replays can duplicate a record whose delivery was ambiguous; silver dedupes on
(exchange, symbol, trade_id).

Files: <path> is being appended to; <path>.replay is a batch taken for replay.
A leftover .replay after a crash is replayed again on the next start.
"""

from __future__ import annotations

import base64
import json
import os
import threading
from pathlib import Path


class SpillLog:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.replay_path = self.path.with_name(self.path.name + ".replay")
        self._f = None
        # Delivery callbacks fire from whichever thread polls the producer.
        self._lock = threading.Lock()
        self.spilled = 0

    def append(self, topic: str, key: bytes | None, value: bytes) -> None:
        rec = {
            "topic": topic,
            "key": base64.b64encode(key).decode() if key is not None else None,
            "value": base64.b64encode(value).decode(),
        }
        with self._lock:
            if self._f is None:
                self.path.parent.mkdir(parents=True, exist_ok=True)
                self._f = open(self.path, "ab")
            self._f.write(json.dumps(rec).encode() + b"\n")
            self._f.flush()
            os.fsync(self._f.fileno())
            self.spilled += 1

    def pending(self) -> bool:
        return self.replay_path.exists() or (self.path.exists() and self.path.stat().st_size > 0)

    def take(self) -> list[tuple[str, bytes | None, bytes]]:
        """Moves current spill into the replay file and returns all records in
        it. Call `done()` once every returned record is delivered."""
        with self._lock:
            if self._f is not None:
                self._f.close()
                self._f = None
            if self.path.exists():
                # Append (not replace) so an unfinished replay batch is kept.
                with open(self.replay_path, "ab") as out, open(self.path, "rb") as src:
                    out.write(src.read())
                    out.flush()
                    os.fsync(out.fileno())
                self.path.unlink()
        if not self.replay_path.exists():
            return []
        records = []
        for line in self.replay_path.read_bytes().splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            key = base64.b64decode(rec["key"]) if rec["key"] is not None else None
            records.append((rec["topic"], key, base64.b64decode(rec["value"])))
        return records

    def done(self) -> None:
        self.replay_path.unlink(missing_ok=True)

    def close(self) -> None:
        with self._lock:
            if self._f is not None:
                self._f.close()
                self._f = None
