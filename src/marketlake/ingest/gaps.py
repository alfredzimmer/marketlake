"""Sequence-gap detection over per-symbol contiguous trade ids."""

from __future__ import annotations

from datetime import UTC, datetime

from marketlake.models import Gap


class GapDetector:
    """Tracks the last trade_id seen per symbol and reports holes.

    Duplicates and out-of-order ids (<= last seen) are reported via `observe`
    returning (False, None) so the caller can count them; they are still
    forwarded downstream, where the silver layer dedupes on (exchange, symbol, trade_id).
    """

    def __init__(self, exchange: str) -> None:
        self.exchange = exchange
        self._last: dict[str, int] = {}

    def observe(self, symbol: str, trade_id: int) -> tuple[bool, Gap | None]:
        """Returns (is_new, gap). is_new is False for duplicates / stale ids."""
        last = self._last.get(symbol)
        if last is None:
            self._last[symbol] = trade_id
            return True, None
        if trade_id <= last:
            return False, None
        self._last[symbol] = trade_id
        if trade_id == last + 1:
            return True, None
        gap = Gap(
            exchange=self.exchange,
            symbol=symbol,
            first_missing=last + 1,
            last_missing=trade_id - 1,
            detected_ts=datetime.now(UTC).isoformat(),
        )
        return True, gap
