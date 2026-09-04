"""vn.py CTA 原生运行时固定验收策略。"""

from vnpy_ctastrategy import CtaTemplate


class VnpyCtaAcceptanceV1(CtaTemplate):
    """固定快慢双均线交叉策略，用于验证订单和账户结算。"""

    author = "PXYBACKTEST"
    fast_window = 2
    slow_window = 3

    def on_init(self) -> None:
        self._closes: list[float] = []
        self._previous_fast: float | None = None
        self._previous_slow: float | None = None

    def on_bar(self, bar) -> None:
        self._closes.append(float(bar.close_price))
        if len(self._closes) < self.slow_window:
            return

        fast = sum(self._closes[-self.fast_window :]) / self.fast_window
        slow = sum(self._closes[-self.slow_window :]) / self.slow_window
        crossed_up = (
            self._previous_fast is not None
            and self._previous_slow is not None
            and self._previous_fast <= self._previous_slow
            and fast > slow
        )
        crossed_down = (
            self._previous_fast is not None
            and self._previous_slow is not None
            and self._previous_fast >= self._previous_slow
            and fast < slow
        )
        self._previous_fast = fast
        self._previous_slow = slow

        if self.pos == 0 and crossed_up:
            self.buy(bar.close_price + 10, 1)
        elif self.pos > 0 and crossed_down:
            self.sell(bar.close_price - 10, 1)


__all__ = ["VnpyCtaAcceptanceV1"]
