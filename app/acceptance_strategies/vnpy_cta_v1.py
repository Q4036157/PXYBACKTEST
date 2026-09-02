"""vn.py CTA 原生运行时固定验收策略。"""

from vnpy_ctastrategy import CtaTemplate


class VnpyCtaAcceptanceV1(CtaTemplate):
    """第一根线挂多单、成交后挂平单，用于验证订单和账户结算。"""

    author = "PXYBACKTEST"

    def on_init(self) -> None:
        self._opened = False
        self._closed = False

    def on_bar(self, bar) -> None:
        if self.pos == 0 and not self._opened:
            self._opened = True
            self.buy(bar.close_price + 10, 1)
        elif self.pos > 0 and not self._closed:
            self._closed = True
            self.sell(bar.close_price - 10, 1)


__all__ = ["VnpyCtaAcceptanceV1"]
