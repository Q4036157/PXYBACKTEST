"""天勤原生固定向量：黄金 1 分钟双均线。"""

from tqsdk import TargetPosTask, TqApi


SYMBOL = "SHFE.au2612"
api = TqApi()
klines = api.get_kline_serial(SYMBOL, duration_seconds=60, data_length=30)
target = TargetPosTask(api, SYMBOL)

while True:
    api.wait_update()
    if not api.is_changing(klines.iloc[-1], "datetime"):
        continue
    closes = klines["close"]
    fast = float(closes.iloc[-5:].mean())
    slow = float(closes.iloc[-20:].mean())
    target.set_target_volume(1 if fast > slow else 0)
