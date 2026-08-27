"""聚宽与 PXYBACKTEST 的截面动量对照策略。

第一版只保留最少的交易规则，目的是验证两个回测引擎的口径，而不是追求最优收益：

* 沪深 300 历史成分股
* 过去 20 个交易日复权收盘价动量
* 每月第一个交易日开盘调仓
* 选动量最高的 5 只股票，等权持有
* 清仓不在目标组合中的股票
* 佣金、印花税按 A 股常见参数设置，滑点先设为 0

提交到 PXYBACKTEST 时，必须使用同一段日期、同一份历史成分股、同一复权口径、
同一费用和同一成交时点。否则两个结果不应要求完全相同。
"""


def initialize(context):
    # 防止研究代码无意中读取未来数据。
    set_option("avoid_future_data", True)
    set_option("use_real_price", True)

    set_benchmark("000300.XSHG")
    set_option("order_volume_ratio", 1.0)

    # 买入不收印花税，卖出收印花税；佣金最低 5 元；第一轮先关闭滑点。
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,
            open_commission=0.0003,
            close_commission=0.0003,
            close_today_commission=0,
            min_commission=5,
        ),
        type="stock",
    )
    set_slippage(FixedSlippage(0))

    g.index = "000300.XSHG"
    g.momentum_days = 20
    g.holding_count = 5

    # time='open' 表示使用上一个交易日收盘前已经确定的信号，在开盘下单。
    run_monthly(rebalance, monthday=1, time="open")


def rebalance(context):
    # 显式按 previous_date 取历史成分股，避免使用当前成分股造成幸存者偏差。
    stocks = get_index_stocks(g.index, date=context.previous_date)
    if not stocks:
        return

    # 在开盘回调中，history 的最后一根应是 previous_date；只使用已经完成的 K 线。
    close = history(
        g.momentum_days + 1,
        unit="1d",
        field="close",
        security_list=stocks,
        skip_paused=False,
        df=True,
        fq="pre",
    )
    if close is None or close.empty:
        return

    # 20 日动量 = 最新复权收盘价 / 20 个交易日前复权收盘价 - 1。
    momentum = close.iloc[-1].div(close.iloc[0]).sub(1).dropna()
    if momentum.empty:
        return

    target = list(momentum.sort_values(ascending=False).head(g.holding_count).index)
    target_set = set(target)

    # 先卖出不在目标组合的股票，避免旧仓位挤占新仓位资金。
    for security in list(context.portfolio.positions):
        if security not in target_set:
            order_target_value(security, 0)

    if not target:
        return

    target_value = context.portfolio.total_value / len(target)
    for security in target:
        order_target_value(security, target_value)


def handle_data(context, data):
    # 所有交易只在 rebalance 中发生。
    return

