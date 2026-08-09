import logging

from app.worker_process import (
    _configure_backtest_worker_logging,
    build_replay_event_snapshot,
)


def test_replay_event_snapshot_is_bounded_to_twenty_items() -> None:
    events = [{"seq": index} for index in range(100)]

    snapshot = build_replay_event_snapshot(events)

    assert len(snapshot) == 20
    assert snapshot[0]["seq"] == 80
    assert snapshot[-1]["seq"] == 99


def test_backtest_worker_logging_emits_info_once(capsys) -> None:
    logger = logging.getLogger("backtest_service")
    original_handlers = list(logger.handlers)
    original_level = logger.level
    original_propagate = logger.propagate
    try:
        logger.handlers.clear()
        _configure_backtest_worker_logging()
        _configure_backtest_worker_logging()

        logger.info("回测优先加载 PXYDATA 1m K线")

        assert len(logger.handlers) == 1
        assert "回测优先加载 PXYDATA 1m K线" in capsys.readouterr().err
    finally:
        logger.handlers[:] = original_handlers
        logger.setLevel(original_level)
        logger.propagate = original_propagate
