"""时间序列机器学习回测。

该模块只消费 PXYDATA 的不可变快照，不读取运行目录中的“最新文件”。
内置 ``linear_regression`` 是一个无第三方 ML 依赖的基线，便于在工作站先验收
数据契约；LightGBM/Transformer 通过延迟导入作为可选模型接入。训练集和
测试集按事件时间排序，并显式应用 purge/embargo，避免把未来信息带入训练。
"""

from __future__ import annotations

import hashlib
import math
import re
import statistics
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any


LEARNING_DATASET_NAMES = {
    "ml_features_daily",
    "factor_matrix_daily",
    "lighter_microstructure_factors",
}
ML_ENGINE_TYPES = {"ml_factor", "deep_learning"}
ML_STRATEGY_ID = "temporal_ml_rank_v1"
ML_STRATEGY_HASH = hashlib.sha256(b"pxybacktest.temporal-ml-rank.v1").hexdigest()


class LearningBacktestError(ValueError):
    """学习数据、切分或模型配置不满足回测契约。"""


@lru_cache(maxsize=1)
def learning_runtime_capabilities() -> dict[str, Any]:
    """返回可选后端状态；不在服务启动时导入大型依赖。"""
    result: dict[str, Any] = {
        "built_in": ["linear_regression"],
        "optional": {"lightgbm": False, "qlib": False, "torch": False, "rd_agent": False},
    }
    for name, module in {
        "lightgbm": "lightgbm",
        "qlib": "qlib",
        "torch": "torch",
        "rd_agent": "rdagent",
    }.items():
        try:
            __import__(module)
        except Exception:
            continue
        result["optional"][name] = True
    result["models"] = ["linear_regression"]
    if result["optional"]["lightgbm"]:
        result["models"].append("lightgbm")
    if result["optional"]["torch"]:
        result["models"].extend(["lstm", "transformer", "transformer_seq"])
    if result["optional"]["lightgbm"] and result["optional"]["torch"]:
        result["models"].append("ensemble")
    return result


def learning_runtime_available() -> bool:
    """内置学习回测能否读取 Parquet 快照。"""
    try:
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        return False
    return True


def load_manifest_feature_rows(
    *,
    data_root: str | Path,
    manifest: dict[str, Any],
    symbols: list[str],
    start: str,
    end: str,
    feature_columns: list[str],
    label_column: str,
) -> list[dict[str, Any]]:
    """只加载 manifest 指定的特征文件，并校验大小和 SHA256。"""
    try:
        from pyarrow import parquet
    except ImportError as exc:
        raise LearningBacktestError("学习回测缺少 pyarrow 运行依赖") from exc

    datasets = manifest.get("datasets") or []
    dataset = next(
        (item for item in datasets if isinstance(item, dict) and item.get("name") in LEARNING_DATASET_NAMES),
        None,
    )
    if not isinstance(dataset, dict):
        raise LearningBacktestError(
            "执行快照缺少 ml_features_daily、factor_matrix_daily 或 lighter_microstructure_factors"
        )
    files = dataset.get("files")
    if not isinstance(files, list) or not files:
        raise LearningBacktestError("学习特征清单没有文件")
    wanted = {str(symbol).strip().upper() for symbol in symbols}
    start_dt = _parse_datetime(start)
    end_dt = _parse_datetime(end)
    root = Path(data_root).resolve()
    rows: list[dict[str, Any]] = []
    required = {"event_time", "symbol", *feature_columns}
    missing_labels = False
    for record in files:
        if not isinstance(record, dict):
            raise LearningBacktestError("学习特征文件记录格式无效")
        path = (root / str(record.get("path") or "")).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise LearningBacktestError("学习特征文件越出数据根目录") from exc
        if not path.is_file():
            raise LearningBacktestError(f"学习特征清单文件不存在: {record.get('path')}")
        if path.stat().st_size != int(record.get("size_bytes") or -1):
            raise LearningBacktestError("学习特征清单文件大小不一致")
        if _sha256_file(path) != str(record.get("sha256") or ""):
            raise LearningBacktestError("学习特征清单文件 SHA256 不一致")
        table = parquet.read_table(path)
        missing = required - set(table.column_names)
        if missing:
            raise LearningBacktestError(f"学习特征缺少字段: {', '.join(sorted(missing))}")
        for raw in _safe_table_rows(table):
            symbol = str(raw.get("symbol") or "").strip().upper()
            if symbol not in wanted:
                continue
            event_dt = _parse_datetime(raw.get("event_time"))
            if not start_dt <= event_dt <= end_dt:
                continue
            available_raw = raw.get("available_at") or raw.get("available_time") or raw.get("event_time")
            decision_raw = raw.get("tradable_from") or raw.get("decision_time") or raw.get("event_time")
            available_dt = _parse_datetime(available_raw)
            decision_dt = _parse_datetime(decision_raw)
            if available_dt > decision_dt:
                raise LearningBacktestError(
                    f"PIT 违规: {symbol} {event_dt.isoformat()} 的 available_at 晚于 decision_time"
                )
            row = {
                "event_time": event_dt.isoformat(),
                "symbol": symbol,
                "available_at": available_dt.isoformat(),
                "decision_time": decision_dt.isoformat(),
            }
            try:
                if label_column in raw and raw[label_column] is not None:
                    row[label_column] = float(raw[label_column])
                else:
                    row[label_column] = None
                    missing_labels = True
                for column in feature_columns:
                    value = float(raw[column])
                    if not math.isfinite(value):
                        raise ValueError(column)
                    row[column] = value
            except (KeyError, TypeError, ValueError) as exc:
                raise LearningBacktestError(f"学习特征存在非数值字段: {symbol}") from exc
            rows.append(row)
    rows.sort(key=lambda item: (item["event_time"], item["symbol"]))
    if not rows:
        raise LearningBacktestError("执行快照在请求区间内没有学习特征")
    if missing_labels:
        _derive_forward_labels(
            rows,
            data_root=Path(data_root).resolve(),
            manifest=manifest,
            label_column=label_column,
        )
    # 预测窗口末端天然没有未来价格；这些行必须丢弃，不能把 NaN 当作 0。
    rows[:] = [row for row in rows if row.get(label_column) is not None]
    if not rows:
        raise LearningBacktestError(
            f"学习特征缺少 {label_column}，且无法从 kline_daily 派生标签"
        )
    return rows


def run_learning_backtest(
    *,
    task_id: str,
    task: dict[str, Any],
    manifest: dict[str, Any],
    data_root: str | Path,
) -> dict[str, Any]:
    """执行严格时间序列的横截面预测回测。"""
    parameters = dict(task.get("parameters") or {})
    feature_columns = [str(item).strip() for item in parameters.get("feature_columns") or [] if str(item).strip()]
    if not feature_columns:
        raise LearningBacktestError("学习回测必须指定 parameters.feature_columns")
    label_column = str(parameters.get("label_column") or "label").strip()
    model_type = str(parameters.get("model_type") or "linear_regression").strip().lower()
    task_type = str(parameters.get("task_type") or "regression").strip().lower()
    if task_type not in {"binary", "ranking", "regression"}:
        raise LearningBacktestError(f"不支持的 task_type: {task_type}")
    if model_type not in {"linear_regression", "linear_logit", "lightgbm", "lstm", "transformer", "transformer_seq", "ensemble"}:
        raise LearningBacktestError(f"不支持的学习模型: {model_type}")
    capabilities = learning_runtime_capabilities()
    if model_type == "lightgbm" and not capabilities["optional"]["lightgbm"]:
        raise LearningBacktestError("lightgbm 未安装；请安装 PXYBACKTEST 的 ml extra")
    if model_type in {"lstm", "transformer", "transformer_seq"} and not capabilities["optional"]["torch"]:
        raise LearningBacktestError("torch 未安装；请安装 PXYBACKTEST 的 ml extra")
    if model_type == "ensemble" and not (
        capabilities["optional"]["torch"] and capabilities["optional"]["lightgbm"]
    ):
        raise LearningBacktestError("ensemble 需要同时安装 torch 和 lightgbm")

    universe = dict(task.get("universe") or {})
    period = dict(task.get("period") or {})
    rows = load_manifest_feature_rows(
        data_root=data_root,
        manifest=manifest,
        symbols=list(universe.get("symbols") or []),
        start=str(period.get("start") or ""),
        end=str(period.get("end") or ""),
        feature_columns=feature_columns,
        label_column=label_column,
    )
    folds = _generate_folds(rows, parameters)
    capital = float(dict(task.get("execution") or {}).get("capital") or 1_000_000)
    fee_rate = float(dict(task.get("execution") or {}).get("rate") or 0.0)
    threshold = float(parameters.get("prediction_threshold") or 0.0)
    top_k = int(parameters.get("top_k") or 0)
    if top_k < 0:
        raise LearningBacktestError("top_k 不能为负数")
    all_daily: list[dict[str, Any]] = []
    fold_meta: list[dict[str, Any]] = []
    for fold_index, (train_end, test_start, test_end) in enumerate(folds):
        train_cutoff = train_end - timedelta(days=int(parameters.get("purge_days") or 0))
        test_cutoff = test_start + timedelta(days=int(parameters.get("embargo_days") or 0))
        train = [r for r in rows if _date(r["event_time"]) <= train_cutoff and _date(r["available_at"]) <= train_cutoff]
        test = [r for r in rows if test_cutoff <= _date(r["event_time"]) <= test_end]
        if len(train) < 2 or not test:
            continue
        model = _fit_model(model_type, train, feature_columns, label_column, parameters)
        sequence_model = model_type in {"lstm", "transformer", "transformer_seq", "ensemble"}
        predictions = []
        for row in test:
            if sequence_model:
                history = _history_for_row(rows, row, int(parameters.get("seq_len") or 1))
                predictions.append((row, model([_features(item, feature_columns) for item in history])))
            else:
                predictions.append((row, model(_features(row, feature_columns))))
        by_day: dict[str, list[tuple[dict[str, Any], float]]] = {}
        for row, prediction in predictions:
            by_day.setdefault(_date(row["event_time"]).isoformat(), []).append((row, prediction))
        for day, values in sorted(by_day.items()):
            values.sort(key=lambda item: item[1], reverse=True)
            if task_type == "ranking":
                selected = values
            else:
                selected = [item for item in values if item[1] > threshold]
            if top_k:
                selected = selected[:top_k]
            if not selected:
                all_daily.append({"date": day, "return": 0.0, "n_selected": 0})
                continue
            gross = statistics.fmean(float(row[label_column]) for row, _ in selected)
            net = gross - fee_rate * 2.0
            all_daily.append({"date": day, "return": net, "n_selected": len(selected), "symbols": [row["symbol"] for row, _ in selected]})
        fold_meta.append({"index": fold_index, "train_end": train_end.isoformat(), "test_start": test_start.isoformat(), "test_end": test_end.isoformat(), "train_rows": len(train), "test_rows": len(test)})

    if not all_daily:
        raise LearningBacktestError("学习回测没有有效的训练/测试折")
    all_daily.sort(key=lambda item: item["date"])
    equity = capital
    peak = capital
    equity_curve: list[dict[str, Any]] = []
    returns: list[float] = []
    for point in all_daily:
        daily_return = float(point["return"])
        returns.append(daily_return)
        equity *= 1.0 + daily_return
        peak = max(peak, equity)
        equity_curve.append({"date": point["date"], "value": equity})
    drawdowns = [point["value"] / max(capital, max(p["value"] for p in equity_curve[:i + 1])) - 1.0 for i, point in enumerate(equity_curve)]
    volatility = statistics.pstdev(returns) if len(returns) > 1 else 0.0
    return {
        "schema_version": 2,
        "contract_version": "pxybacktest.task-result.v2",
        "task_id": task_id,
        "engine_type": str(task.get("engine_type") or "ml_factor"),
        "strategy": task.get("strategy") or {},
        "data_snapshot": dict((task.get("data") or {}).get("snapshot") or {}),
        "run": {"universe": universe, "period": period, "execution": task.get("execution") or {}, "parameters": parameters, "random_seed": task.get("random_seed")},
        "metrics": {"total_return": equity / capital - 1.0, "final_equity": equity, "net_profit": equity - capital, "max_drawdown": min(drawdowns, default=0.0), "sharpe": (statistics.fmean(returns) / volatility * math.sqrt(252) if volatility > 0 else 0.0), "hit_rate": sum(value > 0 for value in returns) / len(returns), "n_trades": sum(int(point.get("n_selected") or 0) for point in all_daily), "n_days": len(all_daily)},
        "curves": {"equity": equity_curve, "drawdown": [{"date": p["date"], "value": drawdowns[i]} for i, p in enumerate(all_daily)]},
        "deals": [],
        "diagnostics": {"adapter": f"pxybacktest.{model_type}.v1", "data_source_policy": "pxydata_snapshot_only", "snapshot_enforcement": "manifest_bound", "strictly_reproducible": model_type in {"linear_regression", "linear_logit"}, "feature_columns": feature_columns, "label_column": label_column, "model_type": model_type, "task_type": task_type, "seq_len": int(parameters.get("seq_len") or 1), "purge_days": int(parameters.get("purge_days") or 0), "embargo_days": int(parameters.get("embargo_days") or 0), "folds": fold_meta, "warnings": ["学习回测只生成研究信号，不提交真实订单。"]},
        "artifacts": [],
    }


def _fit_model(model_type: str, rows: list[dict[str, Any]], columns: list[str], label: str, parameters: dict[str, Any]):
    if model_type == "lightgbm":
        try:
            import lightgbm as lgb
        except ImportError as exc:
            raise LearningBacktestError("lightgbm 未安装") from exc
        model = lgb.LGBMRegressor(random_state=int(parameters.get("seed") or 42), n_estimators=int(parameters.get("n_estimators") or 100), verbosity=-1)
        model.fit([_features(row, columns) for row in rows], [float(row[label]) for row in rows])
        return lambda values: float(model.predict([values])[0])
    if model_type in {"lstm", "transformer", "transformer_seq"}:
        return _fit_torch_sequence(rows, columns, label, parameters, architecture="lstm" if model_type == "lstm" else "transformer")
    if model_type == "ensemble":
        return _fit_ensemble(rows, columns, label, parameters)
    return _fit_linear_regression(rows, columns, label, parameters)


def _fit_torch_sequence(
    rows: list[dict[str, Any]],
    columns: list[str],
    label: str,
    parameters: dict[str, Any],
    *,
    architecture: str = "transformer",
):
    """训练真正使用时间窗口的 LSTM 或 Transformer。"""
    try:
        import torch
        from torch import nn
    except ImportError as exc:
        raise LearningBacktestError("torch 未安装；请安装 PXYBACKTEST 的 ml extra") from exc
    seed = int(parameters.get("seed") or 42)
    torch.manual_seed(seed)
    seq_len = max(1, int(parameters.get("seq_len") or 1))
    sequences, target_values = _training_sequences(rows, columns, label, seq_len)
    if not sequences:
        raise LearningBacktestError("序列模型训练窗内没有足够的 seq_len 样本")
    values = torch.tensor(sequences, dtype=torch.float32)
    targets = torch.tensor(target_values, dtype=torch.float32).reshape(-1, 1)
    if architecture == "lstm":
        hidden = int(parameters.get("hidden_size") or 32)
        recurrent = nn.LSTM(len(columns), hidden, num_layers=int(parameters.get("num_layers") or 1), batch_first=True, dropout=float(parameters.get("dropout") or 0.0))
        head = nn.Linear(hidden, 1)
        def forward(batch):
            output, _ = recurrent(batch)
            return head(output[:, -1, :])
        trainable = [*recurrent.parameters(), *head.parameters()]
    else:
        d_model = int(parameters.get("d_model") or 16)
        heads = int(parameters.get("nhead") or 2)
        if d_model < 4 or d_model % heads:
            raise LearningBacktestError("Transformer 的 d_model 必须 >=4 且能被 nhead 整除")
        encoder_layer = nn.TransformerEncoderLayer(d_model=d_model, nhead=heads, dim_feedforward=max(d_model * 2, 16), dropout=float(parameters.get("dropout") or 0.0), batch_first=True)
        encoder = nn.TransformerEncoder(encoder_layer, num_layers=int(parameters.get("num_layers") or 1))
        projection = nn.Linear(len(columns), d_model)
        position = nn.Parameter(torch.zeros(1, seq_len, d_model))
        head = nn.Linear(d_model, 1)
        def forward(batch):
            encoded = encoder(projection(batch) + position[:, :batch.shape[1], :])
            return head(encoded[:, -1, :])
        trainable = [*encoder.parameters(), *projection.parameters(), position, *head.parameters()]
    optimizer = torch.optim.Adam(trainable, lr=float(parameters.get("learning_rate") or 0.005))
    loss_fn = nn.MSELoss()
    for parameter in trainable:
        parameter.requires_grad_(True)
    for _ in range(max(1, min(int(parameters.get("epochs") or 80), 1000))):
        optimizer.zero_grad()
        prediction = forward(values)
        loss = loss_fn(prediction, targets)
        loss.backward()
        optimizer.step()
    for parameter in trainable:
        parameter.requires_grad_(False)

    def predict(feature_values: list[float]) -> float:
        with torch.no_grad():
            tensor = torch.tensor([feature_values], dtype=torch.float32)
            return float(forward(tensor)[0, 0].item())

    return predict


def _fit_ensemble(rows, columns, label, parameters):
    lgb_predict = _fit_model("lightgbm", rows, columns, label, parameters)
    lstm_predict = _fit_torch_sequence(rows, columns, label, parameters, architecture="lstm")
    transformer_predict = _fit_torch_sequence(rows, columns, label, parameters, architecture="transformer")
    weights = parameters.get("ensemble_weights") or [1.0, 1.0, 1.0]
    try:
        numeric_weights = [float(item) for item in weights]
    except (TypeError, ValueError):
        numeric_weights = []
    if not isinstance(weights, list) or len(numeric_weights) != 3 or any(item <= 0 for item in numeric_weights):
        raise LearningBacktestError("ensemble_weights 必须包含 3 个正权重")
    total = sum(numeric_weights)
    w_lgb, w_lstm, w_transformer = [item / total for item in numeric_weights]
    def predict(sequence):
        flat = sequence[-1]
        return w_lgb * lgb_predict(flat) + w_lstm * lstm_predict(sequence) + w_transformer * transformer_predict(sequence)
    return predict


def _training_sequences(rows, columns, label, seq_len):
    grouped = {}
    for row in sorted(rows, key=lambda item: (str(item.get("symbol") or ""), item["event_time"])):
        grouped.setdefault(str(row.get("symbol") or ""), []).append(row)
    sequences, labels = [], []
    for group in grouped.values():
        for index, row in enumerate(group):
            start = max(0, index - seq_len + 1)
            window = group[start : index + 1]
            if len(window) < seq_len:
                window = [group[0]] * (seq_len - len(window)) + window
            sequences.append([_features(item, columns) for item in window])
            labels.append(float(row[label]))
    return sequences, labels


def _history_for_row(rows, row, seq_len):
    symbol = str(row.get("symbol") or "")
    prior = [item for item in rows if str(item.get("symbol") or "") == symbol and item["event_time"] <= row["event_time"]]
    prior.sort(key=lambda item: item["event_time"])
    window = prior[-max(1, seq_len):]
    if len(window) < seq_len:
        window = ([window[0]] if window else [row]) * (seq_len - len(window)) + window
    return window


def _fit_linear_regression(rows: list[dict[str, Any]], columns: list[str], label: str, parameters: dict[str, Any]):
    means = [statistics.fmean(_features(row, columns)[i] for row in rows) for i in range(len(columns))]
    scales = [statistics.pstdev(_features(row, columns)[i] for row in rows) or 1.0 for i in range(len(columns))]
    weights = [0.0] * (len(columns) + 1)
    lr = float(parameters.get("learning_rate") or 0.03)
    epochs = int(parameters.get("epochs") or 300)
    for _ in range(max(1, min(epochs, 5000))):
        gradients = [0.0] * len(weights)
        for row in rows:
            vector = [1.0] + [(value - means[i]) / scales[i] for i, value in enumerate(_features(row, columns))]
            error = sum(weight * value for weight, value in zip(weights, vector)) - float(row[label])
            for i, value in enumerate(vector):
                gradients[i] += error * value / len(rows)
        weights = [weight - lr * gradient for weight, gradient in zip(weights, gradients)]
    def predict(values: list[float]) -> float:
        vector = [1.0] + [(value - means[i]) / scales[i] for i, value in enumerate(values)]
        return sum(weight * value for weight, value in zip(weights, vector))
    return predict


def _features(row: dict[str, Any], columns: list[str]) -> list[float]:
    return [float(row[column]) for column in columns]


def _generate_folds(rows: list[dict[str, Any]], parameters: dict[str, Any]) -> list[tuple[date, date, date]]:
    days = sorted({_date(row["event_time"]) for row in rows})
    train_days = int(parameters.get("train_days") or max(2, len(days) // 2))
    test_days = int(parameters.get("test_days") or max(1, len(days) // 5))
    step_days = int(parameters.get("step_days") or test_days)
    if train_days < 2 or test_days < 1 or step_days < 1:
        raise LearningBacktestError("train_days/test_days/step_days 必须为正且训练窗至少 2 天")
    folds: list[tuple[date, date, date]] = []
    index = 0
    while index + train_days + test_days <= len(days):
        train_end = days[index + train_days - 1]
        test_start = days[index + train_days]
        test_end = days[index + train_days + test_days - 1]
        folds.append((train_end, test_start, test_end))
        index += step_days
    if not folds:
        raise LearningBacktestError("数据区间不足以生成学习回测折")
    return folds


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise LearningBacktestError(f"无效学习数据时间: {value}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        parsed = parsed.replace(tzinfo=datetime.now().astimezone().tzinfo)
    return parsed


def _date(value: Any) -> date:
    return _parse_datetime(value).date()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_table_rows(table: Any) -> list[dict[str, Any]]:
    """避免本机未安装 tzdata 时 PyArrow 解析带时区 timestamp 失败。"""
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - caller already checks pyarrow
        raise LearningBacktestError("学习回测缺少 pyarrow 运行依赖") from exc
    columns: dict[str, list[Any]] = {}
    for name in table.column_names:
        column = table[name]
        if pa.types.is_timestamp(column.type):
            unit_scale = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}[column.type.unit]
            integers = column.cast(pa.int64()).to_pylist()
            columns[name] = [
                datetime.fromtimestamp(int(value) / unit_scale, tz=timezone.utc).isoformat()
                if value is not None
                else None
                for value in integers
            ]
        else:
            columns[name] = column.to_pylist()
    length = max((len(values) for values in columns.values()), default=0)
    return [{name: values[index] for name, values in columns.items()} for index in range(length)]


def _derive_forward_labels(
    rows: list[dict[str, Any]],
    *,
    data_root: Path,
    manifest: dict[str, Any],
    label_column: str,
) -> None:
    match = re.fullmatch(r"forward_return_(\d+)d", label_column)
    if match is None:
        return
    horizon = int(match.group(1))
    if horizon < 1:
        return
    dataset = next(
        (item for item in manifest.get("datasets") or [] if isinstance(item, dict) and item.get("name") == "kline_daily"),
        None,
    )
    if not isinstance(dataset, dict) or not isinstance(dataset.get("files"), list):
        return
    prices: dict[str, dict[date, float]] = {}
    for record in dataset["files"]:
        if not isinstance(record, dict):
            continue
        path = (data_root / str(record.get("path") or "")).resolve()
        try:
            path.relative_to(data_root)
        except ValueError as exc:
            raise LearningBacktestError("kline_daily 文件越出数据根目录") from exc
        if not path.is_file():
            raise LearningBacktestError("标签派生所需 kline_daily 文件不存在")
        if path.stat().st_size != int(record.get("size_bytes") or -1) or _sha256_file(path) != str(record.get("sha256") or ""):
            raise LearningBacktestError("标签派生所需 kline_daily 文件校验失败")
        try:
            from pyarrow import parquet
            table = parquet.read_table(path)
        except ImportError as exc:
            raise LearningBacktestError("学习回测缺少 pyarrow 运行依赖") from exc
        required = {"symbol", "date", "close"}
        if required - set(table.column_names):
            raise LearningBacktestError("kline_daily 缺少 symbol/date/close 字段")
        for raw in _safe_table_rows(table):
            symbol = str(raw.get("symbol") or "").strip().upper()
            try:
                day = date.fromisoformat(str(raw.get("date") or "")[:10])
                close = float(raw.get("close"))
            except (TypeError, ValueError):
                continue
            if symbol and close > 0 and math.isfinite(close):
                prices.setdefault(symbol, {})[day] = close
    for row in rows:
        if row.get(label_column) is not None:
            continue
        symbol_prices = prices.get(str(row["symbol"]).upper(), {})
        days = sorted(symbol_prices)
        try:
            current_index = days.index(_date(row["event_time"]))
            future_day = days[current_index + horizon]
            row[label_column] = symbol_prices[future_day] / symbol_prices[days[current_index]] - 1.0
        except (ValueError, IndexError, KeyError):
            continue
