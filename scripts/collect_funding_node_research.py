"""只读导出运行节点的公共费率，禁止读取账户、订单与密钥内容。"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess


REMOTE = r'''
import asyncio, json, os, subprocess, sys
from datetime import datetime, timezone
pid = subprocess.check_output(
    ['systemctl', 'show', '-p', 'MainPID', '--value', CONFIG['service']], text=True).strip()
if not pid.isdigit() or int(pid) <= 0:
    raise RuntimeError('运行服务没有有效进程')
for entry in open('/proc/' + pid + '/environ', 'rb').read().split(b'\0'):
    if b'=' in entry:
        key, value = entry.split(b'=', 1)
        os.environ[key.decode()] = value.decode()
os.chdir(CONFIG['backend'])
sys.path.insert(0, os.getcwd())
from services.funding_node_bridge import load_environment
load_environment()
from sqlalchemy import select, func, text
from core.database import history_session, history_engine
from models.funding_history import FundingRateObservation as Observation
from models.funding_history import FundingSettledRate as Settlement

async def collect():
    result = {'contract_version': 'pxybacktest.funding-node-input.v1',
              'collected_at': datetime.now(timezone.utc).isoformat(),
              'start_ms': CONFIG['start_ms'], 'end_ms': CONFIG['end_ms'],
              'symbols': CONFIG['symbols'], 'coverage': {}}
    async with history_session() as db:
        if db.bind.dialect.name == 'postgresql':
            await db.execute(text('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY'))
        for key, model, stamp, fields in (
            ('predictions', Observation, Observation.observed_at_ms,
             ('platform', 'symbol', 'observed_at_ms', 'next_funding_ts',
              'rate', 'interval_hours', 'mark_price')),
            ('settlements', Settlement, Settlement.funding_time,
             ('platform', 'symbol', 'funding_time', 'rate', 'interval_hours',
              'interval_quality', 'source', 'confirmed_at')),
        ):
            coverage = (await db.execute(select(
                model.platform, func.count(), func.min(stamp), func.max(stamp)
            ).group_by(model.platform))).all()
            result['coverage'][key] = [dict(zip(
                ('venue', 'rows', 'first_ms', 'last_ms'), row)) for row in coverage]
            columns = [getattr(model, field) for field in fields]
            query = select(*columns).where(
                model.symbol.in_(CONFIG['symbols']),
                stamp >= CONFIG['start_ms'], stamp < CONFIG['end_ms'],
            ).order_by(stamp, model.platform, model.symbol)
            rows = (await db.execute(query)).all()
            result[key] = [dict(zip(fields, row)) for row in rows]
    await history_engine.dispose()
    return result

print(json.dumps(asyncio.run(collect()), ensure_ascii=False, default=str, allow_nan=False))
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ssh-target", required=True)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--service", default="vnpy-backend")
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--start-ms", type=int, required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[A-Za-z0-9_.@:-]+", args.ssh_target):
        parser.error("SSH 目标格式无效")
    for value in (args.backend, args.python):
        if not re.fullmatch(r"/[A-Za-z0-9_./-]+", value):
            parser.error("远端路径必须是无空格绝对路径")
    if args.start_ms < 0 or args.end_ms <= args.start_ms:
        parser.error("研究时间区间无效")
    if args.output.exists():
        raise FileExistsError("研究输入已经冻结，禁止覆盖")
    config = {"backend": args.backend, "service": args.service,
              "symbols": args.symbols.split(","), "start_ms": args.start_ms,
              "end_ms": args.end_ms}
    command = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=12",
               "-o", "StrictHostKeyChecking=yes", args.ssh_target, args.python, "-"]
    completed = subprocess.run(command, input="CONFIG = " + repr(config) + "\n" + REMOTE,
                               text=True, encoding="utf-8", capture_output=True,
                               timeout=180, check=False)
    if completed.returncode:
        raise RuntimeError(f"公共数据导出失败，退出码 {completed.returncode}")
    data = json.loads(completed.stdout)
    if data.get("contract_version") != "pxybacktest.funding-node-input.v1":
        raise ValueError("节点导出契约无效")
    raw = json.dumps(data, ensure_ascii=False, allow_nan=False).encode("utf-8")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("xb") as output:
        output.write(raw)
    print(json.dumps({"path": str(args.output.resolve()), "sha256": hashlib.sha256(raw).hexdigest(),
                      "predictions": len(data["predictions"]),
                      "settlements": len(data["settlements"]),
                      "coverage": data["coverage"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
