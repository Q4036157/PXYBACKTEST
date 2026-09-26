# XAUUSDm three-day fixed-vector baseline

Status on 2026-09-26: data L0 is prepared; EA L1-L4 parity and full-strategy performance are not yet accepted.

## Immutable input

- Symbol: `XAUUSDm`; UTC range: 2026-08-18 through 2026-08-20.
- PXYDATA snapshot: `btsnap_v1_c15d62f7db38cbac24bc6de45212eea7`.
- Manifest SHA256: `19f01c650b1cda8152547ea2fa230956170863b770c1809937dd383721bbf6e8`.
- Current-code quality report: `quality_13d20729c7374d0e9c061b4e8ea44f45`, `mt5_ticks=PASS`.
- Published files: 3 immutable Parquet partitions, 900,368 rows.
- Rows are MT5 Bid/Ask `quote` events, not exchange trade prints. They cannot produce an authentic traded-volume footprint.

## Loader-only measurement

On this Windows workstation, the verified Python reader processed all 900,368 rows in 6.312 seconds, or 142,638 rows/s, with a sampled peak RSS of 179.8 MiB. This includes SHA256 verification and Arrow-to-Python row conversion. It excludes strategy execution, order matching, account updates, rendering, API/queue overhead and MT5 startup. It must not be compared as a complete backtest against MT5's tester duration.

Reproduce with `python -m scripts.benchmark_mt5_tick_loader` and the data root, snapshot path, manifest hash, symbol and exact `start-ms`/`end-ms` arguments. The command prints JSON and creates no benchmark artifacts.

## Parity gate

The older `123骑士` Oracle recorded in `docs/mt5-parity-contract.md` refers to MQ5 SHA256 `799D17...`, EX5 SHA256 `63FF6A...` and report SHA256 `C2A652...`. None of those matching EA files was found among the current D:/x1/x2 MQL5 sources/binaries; the surviving `123骑士-200/123骑士.mq5` has a different SHA256. Do not silently substitute it or mark the old vector accepted.

Before a full benchmark or C++ rollout, pin the exact EA/EX5, inputs, terminal build, account specification and MT5 report; execute the same strategy against the bound PXYDATA snapshot; then pass ordered-deal, account-path and visual-event checks. Only profile the full matched run after these identities and three dimensions pass.
