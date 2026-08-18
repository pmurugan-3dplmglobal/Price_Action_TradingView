# AI_CONTEXT_INDEX.md — Read-This-First Guide for AI Sessions

Purpose: save tokens. Each code-change request should read **Tier 1** only.
Reference folders (Tier 2 / Tier 3) are optional and should be opened **only
when a task explicitly needs them**.

## App Context (distilled from MASTER_DOCUMENTATION.yaml / Notes.txt / ISSUE_MANAGEMENT.yaml)

- **What**: Price Action Trading system on Zerodha Kite Connect — "Price Action Unified
  Strategy System", Prod Code v02. Architecture = Centralized Shared Core (`common/`) +
  Dual Workspaces (`Trade_Option`, `Trade_Stock`).
- **Strategy**: Unified ABC Bullish & Bearish Reversal Pattern Engine (Negation Theory).
  5 bullish + 5 bearish anchor patterns: Engulfing (ABCD), LL/HH Sweep, Two Higher/Lower
  Highs, Baby Candle/Shooting Star (Hammer), Harami (Inside Bar). Pattern priority 1-5,
  first valid pattern with R:R >= 1.88 triggers the setup. Bull shorthands:
  BULL_ENG / BULL_LL / BULL_2HH / BULL_HAM / BULL_HAR / BULL_BASE; bear mirrors BEAR_*.
- **Coverage**: Index Options (NIFTY token 256265 lot 65, BANKNIFTY token 260105 lot 30,
  SENSEX token 265 lot 20 via **BFO** exchange — NIFTY/BANKNIFTY via NFO), Stock Options
  (50 Nifty 50 constituents), Stock Spot Bull & Bear reversals, plus a Stock EMA Engine
  (13 EMA / 44 EMA crossover, `common/ema_engine.py`, default SL=44EMA, T1=1.5RR, T2=2.5RR,
  T3=3.5RR; on 6060 scans stocks→ATM option contracts, on 6061 scans stock spot symbols).
- **Engines / config defaults**:
  - Index Options Engine (6060, BULL): entry TF `3minute`, anchor TF `15minute`, risk 1.0%.
  - Stock Options Engine (6060, BULL): entry `15minute`, anchor `30minute`, risk 10.0%.
  - Stock Bull Scanner (6061): `daily` profile, entry/anchor `day`, lookback 2000 days.
  - Stock Bear Scanner (6061): `bear_trade` profile, mirror of bull.
  - Stock EMA Engine (6060+6061): default TF `1d`, fast=13, slow=44, scan_interval 300s,
    target universe selector (ALL / NIFTY50 / NIFTY_NEXT_100 / NIFTY_MIDCAP_100 /
    NIFTY_SMALLCAP_250 / INDEX_OPTIONS). "ALL" aggregates 239 symbols.
- **Config precedence**: 1) Manual UI edits (sl_target_overrides.json /
  active_positions_db.json) → 2) Persistent config/DB (program_config.json, trades DB) →
  3) Adaptive defaults.
- **Timeframes**: native Kite minute/3m/5m/10m/15m/30m/60m/day; resampled 75min (from
  15m, offset='15min'), 4hr (from 60m, '240min'), week (W-FRI). Adaptive lookbacks:
  intraday 30-60d, hourly 180-365d, daily/weekly 2000d. **Trade_Stock scanners default
  strictly to 'day'** unless overridden.
- **Risk management**: SL buffer tiers by price (cheap <50: max(0.15, 2%); 50-200:
  max(0.50, 1.5%); 200-500: max(1.00, 1.0%); index spot >=500: max(2.00, 0.5%)). Monitor
  every 5s: SL hit→full exit; T1→SL to breakeven (or full exit if T2/T3 N/A); T2→SL to T1;
  T3→full exit. Targets derived strictly from anchor TF candles. Anchor invalidation =
  any later anchor-TF candle closes below A.low (bull) / above A.high (bear), or T1 touched.
  Strict **closing-basis** evaluation (scanner trims the still-forming last candle).
- **Key invariants (don't break)**: benchmark/anchor_floor/direction persisted end-to-end;
  UI must NOT re-derive A.high/A.low from candle_a_time (engine's persisted benchmark is
  source of truth); entry_time = true execution time (never candle time); canonical paths
  from `common/paths.py` only (PATH_SPLIT family — NEVER cwd-relative paths); trade_db
  rejects duplicate ACTIVE contracts & expired contracts; new features must be isolated
  with zero regression (Non-Negotiable Architecture Thumb Rule).
- **Known failure families (see ISSUE_MANAGEMENT.yaml)**: PATH_SPLIT (path split-brain —
  fixed by `common/paths.py`), DATA_INTEGRITY (duplicate/expired/ghost DB rows, entry_time
  corruption), UI_SYNTAX (JS brace errors in inline HTML templates), SL override loss
  (symbol-vs-contract key matching, get_override_paths()). Always record fixes in
  ISSUE_MANAGEMENT.yaml with a `family:` marker.
- **Verification**: `scratch/run_full_regression_test.py` = 10 tests (imports, Kite auth,
  scanners, display serializers, dashboard API, trade_db, path consistency, DB invariants,
  engine path alignment, entry_time invariants). Syntax: `ast.parse(...)` for .py,
  `node --check` for inline JS blocks.
- **Deploy**: Oracle Cloud Always Free (4 ARM cores/24GB RAM/Ubuntu 24.04), dashboards on
  6060/6061.

## Tier 1 — ALWAYS READ for code changes (core code)

| Path | What it is | When to read |
|---|---|---|
| `common/` | The "brain". `trading_core.py` is a re-export hub (do NOT alter core logic — dead-code removal only). Living logic: `timeframe_utils.py`, `registries.py`, `session.py`, `targets.py`, `patterns_bull.py`, `patterns_bear.py`, `position_monitor.py`, `display_writer.py`, `resolve.py`, `ema_engine.py`, `paths.py` (canonical paths), `dashboard_sl_overrides.py`, `trade_db.py`, `daily_trade_journal.py`, `equity_universe.py`, `spot_enricher.py` | Any strategy/engine/position/logic change |
| `Trade_Option/` | Options Dashboard engine (port 6060). `app_option_Trade.py`, `stock_options_trade_engine.py`, `index_options_trade_engine.py`, UI in `templates/index.html` | Options dashboard / option engines / UI on port 6060 |
| `Trade_Stock/` | Stock Trade Dashboard + scanners (port 6061). `app_Sock_Trade.py`, `stock_reversal_scanner.py` (single real impl, PROFILE-driven), wrappers `stock_bullish_reversal_scanner.py` / `stock_bearish_reversal_scanner.py`, UI in `templates/index.html` | Stock dashboard / scanners / UI on port 6061 |
| `AGENTS.md` | Technical code map (also loaded automatically as session instructions) | Always own the content; keep in sync when arch/ports change |
| `ISSUE_MANAGEMENT.yaml` | Bug/feature tracker — RECORD every fix here | After each fix/feature |
| `MASTER_DOCUMENTATION.yaml` | Master system doc — keep accurate | When behavior changes |
| `paths.py` | Canonical file paths | Any file-path reference — never hardcode paths |
| `Kite_Access_Token_gen.py` | Root-level token generator (single canonical copy) | Only when touching token flow |

## Tier 2 — REFERENCE ONLY (skip by default; high token cost)

Do **NOT** open these on routine code-change requests. Read only when the task
name/content explicitly mentions them.

| Path | What it is | When to use |
|---|---|---|
| `scratch/` | Diagnostic/regression one-off scripts (33 scripts). `run_full_regression_test.py` (10-test regression suite) is the one go-to. Others are throwaway analysis/debug scripts | Explicitly asked about a past debug/analysis; or to run regression |
| `backtest/` | Backtest scripts + `master_backtest_results.json` | Backtest work / results review only |
| `archive/` (incl. `legacy_backup/`) | Decommissioned code — do not read, do not resurrect | Never (moved out of production) |
| `Reference/` | Reference screenshots (JPEGs) | Only when user points at a specific image |
| `__pycache__/` | Compiled bytecode — ignore completely | Never |

## Tier 3 — RUNTIME/DEPLOY (rare)

| Path | What it is | When to use |
|---|---|---|
| `input/` | Secrets/config: `kite_access_token.txt` (daily token), `program_config.json` | Token refresh or config change only |
| `oracle/` | Oracle Cloud deployment scripts (systemd/cron/token check) + README | Cloud deployment tasks only |
| `output/monitor/` | Runtime state: scan_display*.json, trades_db.json, active_positions, trade_journal | Troubleshooting live trades / verifying scan output |

## Decision rules for the AI

1. Code-change request → read Tier 1 folders that touch the feature. Stop there.
2. Never read `__pycache__`, `archive/`, or `Reference/` unless explicitly asked.
3. Use `scratch/run_full_regression_test.py` to verify broad changes; use the
   syntax check from `AGENTS.md` for quick checks.
4. If a task mentions "backtest", "cloud", "token", or a specific debug script,
   read the corresponding Tier 2/3 folder. Otherwise don't.
5. Record every change in `ISSUE_MANAGEMENT.yaml`. Keep this index + AGENTS.md accurate.