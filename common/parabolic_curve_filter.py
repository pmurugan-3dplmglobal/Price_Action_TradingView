"""
common/parabolic_curve_filter.py
Parabolic Multi-Swing Curve & Cascade Structure Detection Filter.

Detects macro parabolic exhaustion structures across multiple swing cycles (e.g. 3 or 4 waves):
- Bullish Reversal: Exhaustion selloff cascade with downward-arched domes (∩),
  progressively lower highs/lows, and terminal base absorption (<2% low difference).
- Bearish Reversal: Exhaustion rally cascade with upward-curved cups (∪),
  progressively higher lows/highs, and terminal ceiling exhaustion (<2% high difference).
"""

from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd


def _get_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    """Helper to safely retrieve DataFrame column regardless of case."""
    for col in [col_name.lower(), col_name.upper(), col_name.capitalize()]:
        if col in df.columns:
            return df[col]
    raise KeyError(f"Column '{col_name}' not found in DataFrame. Available columns: {list(df.columns)}")


def is_parabolic_arch_enhanced(
    df_slice: pd.DataFrame,
    side: str = "BULL",
    min_r2: float = 0.55,
    min_candles: int = 6
) -> bool:
    """
    Enhanced parabolic curve detector:
    1. Uses (High + Close)/2 for Bullish dome ∩ or (Low + Close)/2 for Bearish cup ∪.
    2. Fits 2nd-degree polynomial: y = ax^2 + bx + c.
    3. Curvature check:
       - Bullish: a < 0 (concave down dome ∩).
       - Bearish: a > 0 (concave up cup ∪).
    4. Vertex placement: -b / (2a) situated inside middle 15% - 85% range.
    5. Goodness of Fit: R^2 >= min_r2.
    """
    if len(df_slice) < min_candles:
        return False

    try:
        closes = _get_series(df_slice, 'close').values.astype(float)
        side_upper = str(side).upper()

        if side_upper == "BEAR":
            lows = _get_series(df_slice, 'low').values.astype(float)
            y = (lows + closes) / 2.0
        else:
            highs = _get_series(df_slice, 'high').values.astype(float)
            y = (highs + closes) / 2.0

        n = len(y)
        x = np.arange(n)

        # Fit 2nd-degree polynomial
        poly_coeffs = np.polyfit(x, y, deg=2)
        a, b, c = poly_coeffs

        # 1. Curvature orientation
        if side_upper == "BEAR":
            if a <= 0:  # Must be concave UP (∪) for rally exhaustion cup
                return False
        else:
            if a >= 0:  # Must be concave DOWN (∩) for selloff exhaustion dome
                return False

        # 2. Apex (vertex) inside middle 15% - 85% range
        vertex_x = -b / (2.0 * a)
        if not (0.15 * (n - 1) <= vertex_x <= 0.85 * (n - 1)):
            return False

        # 3. Goodness of Fit (R^2)
        y_pred = np.polyval(poly_coeffs, x)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        ss_res = np.sum((y - y_pred) ** 2)
        r2 = 1.0 - (ss_res / (ss_tot + 1e-8))

        return bool(r2 >= min_r2)

    except Exception:
        return False


def extract_swing_pivots(
    df: pd.DataFrame,
    side: str = "BULL",
    min_candles_per_leg: int = 3,
    include_endpoints: bool = True
) -> List[int]:
    """
    Extracts local swing extrema indices:
    - Bullish: Swing Lows (L1, L2, L3...)
    - Bearish: Swing Highs (H1, H2, H3...)
    Filters adjacent duplicate extrema by retaining the most extreme bar.
    """
    w = max(int(min_candles_per_leg), 1)
    if len(df) < (w * 2 + 1):
        return []

    try:
        side_upper = str(side).upper()
        col = 'low' if side_upper == "BULL" else 'high'
        series = _get_series(df, col).values.astype(float)
        n = len(series)

        raw_pivots = []

        # Optional: check if start of series is an initial pivot
        if include_endpoints and n > w:
            if side_upper == "BULL" and series[0] == np.min(series[:w + 1]):
                raw_pivots.append(0)
            elif side_upper == "BEAR" and series[0] == np.max(series[:w + 1]):
                raw_pivots.append(0)

        # Internal extrema
        for i in range(1, n - 1):
            left_w = min(i, w)
            right_w = min(n - 1 - i, w)
            if left_w < 1 or right_w < 1:
                continue
            window = series[i - left_w : i + right_w + 1]
            target = series[i]
            if side_upper == "BULL" and target == np.min(window):
                raw_pivots.append(i)
            elif side_upper == "BEAR" and target == np.max(window):
                raw_pivots.append(i)

        # Optional: check if end of series is a terminal pivot
        if include_endpoints and n > w:
            if side_upper == "BULL" and series[-1] == np.min(series[-w - 1:]):
                raw_pivots.append(n - 1)
            elif side_upper == "BEAR" and series[-1] == np.max(series[-w - 1:]):
                raw_pivots.append(n - 1)

        # Filter duplicate adjacent pivots
        filtered_pivots: List[int] = []
        for p in raw_pivots:
            if not filtered_pivots:
                filtered_pivots.append(p)
            else:
                prev = filtered_pivots[-1]
                if p - prev <= w:
                    if side_upper == "BULL" and series[p] < series[prev]:
                        filtered_pivots[-1] = p
                    elif side_upper == "BEAR" and series[p] > series[prev]:
                        filtered_pivots[-1] = p
                else:
                    filtered_pivots.append(p)

        return filtered_pivots

    except Exception:
        return []


def validate_parabolic_cascade_structure(
    df: pd.DataFrame,
    swing_indices: List[int],
    side: str = "BULL",
    min_cascading_waves: int = 3,
    min_r2: float = 0.55,
    tolerance_absorption: float = 0.02
) -> Dict[str, Any]:
    """
    Validates macro market structure across consecutive swing waves:
    - Wave-by-wave parabolic curvature check.
    - Monotonicity check (Lower Lows for Bull; Higher Highs for Bear).
    - Macro Cascade: Progressively Lower Highs (Bull) or Higher Lows (Bear).
    - Terminal Base / Ceiling Absorption detection (Point 4).
    """
    if len(swing_indices) < 2:
        return {
            "valid": False,
            "reason": "Insufficient swing points",
            "valid_arch_count": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "details": []
        }

    side_upper = str(side).upper()
    valid_arches = 0
    wave_details = []

    try:
        highs = _get_series(df, 'high').values.astype(float)
        lows = _get_series(df, 'low').values.astype(float)

        for i in range(len(swing_indices) - 1):
            start_idx = swing_indices[i]
            end_idx = swing_indices[i + 1]
            df_wave = df.iloc[start_idx : end_idx + 1]

            is_arch = is_parabolic_arch_enhanced(df_wave, side=side_upper, min_r2=min_r2)

            if side_upper == "BULL":
                # Lower Low check between swing low endpoints
                is_monotone = lows[end_idx] < lows[start_idx]
                wave_peak_or_trough = float(np.max(highs[start_idx : end_idx + 1]))
                base_extrema = float(lows[end_idx])
            else:
                # Higher High check between swing high endpoints
                is_monotone = highs[end_idx] > highs[start_idx]
                wave_peak_or_trough = float(np.min(lows[start_idx : end_idx + 1]))
                base_extrema = float(highs[end_idx])

            wave_details.append({
                "wave_index": i + 1,
                "start_idx": start_idx,
                "end_idx": end_idx,
                "is_arch": is_arch,
                "is_monotone": is_monotone,
                "peak_or_trough": wave_peak_or_trough,
                "base_extrema": base_extrema
            })

            if is_arch and is_monotone:
                valid_arches += 1

        if not wave_details:
            return {"valid": False, "reason": "No waves formed", "details": []}

        # Macro trend cascade check
        extrema_list = [w["peak_or_trough"] for w in wave_details]
        if len(extrema_list) >= 2:
            if side_upper == "BULL":
                # Progressively lower highs (Peak1 > Peak2 > Peak3...)
                cascade_progression = all(extrema_list[j] > extrema_list[j+1] for j in range(len(extrema_list)-1))
            else:
                # Progressively higher lows (Trough1 < Trough2 < Trough3...)
                cascade_progression = all(extrema_list[j] < extrema_list[j+1] for j in range(len(extrema_list)-1))
        else:
            cascade_progression = False

        # Detect terminal base absorption at Point 4
        has_terminal_base = False
        if len(wave_details) >= 2:
            last_wave = wave_details[-1]
            prev_wave = wave_details[-2]
            denom = max(last_wave["base_extrema"], 1e-8)
            price_diff = abs(last_wave["base_extrema"] - prev_wave["base_extrema"]) / denom
            if price_diff <= tolerance_absorption:
                has_terminal_base = True

        # Terminal base pivot index and recency
        terminal_pivot_idx = swing_indices[-1] if swing_indices else -1
        bars_since_terminal_base = len(df) - 1 - terminal_pivot_idx if terminal_pivot_idx >= 0 else 999

        # Total effective qualifying waves (terminal absorption wave qualifies if arches preceded it)
        effective_waves = valid_arches + (1 if (has_terminal_base and not wave_details[-1]["is_arch"]) else 0)

        is_full_pattern_valid = bool(
            effective_waves >= min_cascading_waves and
            cascade_progression and
            (wave_details[-1]["is_arch"] or has_terminal_base)
        )

        return {
            "valid": is_full_pattern_valid,
            "valid_arch_count": valid_arches,
            "effective_waves": effective_waves,
            "cascade_progression": cascade_progression,
            "has_terminal_base": has_terminal_base,
            "terminal_pivot_idx": terminal_pivot_idx,
            "bars_since_terminal_base": bars_since_terminal_base,
            "details": wave_details
        }

    except Exception as e:
        return {
            "valid": False,
            "reason": f"Calculation error: {str(e)}",
            "valid_arch_count": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "terminal_pivot_idx": -1,
            "bars_since_terminal_base": 999,
            "details": []
        }


def detect_parabolic_multi_swings(
    df: pd.DataFrame,
    side: str = "BULL",
    min_swings: int = 3,
    min_candles_per_leg: int = 3,
    min_r2: float = 0.55,
    tolerance_absorption: float = 0.02,
    max_bars_since_base: Optional[int] = 18
) -> Dict[str, Any]:
    """
    Top-level Phase 0 filter wrapper.
    1. Extracts swing pivots.
    2. Validates macro cascade and parabolic curvature.
    3. Enforces that pattern is recently formed (not stale).
    4. Returns full structured metrics dictionary.
    """
    if df is None or len(df) < (min_swings * min_candles_per_leg * 2):
        return {
            "matched": False,
            "valid": False,
            "reason": "Insufficient candle data",
            "pivots": [],
            "valid_arch_count": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "terminal_pivot_idx": -1,
            "bars_since_terminal_base": 999,
            "is_recent": False,
            "details": []
        }

    pivots = extract_swing_pivots(df, side=side, min_candles_per_leg=min_candles_per_leg)
    structure = validate_parabolic_cascade_structure(
        df,
        pivots,
        side=side,
        min_cascading_waves=min_swings,
        min_r2=min_r2,
        tolerance_absorption=tolerance_absorption
    )

    bars_since = structure.get("bars_since_terminal_base", 0)
    is_recent = True
    if max_bars_since_base is not None and max_bars_since_base > 0:
        if bars_since > max_bars_since_base:
            is_recent = False

    is_matched = bool(structure.get("valid", False) and is_recent)

    structure["matched"] = is_matched
    structure["is_recent"] = is_recent
    structure["pivots"] = pivots
    return structure


def is_anchor_after_terminal_base(
    df_anchor: pd.DataFrame,
    anchor_time: Any,
    swing_structure: Dict[str, Any]
) -> bool:
    """
    Verifies that Anchor Candle (A) forms at or AFTER the 4th swing / terminal base.
    Prevents triggering on an anchor candle that formed prior to the swing exhaustion.
    """
    if not swing_structure or not swing_structure.get("matched", False):
        return False

    term_idx = swing_structure.get("terminal_pivot_idx", -1)
    if term_idx < 0 or term_idx >= len(df_anchor):
        return True  # If index not determinable, don't hard block

    try:
        # Check if date/time column is available
        if "date" in df_anchor.columns:
            anchor_time_str = str(anchor_time).split("+")[0].strip()
            df_dates = df_anchor["date"].astype(str).str.split("+").str[0].str.strip().values

            # Find anchor index in df_anchor
            anchor_indices = np.where(df_dates == anchor_time_str)[0]
            if len(anchor_indices) > 0:
                a_idx = anchor_indices[-1]
                # Anchor must form at or after the terminal pivot (allow 1 bar margin for formation)
                return bool(a_idx >= (term_idx - 1))

        return True
    except Exception:
        return True

