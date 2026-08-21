"""
common/swing_detection.py
Parabolic Multi-Swing Curve Fitting & Market Structure Detection Module (Reference Implementation).
Detects cascading parabolic arches (convex/concave polynomial curves) and terminal base absorption
across multi-swing exhaustion structures prior to Anchor/BCD breakouts.
"""
from typing import List, Dict, Tuple, Optional, Any, Union
import numpy as np
import pandas as pd


def _get_series(df: pd.DataFrame, col_name: str) -> pd.Series:
    """Helper to safely retrieve DataFrame column regardless of case."""
    for col in [col_name.lower(), col_name.upper(), col_name.capitalize()]:
        if col in df.columns:
            return df[col]
    raise KeyError(f"Column '{col_name}' not found in DataFrame. Available columns: {list(df.columns)}")


def clean_liquid_candles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strips zero-volume quotation flatlines before swing analysis (Options Protection).
    Retains only bars after real liquidity / traded spread begins.
    """
    if df is None or df.empty:
        return df
    if 'volume' in [c.lower() for c in df.columns]:
        try:
            vol_s = _get_series(df, 'volume').values
            hi_s = _get_series(df, 'high').values
            lo_s = _get_series(df, 'low').values
            is_liquid = (vol_s > 0) | (hi_s != lo_s)
            if np.any(is_liquid):
                first_valid_pos = int(np.argmax(is_liquid))
                if first_valid_pos > 0:
                    return df.iloc[first_valid_pos:].reset_index(drop=True)
        except Exception:
            pass
    return df


def is_parabolic_arch_enhanced(
    df_slice: pd.DataFrame, 
    side: str = "BULL",
    min_r2: float = 0.55,
    allow_skew: bool = True,
    min_candles: int = 6
) -> bool:
    """
    Enhanced parabolic arch/dome detector:
    1. Uses Highs/Closes (BULL dome ∩) or Lows/Closes (BEAR cup ∪) for curvature definition.
    2. Fits 2nd-degree polynomial: y = ax^2 + bx + c.
    3. For BULL (bottom exhaustion dome ∩): a < 0.
       For BEAR (top exhaustion cup ∪): a > 0.
    4. Apex/vertex in middle 15% - 85% range.
    5. Goodness of Fit (R^2) >= min_r2.
    """
    if df_slice is None or len(df_slice) < min_candles:
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
        
        # 1. Curvature direction check
        if side_upper == "BEAR":
            # Must be concave up cup (a > 0)
            if a <= 0:
                return False
        else:
            # Must be concave down dome (a < 0)
            if a >= 0:
                return False
            
        # 2. Apex (vertex) inside middle 15% - 85% range
        if abs(a) < 1e-12:
            return False
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
    include_endpoints: bool = True,
    min_atr_factor: float = 0.6
) -> List[int]:
    """
    Extracts local extrema indices for swing waves, ignoring dead zero-volume flatline candles.
    For BULL: extracts swing low indices L1, L2, L3, ...
    For BEAR: extracts swing high indices H1, H2, H3, ...
    Filters adjacent duplicate extrema by retaining the most extreme bar.
    Filters micro-ripples using ATR displacement (>= min_atr_factor * ATR).
    """
    w = max(int(min_candles_per_leg), 1)
    if df is None or len(df) < (w * 2 + 1):
        return []
        
    try:
        side_upper = str(side).upper()
        col = 'low' if side_upper == "BULL" else 'high'
        series = _get_series(df, col).values.astype(float)
        highs = _get_series(df, 'high').values.astype(float)
        lows = _get_series(df, 'low').values.astype(float)
        closes = _get_series(df, 'close').values.astype(float)
        volumes = _get_series(df, 'volume').values.astype(float) if 'volume' in [c.lower() for c in df.columns] else np.ones(len(df))
        n = len(series)

        # Dynamic 14-period ATR calculation
        tr = np.zeros(n)
        tr[0] = highs[0] - lows[0]
        for idx in range(1, n):
            tr[idx] = max(highs[idx] - lows[idx], abs(highs[idx] - closes[idx - 1]), abs(lows[idx] - closes[idx - 1]))
        atr_arr = pd.Series(tr).rolling(14, min_periods=1).mean().values
        
        raw_pivots: List[int] = []
        
        # Optional: check if start of series is an initial pivot
        if include_endpoints and n > w:
            if side_upper == "BULL" and series[0] == np.min(series[:w + 1]):
                raw_pivots.append(0)
            elif side_upper == "BEAR" and series[0] == np.max(series[:w + 1]):
                raw_pivots.append(0)
                
        # Internal extrema
        for i in range(1, n - 1):
            # Ignore dead flatline quotation bars (0 volume and high == low)
            if volumes[i] == 0 and highs[i] == lows[i]:
                continue
                
            left_w = min(i, w)
            right_w = min(n - 1 - i, w)
            if left_w < 1 or right_w < 1:
                continue
            window = series[i - left_w : i + right_w + 1]
            target = series[i]

            cur_atr = atr_arr[i] if not np.isnan(atr_arr[i]) and atr_arr[i] > 0 else (target * 0.015)
            min_disp = min_atr_factor * cur_atr

            if side_upper == "BULL" and target == np.min(window):
                window_highs = highs[i - left_w : i + right_w + 1]
                if min_atr_factor <= 0 or (np.max(window_highs) - target) >= min_disp:
                    raw_pivots.append(i)
            elif side_upper == "BEAR" and target == np.max(window):
                window_lows = lows[i - left_w : i + right_w + 1]
                if min_atr_factor <= 0 or (target - np.min(window_lows)) >= min_disp:
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
    Validates full market structure:
    - Cascading Parabolic Arches (Lower Highs + Lower Lows for BULL; Higher Highs + Higher Lows for BEAR)
    - Terminal Base / Support Absorption detection (Point 4)
    - Multi-Tier Classification (Tier 1 Gold, Tier 2 Core, Tier 3 Momentum)
    """
    if len(swing_indices) < 2:
        return {
            "valid": False, 
            "reason": "Insufficient swing points", 
            "valid_arch_count": 0,
            "effective_waves": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "tier": 3,
            "tier_label": "TIER_3_MOMENTUM",
            "tier_badge": "🥉 T3",
            "terminal_pivot_idx": -1,
            "bars_since_terminal_base": 999,
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
            
            # Test parabolic curvature
            is_arch = is_parabolic_arch_enhanced(df_wave, side=side_upper, min_r2=min_r2)
            
            if side_upper == "BULL":
                is_monotone = lows[end_idx] < lows[start_idx]  # Lower Low
                wave_peak_or_trough = float(np.max(highs[start_idx : end_idx + 1]))  # Peak
                base_extrema = float(lows[end_idx])
            else:
                is_monotone = highs[end_idx] > highs[start_idx]  # Higher High
                wave_peak_or_trough = float(np.min(lows[start_idx : end_idx + 1]))  # Trough
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
            return {
                "valid": False, 
                "reason": "No waves constructed", 
                "valid_arch_count": 0,
                "effective_waves": 0,
                "cascade_progression": False,
                "has_terminal_base": False,
                "tier": 3,
                "tier_label": "TIER_3_MOMENTUM",
                "tier_badge": "🥉 T3",
                "terminal_pivot_idx": -1,
                "bars_since_terminal_base": 999,
                "details": []
            }

        # Macro trend check: Progressive cascade
        extrema_list = [w["peak_or_trough"] for w in wave_details]
        if len(extrema_list) >= 2:
            if side_upper == "BULL":
                cascade_progression = all(extrema_list[j] > extrema_list[j+1] for j in range(len(extrema_list)-1))
            else:
                cascade_progression = all(extrema_list[j] < extrema_list[j+1] for j in range(len(extrema_list)-1))
        else:
            cascade_progression = True if len(extrema_list) == 1 else False

        # Detect terminal base / absorption (Point 4)
        has_terminal_base = False
        if len(wave_details) >= 2:
            last_wave = wave_details[-1]
            prev_wave = wave_details[-2]
            denom = max(last_wave["base_extrema"], 1e-8)
            price_diff = abs(last_wave["base_extrema"] - prev_wave["base_extrema"]) / denom
            if price_diff <= tolerance_absorption:
                has_terminal_base = True

        terminal_pivot_idx = swing_indices[-1] if swing_indices else -1
        bars_since_terminal_base = len(df) - 1 - terminal_pivot_idx if terminal_pivot_idx >= 0 else 999
        effective_waves = valid_arches + (1 if (has_terminal_base and not wave_details[-1]["is_arch"]) else 0)

        is_full_pattern_valid = bool(
            effective_waves >= min_cascading_waves and 
            cascade_progression and 
            (wave_details[-1]["is_arch"] or has_terminal_base)
        )

        # Multi-Tier Soft Classification
        # Tier 1 (Gold): >= 3 waves + cascade progression + (arch or terminal base) -> 100% Capital (Max Risk)
        # Tier 2 (Core): >= 2 waves + cascade progression -> 70% Capital (Standard)
        # Tier 3 (Momentum): 1 wave or structural re-entry -> 50% Capital (Scalp/Light)
        if effective_waves >= 3 and cascade_progression and (wave_details[-1]["is_arch"] or has_terminal_base):
            tier = 1
            tier_label = "TIER_1_GOLD"
            tier_badge = "🥇 T1"
            risk_scale = 1.0
        elif (effective_waves >= 2 or valid_arches >= 2) and cascade_progression:
            tier = 2
            tier_label = "TIER_2_CORE"
            tier_badge = "🥈 T2"
            risk_scale = 0.70
        else:
            tier = 3
            tier_label = "TIER_3_MOMENTUM"
            tier_badge = "🥉 T3"
            risk_scale = 0.50

        return {
            "valid": is_full_pattern_valid,
            "valid_arch_count": valid_arches,
            "effective_waves": effective_waves,
            "cascade_progression": cascade_progression,
            "has_terminal_base": has_terminal_base,
            "tier": tier,
            "tier_label": tier_label,
            "tier_badge": tier_badge,
            "risk_scale": risk_scale,
            "terminal_pivot_idx": terminal_pivot_idx,
            "bars_since_terminal_base": bars_since_terminal_base,
            "details": wave_details
        }
    except Exception as e:
        return {
            "valid": False,
            "reason": f"Calculation error: {str(e)}",
            "valid_arch_count": 0,
            "effective_waves": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "tier": 3,
            "tier_label": "TIER_3_MOMENTUM",
            "tier_badge": "🥉 T3",
            "risk_scale": 0.50,
            "terminal_pivot_idx": -1,
            "bars_since_terminal_base": 999,
            "details": []
        }


def detect_parabolic_multi_swings(
    df: pd.DataFrame,
    side: str = "BULL",
    min_swings: int = 3,
    min_candles_per_leg: int = 3,
    min_r2: float = 0.50,
    tolerance_absorption: float = 0.02,
    max_bars_since_base: Optional[int] = 18,
    max_bars_after_terminal: Optional[int] = None
) -> Dict[str, Any]:
    """
    Complete end-to-end multi-swing parabolic cascade detector with Multi-Tier Scoring.
    Evaluates:
    - Tier 1 (Gold): >= 3 Parabolic Waves (R^2 >= 0.55) with Terminal Absorption Base (100% Sizing).
    - Tier 2 (Core): >= 2 Parabolic Waves (R^2 >= 0.50) (e.g. Double Bottom / Liquidity Sweep) (70% Sizing).
    - Tier 3 (Momentum): Trend Continuation / Structural Re-entry (50% Sizing).
    """
    effective_max_bars = max_bars_since_base if max_bars_since_base is not None else max_bars_after_terminal
    if effective_max_bars is None:
        effective_max_bars = 18

    # Stage 0: Clean zero-volume quotation flatlines before swing analysis
    df = clean_liquid_candles(df)

    w = max(int(min_candles_per_leg), 1)
    if df is None or len(df) < (w * 2 + 1):
        return {
            "matched": False, 
            "valid": False, 
            "reason": "Insufficient candles", 
            "pivots": [],
            "swing_indices": [],
            "valid_arch_count": 0,
            "effective_waves": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "terminal_pivot_idx": -1,
            "terminal_swing_idx": -1,
            "bars_since_terminal_base": 999,
            "bars_since_terminal": 999,
            "is_recent": False,
            "tier": 3, 
            "tier_label": "TIER_3_MOMENTUM", 
            "tier_badge": "🥉 T3",
            "risk_scale": 0.50,
            "details": []
        }
        
    pivots = extract_swing_pivots(df, side=side, min_candles_per_leg=w, include_endpoints=True)
    if len(pivots) < 3 and w > 2:
        # Fallback to lighter 2-candle pivot order to detect tighter 2-wave structures
        pivots_light = extract_swing_pivots(df, side=side, min_candles_per_leg=2, include_endpoints=True)
        if len(pivots_light) >= 3:
            pivots = pivots_light

    if len(pivots) < 2:
        return {
            "matched": False, 
            "reason": f"Insufficient swing pivots ({len(pivots)} found, need >= 2)", 
            "valid": False,
            "pivots": pivots,
            "swing_indices": pivots,
            "tier": 3,
            "tier_label": "TIER_3_MOMENTUM",
            "tier_badge": "🥉 T3",
            "valid_arch_count": 0,
            "effective_waves": 0,
            "cascade_progression": False,
            "has_terminal_base": False,
            "terminal_pivot_idx": -1,
            "terminal_swing_idx": -1,
            "bars_since_terminal_base": 999,
            "bars_since_terminal": 999,
            "is_recent": False,
            "details": []
        }

    structure = validate_parabolic_cascade_structure(
        df, 
        swing_indices=pivots, 
        side=side,
        min_cascading_waves=min_swings, 
        min_r2=min_r2,
        tolerance_absorption=tolerance_absorption
    )
    
    bars_since = structure.get("bars_since_terminal_base", 0)
    is_recent = True
    if effective_max_bars > 0:
        if bars_since > effective_max_bars:
            is_recent = False

    # Matched if full pattern is valid or Tier 1 / Tier 2 (>= 2 valid cascading arches)
    is_matched = bool((structure.get("valid", False) or structure.get("tier", 3) <= 2) and is_recent)

    terminal_idx = structure.get("terminal_pivot_idx", pivots[-1] if pivots else -1)
    
    structure["matched"] = is_matched
    structure["is_recent"] = is_recent
    structure["pivots"] = pivots
    structure["swing_indices"] = pivots
    structure["terminal_pivot_idx"] = terminal_idx
    structure["terminal_swing_idx"] = terminal_idx
    structure["bars_since_terminal_base"] = bars_since
    structure["bars_since_terminal"] = bars_since
    
    date_col = None
    for c in ["date", "Date", "datetime", "Datetime", "timestamp", "Timestamp"]:
        if c in df.columns:
            date_col = c
            break
    if date_col and len(df) > terminal_idx and terminal_idx >= 0:
        structure["terminal_swing_date"] = str(df[date_col].iloc[terminal_idx])

    return structure


def is_anchor_after_terminal_base(
    df_anchor: pd.DataFrame,
    anchor_time: Any,
    swing_structure: Dict[str, Any]
) -> bool:
    """
    Verifies that Anchor Candle (A) forms at or AFTER the terminal base pivot.
    Prevents triggering on an anchor candle that formed prior to the swing exhaustion.
    """
    if not swing_structure or not swing_structure.get("matched", False):
        return False

    term_idx = swing_structure.get("terminal_pivot_idx", swing_structure.get("terminal_swing_idx", -1))
    if term_idx < 0 or term_idx >= len(df_anchor):
        return True  # If index not determinable, don't hard block

    try:
        date_col = None
        for c in ["date", "Date", "datetime", "Datetime", "timestamp", "Timestamp"]:
            if c in df_anchor.columns:
                date_col = c
                break
                
        if date_col:
            anchor_time_str = str(anchor_time).split("+")[0].strip()
            df_dates = df_anchor[date_col].astype(str).str.split("+").str[0].str.strip().values

            # Find anchor index in df_anchor
            anchor_indices = np.where(df_dates == anchor_time_str)[0]
            if len(anchor_indices) > 0:
                a_idx = anchor_indices[-1]
                # Anchor must form at or after the terminal pivot (allow 1 bar margin for formation)
                return bool(a_idx >= (term_idx - 1))

        return True
    except Exception:
        return True
