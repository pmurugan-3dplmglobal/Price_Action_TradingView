"""
common/parabolic_curve_filter.py
Parabolic Multi-Swing Curve & Cascade Structure Detection Filter.
Re-exports reference implementation from common/swing_detection.py.
"""
from swing_detection import (
    _get_series,
    is_parabolic_arch_enhanced,
    extract_swing_pivots,
    validate_parabolic_cascade_structure,
    detect_parabolic_multi_swings,
    is_anchor_after_terminal_base
)

__all__ = [
    "_get_series",
    "is_parabolic_arch_enhanced",
    "extract_swing_pivots",
    "validate_parabolic_cascade_structure",
    "detect_parabolic_multi_swings",
    "is_anchor_after_terminal_base"
]

