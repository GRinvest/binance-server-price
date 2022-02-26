from sys import float_info as sflt

import pandas as pd
from numpy import nan as npNaN


def non_zero_range(high: pd.Series, low: pd.Series) -> pd.Series:
    """Returns the difference of two series and adds epsilon to any zero values.
    This occurs commonly in crypto data when 'high' = 'low'."""
    diff = high - low
    if diff.eq(0).any().any():
        diff += sflt.epsilon
    return diff


def true_range(high, low, close, drift=None):
    high_low_range = non_zero_range(high, low)
    prev_close = close.shift(drift)
    ranges = [high_low_range, high - prev_close, prev_close - low]
    _true_range = pd.concat(ranges, axis=1)
    _true_range = _true_range.abs().max(axis=1)
    _true_range.iloc[:drift] = npNaN
    return _true_range


def hlc3(high, low, close):
    """Indicator: HLC3"""
    temp = (high + low + close) / 3.0

    temp.name = "HLC3"
    return temp
