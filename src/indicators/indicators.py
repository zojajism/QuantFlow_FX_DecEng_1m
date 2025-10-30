

from typing import List
import math

def ema8_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-8:]]
    return sum(closes) / len(closes)

def ema21_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-21:]]
    return sum(closes) / len(closes)

def rsi14_placeholder(candles: List[dict]) -> float:
    if len(candles) < 2:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-15:]]
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d for d in diffs if d > 0]
    losses = [-d for d in diffs if d < 0]
    avg_gain = sum(gains) / 14 if gains else 0.0
    avg_loss = sum(losses) / 14 if losses else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def atr14_placeholder(candles: List[dict]) -> float:
    if len(candles) < 2:
        return float("nan")
    window = candles[-15:]
    trs = []
    for i in range(1, len(window)):
        h = float(window[i]["high"])
        l = float(window[i]["low"])
        prev_close = float(window[i - 1]["close"])
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    return sum(trs) / len(trs) if trs else float("nan")

def bollinger_mid_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-20:]]
    return sum(closes) / len(closes)

def bollinger_upper_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-20:]]
    mean = sum(closes) / len(closes)
    variance = sum((x - mean) ** 2 for x in closes) / len(closes)
    std = variance ** 0.5
    return mean + 2 * std

def bollinger_lower_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-20:]]
    mean = sum(closes) / len(closes)
    variance = sum((x - mean) ** 2 for x in closes) / len(closes)
    std = variance ** 0.5
    return mean - 2 * std

def macd_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles]
    ema12 = sum(closes[-12:]) / min(len(closes), 12)
    ema26 = sum(closes[-26:]) / min(len(closes), 26)
    return ema12 - ema26

def vwap_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    total_pv = sum(float(c["close"]) * float(c["volume"]) for c in candles)
    total_vol = sum(float(c["volume"]) for c in candles)
    return total_pv / total_vol if total_vol > 0 else float("nan")

def donchian_high_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    highs = [float(c["high"]) for c in candles[-20:]]
    return max(highs)

def donchian_low_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    lows = [float(c["low"]) for c in candles[-20:]]
    return min(lows)

def obv_placeholder(candles: List[dict]) -> float:
    if len(candles) < 2:
        return float("nan")
    obv = 0.0
    for i in range(1, len(candles)):
        c_prev = float(candles[i - 1]["close"])
        c_curr = float(candles[i]["close"])
        vol = float(candles[i]["volume"])
        if c_curr > c_prev:
            obv += vol
        elif c_curr < c_prev:
            obv -= vol
    return obv

# ---- Simple Moving Averages (SMA) ----
def sma50_placeholder(candles: List[dict]) -> float:
    if len(candles) < 1:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-50:]]
    return sum(closes) / len(closes)

def sma200_placeholder(candles: List[dict]) -> float:
    if len(candles) < 1:
        return float("nan")
    closes = [float(c["close"]) for c in candles[-200:]]
    return sum(closes) / len(closes)

# ---- MACD Components ----
def macd_line_placeholder(candles: List[dict]) -> float:
    # Same as your macd_placeholder
    if not candles:
        return float("nan")
    closes = [float(c["close"]) for c in candles]
    ema12 = sum(closes[-12:]) / min(len(closes), 12)
    ema26 = sum(closes[-26:]) / min(len(closes), 26)
    return ema12 - ema26

def macd_signal_placeholder(candles: List[dict]) -> float:
    # Very simplified signal line (9-period SMA of MACD line)
    if len(candles) < 26:
        return float("nan")
    macd_values = []
    closes = [float(c["close"]) for c in candles]
    for i in range(len(closes)-26+1, len(closes)+1):
        window = closes[:i]
        ema12 = sum(window[-12:]) / min(len(window), 12)
        ema26 = sum(window[-26:]) / min(len(window), 26)
        macd_values.append(ema12 - ema26)
    if len(macd_values) < 9:
        return float("nan")
    return sum(macd_values[-9:]) / 9

# ---- Stochastic Oscillator ----
def stoch_k_placeholder(candles: List[dict]) -> float:
    if len(candles) < 14:
        return float("nan")
    window = candles[-14:]
    highs = [float(c["high"]) for c in window]
    lows = [float(c["low"]) for c in window]
    closes = [float(c["close"]) for c in window]
    highest_high = max(highs)
    lowest_low = min(lows)
    if highest_high == lowest_low:
        return 50.0
    return (closes[-1] - lowest_low) / (highest_high - lowest_low) * 100.0

def stoch_d_placeholder(candles: List[dict]) -> float:
    if len(candles) < 16:
        return float("nan")
    # 3-period SMA of %K
    k_values = []
    for i in range(len(candles)-14+1, len(candles)+1):
        window = candles[i-14:i]
        highs = [float(c["high"]) for c in window]
        lows = [float(c["low"]) for c in window]
        closes = [float(c["close"]) for c in window]
        highest_high = max(highs)
        lowest_low = min(lows)
        k = 50.0 if highest_high == lowest_low else (closes[-1] - lowest_low) / (highest_high - lowest_low) * 100.0
        k_values.append(k)
    if len(k_values) < 3:
        return float("nan")
    return sum(k_values[-3:]) / 3

# ---- CCI (Commodity Channel Index) ----
def cci_placeholder(candles: List[dict]) -> float:
    if len(candles) < 20:
        return float("nan")
    window = candles[-20:]
    typical_prices = [(float(c["high"]) + float(c["low"]) + float(c["close"])) / 3 for c in window]
    tp_mean = sum(typical_prices) / len(typical_prices)
    mean_dev = sum(abs(tp - tp_mean) for tp in typical_prices) / len(typical_prices)
    if mean_dev == 0:
        return 0.0
    return (typical_prices[-1] - tp_mean) / (0.015 * mean_dev)

# ---- ATR Moving Average (for comparison) ----
def atr_ma_placeholder(candles: List[dict]) -> float:
    if len(candles) < 30:
        return float("nan")
    trs = []
    for i in range(1, len(candles)):
        h = float(candles[i]["high"])
        l = float(candles[i]["low"])
        prev_close = float(candles[i - 1]["close"])
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
    if len(trs) < 30:
        return float("nan")
    atr_values = [sum(trs[i-14:i]) / 14 for i in range(14, len(trs))]
    return sum(atr_values[-30:]) / 30

# ---- MFI (Money Flow Index) ----
def mfi_placeholder(candles: List[dict]) -> float:
    if len(candles) < 15:
        return float("nan")
    window = candles[-15:]
    pos_flow, neg_flow = 0.0, 0.0
    for i in range(1, len(window)):
        tp_prev = (float(window[i-1]["high"]) + float(window[i-1]["low"]) + float(window[i-1]["close"])) / 3
        tp_curr = (float(window[i]["high"]) + float(window[i]["low"]) + float(window[i]["close"])) / 3
        raw_money_flow = tp_curr * float(window[i]["volume"])
        if tp_curr > tp_prev:
            pos_flow += raw_money_flow
        elif tp_curr < tp_prev:
            neg_flow += raw_money_flow
    if neg_flow == 0:
        return 100.0
    mfr = pos_flow / neg_flow
    return 100 - (100 / (1 + mfr))

# ---- Volume & Volume MA ----
def volume_placeholder(candles: List[dict]) -> float:
    if not candles:
        return float("nan")
    return float(candles[-1]["volume"])

def volume_ma_placeholder(candles: List[dict]) -> float:
    if len(candles) < 20:
        return float("nan")
    vols = [float(c["volume"]) for c in candles[-20:]]
    return sum(vols) / len(vols)