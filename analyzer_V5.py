import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- 核心配置区 ---
TOTAL_ASSETS = 100000              # 模拟总资产，用于计算建议买入金额
FUND_DATA_DIR = 'fund_data'
BENCHMARK_CODE = '510300'          # 天气风向标（沪深300）
REPORT_BASE_NAME = 'Trading_Decision_Report'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    """适配 CSV 格式：日期,开盘,收盘,最高,最低,成交量"""
    try:
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')
        
        column_map = {'日期': 'date', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low'}
        df = df.rename(columns=column_map)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        for col in ['close', 'high', 'low']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna(subset=['close'])
    except:
        return None

def get_market_weather():
    """环境感应：判定大盘所处的‘季节’"""
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "未知天气", 1.0
    df = load_data(path)
    if df is None or len(df) < 25: return 0, "数据收集中", 1.0
    
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    
    if bias < -4: return bias, "❄️ 深冬 (极寒，必须极度超跌)", 0.6
    if bias < -2: return bias, "🌨️ 初冬 (转冷，严控仓位)", 0.8
    if bias < 1:  return bias, "🌤️ 早春 (蓄势，正常执行)", 1.0
    return bias, "☀️ 盛夏 (亢奋，警惕追高)", 0.5

def check_history_win_rate(df, lookback=250):
    """回测该标的过去一年中类似信号的胜率（T+5 涨幅 > 2% 算成功）"""
    if len(df) < 60: return "N/A"
    temp = df.tail(lookback).copy()
    temp['MA5'] = temp['close'].rolling(5).mean()
    # 简易RSI
    delta = temp['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    temp['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    
    success, total = 0, 0
    for i in range(20, len(temp)-6):
        if temp['rsi'].iloc[i] < 35 and temp['close'].iloc[i] > temp['MA5'].iloc[i]:
            total += 1
            future_max = temp['close'].iloc[i+1:i+6].max()
            if (future_max - temp['close'].iloc[i]) / temp['close'].iloc[i] >= 0.02:
                success += 1
    return f"{success/total:.0%}" if total > 0 else "0%"

def analyze_logic(df, bias_val, weather_multiplier):
    if len(df) < 30: return None
    
    # 1. 动态阈值
    dynamic_rsi_limit = 35 + (bias_val * 1.2)
    df['MA5'] = df['close'].rolling(5).mean()
    
    # 2. ATR 及 风险计算
    tr = pd.concat([
        (df['high'] - df['low']),
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()

    # 3. RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    
    # 4. 回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    is_right_side = last['close'] > last['MA5'] and (last['MA5'] >= df['MA5'].iloc[-2] * 0.999)
    is_oversold = last['rsi'] < dynamic_rsi_limit

    # 初始状态
    sort_weight = 1
    decision = "🔴 观望 (未见止跌)"
    pos_ratio = "0%"
    stop_price = 0.0
    win_rate = check_history_win_rate(df)

    if abs(last['drawdown']) >= 0.045:
        if is_right_side and is_oversold:
            decision = "🟢 买入 (环境确认)"
            sort_weight = 3
            stop_price = last['close'] - (2 * last['atr'])
            risk_unit = last['close'] - stop_price
            if risk_unit > 0:
                raw_pos = (TOTAL_ASSETS * 0.01) / (risk_unit / last['close'])
                pos_ratio = f"{(raw_pos * weather_multiplier / TOTAL_ASSETS):.1%}"
        elif is_oversold:
            decision = "🟡 预警 (待破5日线)"
            sort_weight = 2
            
        return {
            'code': "", 'close': last['close'], 'rsi': last['rsi'], 'drawdown': last['drawdown'],
            'pos': pos_ratio, 'stop': stop_price, 'decision': decision, 'weight': sort_weight, 'win': win_rate
        }
    return None

def main():
    run_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    file_time = datetime.now().strftime('%Y%m%d_%H%M')
    bias_val, weather_desc, weather_multiplier = get_market_weather()
    
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df, bias_val, weather_multiplier)
            if res:
                res['code'] = code
                results.append(res)

    # 核心：按买入强度排序
    results = sorted(results, key=lambda x: x['weight'], reverse=True)

    report_name = f"{REPORT_BASE_NAME}_{file_time}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金实战决策报告 (V5.8 排序增强版)\n")
        f.write(f"**分析时间**: {run_time} | **市场环境**: {weather_desc}\n\n")
        f.write(f"| 代码 | 现价 | RSI | 回撤 | 建议仓位 | 止损参考 | 信号胜率 | 最终决策 |\n")
        f.write(f"| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        for r in results:
            stop_str = f"{r['stop']:.3f}" if r['stop'] > 0 else "0.000"
            f.write(f"| {r['code']} | {r['close']:.3f} | {r['rsi']:.1f} | {r['drawdown']:.1%} | {r['pos']} | {stop_str} | {r['win']} | **{r['decision']}** |\n")
            
            # 复盘日志保存
            with open('history_signals.csv', 'a', encoding='utf-8') as log:
                if log.tell() == 0: log.write("日期,代码,价格,决策,仓位,天气\n")
                log.write(f"{run_time},{r['code']},{r['close']},{r['decision']},{r['pos']},{weather_desc}\n")

if __name__ == "__main__":
    main()
