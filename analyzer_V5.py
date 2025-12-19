import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- 核心实战配置 ---
TOTAL_ASSETS = 100000              
FUND_DATA_DIR = 'fund_data'
BENCHMARK_CODE = '510300'          
REPORT_BASE_NAME = 'Trading_Decision_Report'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    try:
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')
        
        column_map = {
            '日期': 'date', '开盘': 'open', '收盘': 'close', 
            '最高': 'high', '最低': 'low', '成交量': 'volume'
        }
        df = df.rename(columns=column_map)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        
        for col in ['close', 'high', 'low']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna(subset=['close'])
    except Exception as e:
        return None

def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "未知天气", 1.0
    df = load_data(path)
    if df is None or len(df) < 25: return 0, "数据收集中", 1.0
    
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    
    if bias < -4: return bias, "❄️ 深冬 (极寒，需大幅超跌)", 0.6
    if bias < -2: return bias, "🌨️ 初冬 (转冷，严格过滤)", 0.8
    if bias < 1:  return bias, "🌤️ 早春 (蓄势，正常操作)", 1.0
    return bias, "☀️ 盛夏 (亢奋，注意风险)", 0.5

def analyze_logic(df, bias_val, weather_multiplier):
    if len(df) < 30: return None
    
    dynamic_rsi_limit = 35 + (bias_val * 1.2)
    df['MA5'] = df['close'].rolling(5).mean()
    
    # ATR 计算
    tr = pd.concat([
        (df['high'] - df['low']),
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()

    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    
    # 回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max

    last = df.iloc[-1]
    is_right_side = last['close'] > last['MA5'] and (last['MA5'] >= df['MA5'].iloc[-2])
    is_oversold = last['rsi'] < dynamic_rsi_limit

    if abs(last['drawdown']) >= 0.045:
        # 默认权重：🟢为3, 🟡为2, 🔴为1，用于排序
        sort_weight = 1
        decision = "🔴 观望 (未见止跌)"
        pos_ratio = "0%"
        stop_price = 0.0
        
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
            'close': last['close'], 'rsi': last['rsi'], 'drawdown': last['drawdown'],
            'pos': pos_ratio, 'stop': stop_price, 'decision': decision, 'weight': sort_weight
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

    # --- 核心改进：按信号强度排序 (🟢 > 🟡 > 🔴) ---
    results = sorted(results, key=lambda x: x['weight'], reverse=True)

    report_name = f"{REPORT_BASE_NAME}_{file_time}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金实战决策报告 (V5.8)\n")
        f.write(f"**分析时间**: {run_time} | **市场环境**: {weather_desc}\n\n")
        f.write(f"> **实战提醒**: 当前大盘偏离度 {bias_val:.2f}%。系统已优先排列高价值信号。\n\n")
        f.write("| 基金代码 | 现价 | RSI | 20日回撤 | 建议仓位 | 止损参考 | 最终决策 |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
        
        for r in results:
            f.write(f"| {r['code']} | {r['close']:.3f} | {r['rsi']:.1f} | {r['drawdown']:.1%}| {r['pos']} | {r['stop']:.3f} | **{r['decision']}** |\n")
            
            # 复盘日志累积
            with open('history_signals.csv', 'a', encoding='utf-8') as log:
                if log.tell() == 0: log.write("日期,代码,价格,决策,建议仓位,天气\n")
                log.write(f"{run_time},{r['code']},{r['close']},{r['decision']},{r['pos']},{weather_desc}\n")

if __name__ == "__main__":
    main()
