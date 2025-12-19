import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime

# --- 实战配置区 ---
TOTAL_ASSETS = 100000        # 假设你的总资金量（用于计算仓位）
FUND_DATA_DIR = 'fund_data'
BENCHMARK_CODE = '510300'    # 沪深300作为天气预报风向标

# --- 1. 天气预报逻辑 ---
def get_market_weather():
    """根据大盘偏离度判定天气"""
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "未知", 1.0
    
    df = pd.read_csv(path).tail(30)
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    
    if bias < -4: return bias, "❄️ 深冬（极度严寒，提级审核）", 0.6  # 仓位系数
    if bias < -2: return bias, "🌨️ 初冬（微寒，严格过滤）", 0.8
    if bias < 1:  return bias, "🌤️ 早春（蓄势，正常执行）", 1.0
    return bias, "☀️ 盛夏（亢奋，警惕追高）", 0.5

# --- 2. 核心分析逻辑 ---
def analyze_logic(df, bias_val, weather_multiplier):
    if len(df) < 30: return None
    
    # 动态调整阈值：天气越冷，RSI门槛越低（要求更超跌）
    base_rsi_limit = 35
    dynamic_rsi_limit = base_rsi_limit + (bias_val * 1.5) 
    
    # 计算指标
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    # ATR计算（用于动态止损和仓位）
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    df['atr'] = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1).rolling(14).mean()
    
    # RSI计算
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/14, adjust=False).mean()
    df['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))

    last = df.iloc[-1]
    
    # 判定条件
    is_oversold = last['rsi'] < dynamic_rsi_limit
    is_stop_falling = last['close'] > last['MA5'] and (last['MA5'] >= df['MA5'].iloc[-2])
    
    if is_oversold and is_stop_falling:
        # 仓位计算：单笔风险不超过总资产的 1%
        stop_loss_price = last['close'] - (2 * last['atr'])
        risk_per_share = last['close'] - stop_loss_price
        # 建议金额 = (总资产 * 1%) / 风险间距 * 天气系数
        suggested_amt = (TOTAL_ASSETS * 0.01) / (risk_per_share / last['close']) * weather_multiplier
        pos_ratio = suggested_amt / TOTAL_ASSETS
        
        return {
            'close': last['close'],
            'rsi': last['rsi'],
            'stop_loss': stop_loss_price,
            'pos_ratio': pos_ratio,
            'weather_limit': dynamic_rsi_limit
        }
    return None

# --- 3. 自动化报告与记录 ---
def main():
    bias_val, weather_desc, weather_multiplier = get_market_weather()
    results = []
    
    for f in glob.glob(os.path.join(FUND_DATA_DIR, "*.csv")):
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        
        df = pd.read_csv(f)
        res = analyze_logic(df, bias_val, weather_multiplier)
        if res:
            res['code'] = code
            results.append(res)

    # 打印报告
    print(f"\n{'='*50}")
    print(f"ETF实战决策报告 V5.8 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"当前市场天气：{weather_desc}")
    print(f"动态RSI门槛：{35 + (bias_val * 1.5):.1f}")
    print(f"{'='*50}\n")
    
    if not results:
        print("今日无符合条件的优质‘种子’。")
    else:
        print(f"{'代码':<8} | {'现价':<6} | {'RSI':<5} | {'建议仓位':<8} | {'止损价':<6}")
        for r in results:
            print(f"{r['code']:<8} | {r['close']:<8.3f} | {r['rsi']:<7.1f} | {r['pos_ratio']:<11.1%} | {r['stop_loss']:.3f}")
            
            # 自动记录复盘日志
            with open('history_signals.csv', 'a', encoding='utf-8') as f_log:
                f_log.write(f"{datetime.now().date()},{r['code']},{r['close']},{r['pos_ratio']:.2%},{r['stop_loss']:.3f},{weather_desc}\n")

if __name__ == "__main__":
    if not os.path.exists('history_signals.csv'):
        with open('history_signals.csv', 'w') as f: f.write("日期,代码,价格,仓位,止损价,天气\n")
    main()
