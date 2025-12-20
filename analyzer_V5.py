import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime

# --- 豹哥核心配置 ---
TOTAL_ASSETS = 100000              # 你的总本金，建议仓位会根据这个计算
FUND_DATA_DIR = 'fund_data'        # 数据文件夹
BENCHMARK_CODE = '510300'          # 大盘风向标
WIN_RATE_THRESHOLD = 0.40          # 历史胜率低于40%的一律不要
TURNOVER_CONFIRM = 1.0             # 换手倍率低于1.0的一律不买

def load_data(filepath):
    try:
        df = pd.read_csv(filepath, encoding='utf-8')
    except:
        df = pd.read_csv(filepath, encoding='gbk')
    df.columns = [c.strip() for c in df.columns]
    column_map = {'日期': 'date', '收盘': 'close', '最高': 'high', '最低': 'low', '换手率': 'turnover'}
    df = df.rename(columns=column_map)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date').reset_index(drop=True)
    for col in ['close', 'high', 'low', 'turnover']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    return df.dropna(subset=['close'])

def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "🌤️ 早春", 1.0
    df = load_data(path)
    df['MA20'] = df['close'].rolling(20).mean()
    bias = ((df['close'].iloc[-1] - df['MA20'].iloc[-1]) / df['MA20'].iloc[-1]) * 100
    if bias < -4: return bias, "❄️ 深冬 (严控仓位)", 0.5
    if bias < -2: return bias, "🌨️ 初冬 (谨慎出击)", 0.8
    return bias, "🌤️ 早春 (正常执行)", 1.0

def check_history_win_rate(df):
    if len(df) < 60: return 0.0
    temp = df.tail(250).copy()
    temp['MA5'] = temp['close'].rolling(5).mean()
    delta = temp['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    temp['rsi'] = 100 - (100 / (1 + gain/loss.replace(0, 0.001)))
    success, total = 0, 0
    for i in range(20, len(temp)-6):
        if temp['rsi'].iloc[i] < 35 and temp['close'].iloc[i] > temp['MA5'].iloc[i]:
            total += 1
            if (temp['close'].iloc[i+1:i+6].max() - temp['close'].iloc[i]) / temp['close'].iloc[i] >= 0.02:
                success += 1
    return success/total if total > 0 else 0.0

def analyze():
    bias_val, weather, multiplier = get_market_weather()
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    
    print(f"\n🚀 豹哥实战指令报告 ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print(f"当前环境: {weather}")
    print("-" * 70)
    print(f"{'代码':<8} | {'动作':<10} | {'买入参考':<8} | {'建议买多少':<10} | {'止损卖出价':<8}")
    print("-" * 70)

    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        df = load_data(f)
        if len(df) < 30: continue
        
        last = df.iloc[-1]
        df['MA5'] = df['close'].rolling(5).mean()
        df['TO_MA10'] = df['turnover'].rolling(10).mean()
        
        # 指标计算
        tr = pd.concat([(df['high'] - df['low']), (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14).mean().iloc[-1]
        
        # 判定条件
        is_right_side = last['close'] > last['MA5']
        to_ratio = last['turnover'] / df['TO_MA10'].iloc[-1] if df['TO_MA10'].iloc[-1] > 0 else 0
        win_rate = check_history_win_rate(df)
        
        # 核心逻辑
        action = "🔴 别看"
        buy_price = f"{last['close']:.3f}"
        pos_str = "0"
        stop_price = "0.000"
        
        # 只要跌得多且站上5日线
        drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
        
        if drawdown < -0.045:
            if is_right_side:
                # 双重装甲过滤
                if to_ratio >= TURNOVER_CONFIRM and win_rate >= WIN_RATE_THRESHOLD:
                    action = "🟢 搞它"
                    # 计算止损：现价 - 2倍波动
                    stop_val = last['close'] - (2 * atr)
                    stop_price = f"{stop_val:.3f}"
                    # 计算仓位：总本金1%风险测算
                    risk_per_share = last['close'] - stop_val
                    if risk_per_share > 0:
                        raw_pos = (TOTAL_ASSETS * 0.01) / (risk_per_share / last['close'])
                        pos_str = f"{min(raw_pos * multiplier, TOTAL_ASSETS*0.3)/10000:.1f}万"
                else:
                    action = "🟡 等信号"
            else:
                action = "🟡 等破5日线"

        if action != "🔴 别看":
            results.append([code, action, buy_price, pos_str, stop_price])

    # 排序：搞它的放在最前面
    results.sort(key=lambda x: x[1], reverse=False)
    for r in results:
        print(f"{r[0]:<8} | {r[1]:<10} | {r[2]:<10} | {r[3]:<12} | {r[4]:<8}")
    print("-" * 70)
    print("豹哥嘱托：不绿不买，到点就卖，别跟基金谈恋爱！")

if __name__ == "__main__":
    analyze()
