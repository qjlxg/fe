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
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        return df.dropna(subset=['close'])
    except:
        return None

def get_market_weather():
    path = os.path.join(FUND_DATA_DIR, f"{BENCHMARK_CODE}.csv")
    if not os.path.exists(path): return 0, "🌤️ 早春", 1.0
    df = load_data(path)
    if df is None or len(df) < 20: return 0, "🌤️ 早春", 1.0
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
    
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == BENCHMARK_CODE: continue
        df = load_data(f)
        if df is None or len(df) < 30: continue
        
        df['MA5'] = df['close'].rolling(5).mean()
        df['TO_MA10'] = df['turnover'].rolling(10).mean()
        tr = pd.concat([(df['high'] - df['low']), (df['high'] - df['close'].shift()).abs(), (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14).mean()
        
        last = df.iloc[-1]
        to_ratio = last['turnover'] / last['TO_MA10'] if last['TO_MA10'] > 0 else 0
        win_rate = check_history_win_rate(df)
        drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
        
        action = "🔴 别看"
        buy_price = f"{last['close']:.3f}"
        pos_val = 0.0
        pos_str = "0"
        stop_price = "0.000"
        
        if drawdown < -0.045:
            if last['close'] > last['MA5']:
                if to_ratio >= TURNOVER_CONFIRM and win_rate >= WIN_RATE_THRESHOLD:
                    action = "🟢 搞它"
                    stop_val = last['close'] - (2 * last['atr'])
                    stop_price = f"{stop_val:.3f}"
                    risk_per_share = last['close'] - stop_val
                    if risk_per_share > 0:
                        raw_pos = (TOTAL_ASSETS * 0.01) / (risk_per_share / last['close'])
                        pos_val = min(raw_pos * multiplier, TOTAL_ASSETS * 0.3)
                        pos_str = f"{pos_val/10000:.1f}万"
                else:
                    action = "🟡 等信号"
            else:
                action = "🟡 等破5日线"

        if action != "🔴 别看":
            # 记录用于排序的权重：🟢为2，🟡为1；🟢内部按金额排
            weight = 2 if action == "🟢 搞它" else 1
            results.append({
                'code': code, 'action': action, 'price': buy_price, 
                'pos_str': pos_str, 'pos_val': pos_val, 
                'stop': stop_price, 'weight': weight
            })

    # --- 排序逻辑：动作权重降序，金额降序 ---
    results.sort(key=lambda x: (x['weight'], x['pos_val']), reverse=True)

    # --- 生成报告内容 ---
    report = []
    report.append(f"\n🚀 豹哥实战指令报告 ({datetime.now().strftime('%Y-%m-%d %H:%M')})")
    report.append(f"当前环境: {weather}")
    report.append("-" * 75)
    report.append(f"{'代码':<8} | {'动作':<10} | {'买入参考':<8} | {'建议买多少':<10} | {'止损卖出价':<8}")
    report.append("-" * 75)

    for r in results:
        report.append(f"{r['code']:<8} | {r['action']:<10} | {r['price']:<10} | {r['pos_str']:<12} | {r['stop']:<8}")
    
    report.append("-" * 75)
    report.append("豹哥嘱托：【1.不绿不买】 【2.按量下单】 【3.破位必卖】")

    final_text = "\n".join(report)
    print(final_text)
    
    # 写入文件
    with open("豹哥操作手册.txt", "w", encoding="utf-8") as f:
        f.write(final_text)
    print(f"\n✅ 报告已保存至目录下的 [豹哥操作手册.txt]")

if __name__ == "__main__":
    analyze()
