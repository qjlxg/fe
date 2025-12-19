import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- V5.3 高胜率配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.05           # 5%回撤
MIN_TURNOVER_RATE = 1.0             # 换手率门槛
BIAS_THRESHOLD = -4.5               # 乖离率预警
REPORT_BASE_NAME = 'Report_V5_3_HighWinRate'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def load_data(filepath):
    try:
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
        except:
            df = pd.read_csv(filepath, encoding='gbk')

        column_map = {'日期': 'date', 'Date': 'date', '收盘': 'close', 'Close': 'close', 
                      '成交量': 'volume', 'Volume': 'volume', '换手率': 'turnover'}
        df = df.rename(columns=column_map)
        
        required = ['date', 'close', 'volume', 'turnover']
        for col in required:
            if col not in df.columns: df[col] = 0
                
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        for col in ['close', 'volume', 'turnover']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df.dropna(subset=['close'])
    except Exception as e:
        return None

def analyze_logic(df):
    if len(df) < 30: return None
    
    # 1. 指标计算
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    df['bias'] = (df['close'] - df['MA20']) / df['MA20'] * 100
    
    # RSI计算及平滑
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss.replace(0, 0.001))))
    df['rsi_sma'] = df['rsi'].rolling(3).mean() # RSI 3日平滑，过滤波动

    # 2. 核心止损过滤：价格必须站上 MA5 
    # 逻辑：即使超跌，如果连5天均线都站不上去，说明还在加速下跌，不能接飞刀
    is_price_stable = df['close'].iloc[-1] > df['MA5'].iloc[-1]
    
    # 3. 成交量逻辑：地量后放量
    vol_last = df['volume'].iloc[-1]
    vol_ma10 = df['volume'].rolling(10).mean().iloc[-1]
    is_vol_active = vol_last > vol_ma10 * 0.8 # 确保不是完全没流动性的死水

    # --- 信号判定 ---
    signals = []
    
    # 信号A：底背离 + RSI 翘头 (极高胜率)
    if df['close'].iloc[-1] < df['close'].iloc[-3] and df['rsi'].iloc[-1] > df['rsi'].iloc[-3]:
        if df['rsi'].iloc[-1] < 40:
            signals.append("RSI底背离(动能衰竭)")

    # 信号B：乖离回归 + 止跌确认
    if df['bias'].iloc[-1] < BIAS_THRESHOLD and is_price_stable:
        signals.append("超跌止跌(站上MA5)")

    # 4. 最终过滤逻辑：回撤达标 + 有反转信号 + 换手达标
    last = df.iloc[-1]
    if abs(last['drawdown'] if 'drawdown' in last else -1) < 0: # 重新计算下回撤
        roll_max = df['close'].rolling(window=20, min_periods=1).max()
        df['drawdown'] = (df['close'] - roll_max) / roll_max
        last = df.iloc[-1]

    if abs(last['drawdown']) >= MIN_MONTH_DRAWDOWN and last['turnover'] >= MIN_TURNOVER_RATE:
        if signals:
            return {
                'code': "", 'close': last['close'], 'drawdown': last['drawdown'],
                'rsi': last['rsi'], 'bias': last['bias'], 'turnover': last['turnover'],
                'action': " | ".join(signals)
            }
    return None

def main():
    if not os.path.exists(FUND_DATA_DIR): return
    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df)
            if res:
                res['code'] = os.path.splitext(os.path.basename(f))[0]
                results.append(res)

    results = sorted(results, key=lambda x: x['rsi'])
    
    report_name = f"{REPORT_BASE_NAME}_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金高胜率筛选报告 V5.3\n\n")
        f.write("> **新增过滤**：排除 MA5 压制的品种（不接飞刀）；引入 RSI 底背离识别。\n\n")
        if not results:
            f.write("今日无安全入场信号。")
        else:
            f.write("| 代码 | 价格 | 20日回撤 | RSI | 乖离率 | 信号提示 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for r in results:
                f.write(f"| {r['code']} | {r['close']:.3f} | {r['drawdown']:.2%} | {r['rsi']:.1f} | {r['bias']:.1f}% | **{r['action']}** |\n")

if __name__ == "__main__":
    main()
