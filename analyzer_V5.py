import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import logging

# --- V5.4 高胜率/ETF专用配置参数 ---
FUND_DATA_DIR = 'fund_data'
MIN_MONTH_DRAWDOWN = 0.05           # 20日内回撤门槛 (5%)
MIN_TURNOVER_RATE = 1.0             # 换手率门槛 (1%)
BIAS_THRESHOLD = -4.5               # 乖离率预警 (ETF跌破均线4.5%通常是极限)
REPORT_BASE_NAME = 'Report_V5_4'

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
    """
    针对 ETF 优化的过滤逻辑
    """
    if len(df) < 30: return None
    
    # 1. 基础指标计算 (先计算，再引用，修复 KeyError)
    # 计算均线
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    # 计算乖离率
    df['bias'] = (df['close'] - df['MA20']) / df['MA20'] * 100
    
    # 计算20日最大回撤
    roll_max = df['close'].rolling(window=20, min_periods=1).max()
    df['drawdown'] = (df['close'] - roll_max) / roll_max
    
    # 计算 RSI(14)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss.replace(0, 0.001))))

    # 2. 提取最新数据
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 3. 核心信号判定
    signals = []
    
    # 【信号A】底背离确认：价格新低但动能(RSI)没创新低
    # 这是减少假信号的关键：说明下跌速度正在放缓
    if last['close'] < prev['close'] and last['rsi'] > prev['rsi'] and last['rsi'] < 40:
        signals.append("RSI底背离")

    # 【信号B】止跌确认：价格必须重新站上 5 日均线
    # 目的：不接飞刀。如果还在 MA5 下方，说明跌势未止
    if last['close'] > last['MA5'] and prev['close'] <= prev['MA5']:
        if last['bias'] < -2.0: # 稍微超跌即可
            signals.append("MA5金叉(止跌确认)")

    # 4. 最终复合筛选
    # 必须满足：20日大回撤 + (背离 或 止跌) + 换手率足够
    if abs(last['drawdown']) >= MIN_MONTH_DRAWDOWN and last['turnover'] >= MIN_TURNOVER_RATE:
        if signals:
            return {
                'code': "", 
                'close': last['close'],
                'drawdown': last['drawdown'],
                'rsi': last['rsi'],
                'bias': last['bias'],
                'turnover': last['turnover'],
                'action': " | ".join(signals)
            }
    return None

def main():
    if not os.path.exists(FUND_DATA_DIR):
        os.makedirs(FUND_DATA_DIR)
        return

    files = glob.glob(os.path.join(FUND_DATA_DIR, "*.csv"))
    results = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        df = load_data(f)
        if df is not None:
            res = analyze_logic(df)
            if res:
                res['code'] = code
                results.append(res)

    # 按回撤幅度排序
    results = sorted(results, key=lambda x: x['drawdown'])

    report_name = f"{REPORT_BASE_NAME}_{datetime.now().strftime('%Y%m%d')}.md"
    with open(report_name, 'w', encoding='utf-8') as f:
        f.write(f"# 基金高胜率分析报告 (V5.4)\n\n")
        f.write("> **防亏损机制**：增加了 MA5 止跌确认（不接飞刀）和 RSI 底背离过滤。\n\n")
        if not results:
            f.write("今日市场未发现满足“止跌确认”的高胜率信号。\n")
        else:
            f.write("| 代码 | 价格 | 20日回撤 | RSI | 乖离率 | 信号提示 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for r in results:
                f.write(f"| {r['code']} | {r['close']:.3f} | {r['drawdown']:.2%} | {r['rsi']:.1f} | {r['bias']:.1f}% | **{r['action']}** |\n")
    
    print(f"分析完成，报告已生成：{report_name}")

if __name__ == "__main__":
    main()
