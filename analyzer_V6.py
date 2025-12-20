import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, time as dt_time
import warnings
import csv

warnings.filterwarnings('ignore')

# --- 系统配置 ---
TOTAL_ASSETS = 100000
DATA_DIR = 'fund_data'
MARKET_INDEX = '510300'
STRATEGY_LOG = "天枢进阶实战日志.csv"

# 核心参数
DRAWDOWN_THRESHOLD = -0.045
ATR_MULTIPLIER = 2
RISK_PER_TRADE = 0.01

def calculate_indicators(df):
    """计算核心技术指标"""
    # 1. 均线
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    # 2. RSI (14日)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    
    # 3. MACD (12, 26, 9)
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    
    # 4. 换手率均线
    df['TO_MA5'] = df['turnover'].rolling(5).mean()
    
    # 5. ATR 止损
    tr = pd.concat([
        (df['high'] - df['low']), 
        (df['high'] - df['close'].shift()).abs(), 
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    
    return df

def analyze_logic(df):
    """多指标共振逻辑"""
    if len(df) < 30: return "⚪ 观望", 0
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    
    # 条件1：价格站上5日线 (趋势初步扭转)
    cond1 = last['close'] > last['MA5']
    
    # 条件2：RSI 处于低位回升或非超买区 (底部确认)
    cond2 = last['RSI'] > 35  # 从超卖区脱离
    
    # 条件3：MACD 柱状图翻红 或 DIF上行 (动能增强)
    cond3 = last['MACD_Hist'] > prev['MACD_Hist']
    
    # 条件4：放量 (换手率超过5日平均的1.2倍，代表有主力吃货)
    cond4 = last['turnover'] > (last['TO_MA5'] * 1.2)
    
    # 条件5：超跌空间
    cond5 = drawdown < DRAWDOWN_THRESHOLD

    if cond1 and cond5:
        # 如果满足价格和回撤，再看辅助指标减分或加分
        score = sum([cond2, cond3, cond4])
        if score >= 2: # 至少满足两个辅助指标才介入
            return "🟢 介入", score
        else:
            return "🟡 观察", score
            
    return "⚪ 观望", 0

def execute_analysis():
    # ... (此处复用之前的环境检查代码) ...
    # 获取大盘权重
    from __main__ import get_market_sentiment, load_data, calculate_position # 假设在同一脚本或导入
    bias, sentiment, weight = get_market_sentiment()
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    findings = []

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        
        df = load_data(f)
        df = calculate_indicators(df)
        
        decision, score = analyze_logic(df)
        
        if decision != "⚪ 观望":
            last = df.iloc[-1]
            stop_price = last['close'] - (ATR_MULTIPLIER * last['atr'])
            shares = calculate_position(last['close'], stop_price, weight) if decision == "🟢 介入" else 0
            
            findings.append({
                'code': code, 'decision': decision, 'price': last['close'], 
                'shares': shares, 'stop': round(stop_price, 3),
                'score': score, 'rsi': round(last['RSI'], 1)
            })

    # 排序：评分最高（共振指标最多）的排前面
    findings.sort(key=lambda x: (x['score'], x['shares']), reverse=True)

    # --- 报告输出 ---
    print("\n" + "—"*85)
    print(f"🚀 天枢进阶版 | 指标共振系统 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"大盘状态: {sentiment} | 过滤因子: RSI+MACD+Turnover")
    print("—"*85)
    print(f"{'代码':<8} | {'决策':<8} | {'共振数':<6} | {'现价':<8} | {'RSI':<6} | {'建议股数':<10} | {'止损线':<8}")
    print("-" * 85)

    for r in findings:
        print(f"{r['code']:<8} | {r['decision']:<8} | {r['score']:<8} | {r['price']:<8.3f} | {r['rsi']:<6} | {r['shares']:<12} | {r['stop']:<8.3f}")

if __name__ == "__main__":
    execute_analysis()
