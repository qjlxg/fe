import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime

# --- 增强版系统配置 ---
TOTAL_ASSETS = 100000
DATA_DIR = 'fund_data'
MIN_DAILY_AMOUNT = 50000000  # 流动性门槛：日成交额需 > 5000万
RISK_PER_TRADE = 0.02       # 单笔风险提高到 2% (ETF波动较小)

# ETF 专用动态参数
ETF_DD_THRESHOLD = -0.06     # 超跌回撤放宽至 -6%
ETF_RSI_FLOOR = 40          # 提高 RSI 门槛，确保不是在阴跌中

def calculate_advanced_indicators(df):
    """为 ETF 优化的指标计算"""
    if len(df) < 60: return df
    
    # 基础均线系统
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    # 流动性：5日平均成交额
    df['AMT_MA5'] = df['amount'].rolling(5).mean()
    
    # RSI (14日)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    
    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    
    # ATR 波动率
    tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    
    return df

def generate_exit_signals(df):
    """卖出逻辑：多维离场检测"""
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    reasons = []
    # 1. 均线死叉/跌破 (MA10)
    if last['close'] < last['MA10']:
        reasons.append("破10日线")
    # 2. MACD 走弱 (红柱缩短)
    if last['MACD_Hist'] < prev['MACD_Hist'] and last['MACD_Hist'] > 0:
        reasons.append("动能减弱")
    # 3. 超买止盈
    if last['RSI'] > 75:
        reasons.append("RSI超买")
        
    return " | ".join(reasons) if reasons else "持仓/安全"

def analyze_etf_logic(df):
    """为 ETF 调优的进场共振逻辑"""
    if len(df) < 30: return "⚪ 观望", 0
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 1. 流动性过滤
    if last['AMT_MA5'] < MIN_DAILY_AMOUNT:
        return "⚪ 流动性差", 0
    
    # 2. 超跌计算
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    
    # 核心买入条件
    cond_price = last['close'] > last['MA5']     # 价格站上5日线
    cond_dd = drawdown < ETF_DD_THRESHOLD      # 满足超跌
    
    # 辅助评分项
    score_items = [
        last['RSI'] > ETF_RSI_FLOOR,           # 强弱度过滤
        last['MACD_Hist'] > prev['MACD_Hist'], # 动能改善
        last['turnover'] > df['turnover'].rolling(5).mean().iloc[-1] * 1.1 # 温和放量
    ]
    
    if cond_price and cond_dd:
        score = sum(score_items)
        return ("🟢 介入", score) if score >= 2 else ("🟡 观察", score)
    
    return "⚪ 观望", 0

# --- 后续配套功能建议 ---
# 1. 轮动模块：每周比较一次池内 ETF 的 20日强度（ROC），只保留前三名。
# 2. 回测模块：接入 Tushare 或 AKShare 的历史数据进行 Vectorized Backtest。
