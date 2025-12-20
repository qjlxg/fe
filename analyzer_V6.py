import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# --- 系统配置 ---
TOTAL_ASSETS = 100000     # 初始模拟资金
DATA_DIR = 'fund_data'    # 数据存储目录
MARKET_INDEX = '510300'   # 沪深300ETF作为行情风向标
DRAWDOWN_THRESHOLD = -0.045
ATR_MULTIPLIER = 2        # ATR止损倍数
RISK_PER_TRADE = 0.01     # 单笔交易承担总资金 1% 的风险

# --- 1. 核心工具函数 ---
def load_data(file_path):
    """加载数据并格式化"""
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')
        return df
    except Exception as e:
        print(f"读取文件 {file_path} 出错: {e}")
        return pd.DataFrame()

def get_market_sentiment():
    """计算大盘情绪滤网"""
    mkt_file = os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")
    if not os.path.exists(mkt_file):
        return 0, "未知", 1.0  # 无数据时默认不加权
    
    mkt_df = load_data(mkt_file)
    if len(mkt_df) < 20: return 0, "数据不足", 1.0
    
    # 计算乖离率 (BIAS)
    ma20 = mkt_df['close'].rolling(20).mean().iloc[-1]
    current = mkt_df['close'].iloc[-1]
    bias = (current - ma20) / ma20
    
    if bias > 0.03: return bias, "🔥 极强", 1.2
    if bias < -0.03: return bias, "❄️ 冰点", 0.7
    return bias, "⚖️ 平衡", 1.0

def calculate_position(price, stop_price, market_weight):
    """基于固定风险额度的仓位计算"""
    risk_cash = TOTAL_ASSETS * RISK_PER_TRADE
    unit_risk = max(price - stop_price, price * 0.01) # 最小风险间距设定为1%
    
    shares = (risk_cash / unit_risk) * market_weight
    return int(shares // 100) * 100

# --- 2. 指标与逻辑 ---
def calculate_indicators(df):
    """计算核心技术指标"""
    if len(df) < 30: return df
    
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    
    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    
    # 换手率均线
    df['TO_MA5'] = df['turnover'].rolling(5).mean()
    
    # ATR
    tr = pd.concat([
        (df['high'] - df['low']), 
        (df['high'] - df['close'].shift()).abs(), 
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    
    return df

def analyze_logic(df):
    """多指标共振决策"""
    if len(df) < 30 or 'MA5' not in df.columns: 
        return "⚪ 观望", 0
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    
    # 条件
    cond1 = last['close'] > last['MA5']
    cond2 = last['RSI'] > 35
    cond3 = last['MACD_Hist'] > prev['MACD_Hist']
    cond4 = last['turnover'] > (last['TO_MA5'] * 1.2)
    cond5 = drawdown < DRAWDOWN_THRESHOLD

    if cond1 and cond5:
        score = sum([cond2, cond3, cond4])
        return ("🟢 介入", score) if score >= 2 else ("🟡 观察", score)
    return "⚪ 观望", 0

# --- 3. 执行主程序 ---
def execute_analysis():
    if not os.path.exists(DATA_DIR):
        print(f"⚠️ 错误: 目录 '{DATA_DIR}' 不存在，请先下载数据。")
        return

    bias, sentiment, mkt_weight = get_market_sentiment()
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
            shares = calculate_position(last['close'], stop_price, mkt_weight) if decision == "🟢 介入" else 0
            
            findings.append({
                'code': code, 'decision': decision, 'price': last['close'], 
                'shares': shares, 'stop': round(stop_price, 3),
                'score': score, 'rsi': round(last['RSI'], 1)
            })

    findings.sort(key=lambda x: (x['score'], x['shares']), reverse=True)

    # 输出报告
    print("\n" + "="*85)
    print(f"🚀 天枢共振系统 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"大盘情绪: {sentiment} (Bias: {bias:.2%}) | 风险暴露: {RISK_PER_TRADE*100}%")
    print("="*85)
    print(f"{'代码':<8} | {'决策':<8} | {'共振':<4} | {'现价':<8} | {'RSI':<6} | {'建议股数':<10} | {'止损线':<8}")
    print("-" * 85)

    for r in findings:
        print(f"{r['code']:<8} | {r['decision']:<8} | {r['score']:<4} | {r['price']:<8.3f} | {r['rsi']:<6} | {r['shares']:<12} | {r['stop']:<8.3f}")

if __name__ == "__main__":
    execute_analysis()
