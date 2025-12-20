import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# --- 系统配置 ---
TOTAL_ASSETS = 100000
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'  # 持仓记录文件
MARKET_INDEX = '510300'
MAX_HOLD_COUNT = 5
MIN_DAILY_AMOUNT = 50000000 
RISK_PER_TRADE = 0.015

# --- 1. 核心计算与退出逻辑 ---
def calculate_indicators(df):
    """计算指标：含 ATR、MA、ROC、MACD、RSI"""
    if len(df) < 60: return df
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    df['ROC20'] = df['close'].pct_change(20)
    
    # MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean(); exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2; df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    
    # RSI
    delta = df['close'].diff(); gain = (delta.where(delta > 0, 0)).rolling(14).mean(); loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    df['TO_MA5'] = df['turnover'].rolling(5).mean()
    return df

def check_exit_conditions(code, df, portfolio_row):
    """持仓卖出信号判定"""
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    reasons = []
    # 1. 智能止损位触发 (读取账本中的止损价)
    if last['close'] <= portfolio_row['stop_price']:
        reasons.append("💥 触发止损")
    # 2. 趋势破位 (跌破10日线)
    if last['close'] < last['MA10']:
        reasons.append("📉 破10日线")
    # 3. 动能衰竭 (MACD红柱缩短且RSI高位)
    if last['MACD_Hist'] < prev['MACD_Hist'] and last['RSI'] > 65:
        reasons.append("⚠️ 动能减弱")
    
    return " | ".join(reasons) if reasons else "✅ 正常"

# --- 2. 持仓与扫描执行 ---
def execute_system():
    # A. 加载持仓账本
    if not os.path.exists(PORTFOLIO_FILE):
        pd.DataFrame(columns=['code', 'buy_price', 'shares', 'stop_price']).to_csv(PORTFOLIO_FILE, index=False)
    portfolio = pd.read_csv(PORTFOLIO_FILE)
    
    # B. 大盘滤网
    from __main__ import get_market_sentiment # 沿用前述函数
    bias, sentiment, mkt_weight = get_market_sentiment()
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    new_signals = []
    hold_monitor = []

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        
        df = pd.read_csv(f)
        df.columns = [c.lower() for c in df.columns]
        df = calculate_indicators(df)
        if len(df) < 30: continue
        last = df.iloc[-1]

        # --- 情况 1：监控持仓 ---
        if code in portfolio['code'].astype(str).values:
            p_row = portfolio[portfolio['code'].astype(str) == code].iloc[0]
            status = check_exit_conditions(code, df, p_row)
            profit = (last['close'] - p_row['buy_price']) / p_row['buy_price']
            hold_monitor.append({
                'code': code, 'profit': profit, 'status': status, 
                'price': last['close'], 'stop': p_row['stop_price']
            })
            continue # 持仓券不参与新信号扫描

        # --- 情况 2：扫描新信号 ---
        if last['amount'] < MIN_DAILY_AMOUNT: continue
        
        from __main__ import analyze_etf_logic # 沿用前述逻辑
        decision, score = analyze_etf_logic(df)
        
        if decision != "⚪ 观望":
            atr_stop = last['close'] - (2 * last['atr'])
            ma10_stop = last['MA10'] * 0.95
            stop_p = min(atr_stop, ma10_stop)
            
            new_signals.append({
                'code': code, 'roc20': last['ROC20'], 'score': score,
                'price': last['close'], 'stop': stop_p, 'decision': decision
            })

    # --- 3. 结果输出 ---
    print(f"\n🚀 天枢全仓位管理系统 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"大盘状态: {sentiment} | 建议总仓位权重: {mkt_weight}")
    
    # 表格1：持仓监控表
    print("\n" + "【持仓监控表】" + "—"*70)
    print(f"{'代码':<8} | {'盈亏%':<8} | {'现价':<8} | {'止损价':<8} | {'状态/建议':<10}")
    for h in hold_monitor:
        color_status = f"🚩 {h['status']}" if "✅" not in h['status'] else h['status']
        print(f"{h['code']:<8} | {h['profit']:>7.2%} | {h['price']:<8.3f} | {h['stop']:<8.3f} | {color_status}")

    # 表格2：新券备选池
    print("\n" + "【新券入场池】" + "—"*70)
    new_signals.sort(key=lambda x: (x['roc20'], x['score']), reverse=True)
    print(f"{'代码':<8} | {'ROC20%':<8} | {'得分':<4} | {'入场参考价':<10} | {'拟设止损':<8}")
    for s in new_signals[:MAX_HOLD_COUNT]:
        print(f"{s['code']:<8} | {s['roc20']:>7.2%} | {s['score']:<4} | {s['price']:<10.3f} | {s['stop']:<8.3f}")

if __name__ == "__main__":
    execute_system()
