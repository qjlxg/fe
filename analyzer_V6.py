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
PORTFOLIO_FILE = 'portfolio.csv'
MARKET_INDEX = '510300'
MAX_HOLD_COUNT = 5
MIN_DAILY_AMOUNT = 50000000
RISK_PER_TRADE = 0.015
ETF_DD_THRESHOLD = -0.06

# --- 缺失函数补全（必须有）---
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        return df
    except:
        return pd.DataFrame()

def get_market_sentiment():
    mkt_path = os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")
    if not os.path.exists(mkt_path):
        return 0, "未知", 1.0
    mkt_df = load_data(mkt_path)
    if len(mkt_df) < 20:
        return 0, "数据不足", 1.0
    ma20 = mkt_df['close'].rolling(20).mean().iloc[-1]
    current = mkt_df['close'].iloc[-1]
    bias = (current - ma20) / ma20
    if bias > 0.02: return bias, "🔥 强劲", 1.2
    if bias < -0.02: return bias, "❄️ 冰点", 0.6
    return bias, "⚖️ 平衡", 1.0

def calculate_indicators(df):
    if len(df) < 60: return df
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    
    tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    df['ROC20'] = df['close'].pct_change(20)
    
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    df['TO_MA5'] = df['turnover'].rolling(5).mean()
    df['AMT_MA5'] = df['amount'].rolling(5).mean()
    return df

def analyze_etf_logic(df):
    if len(df) < 30: return "⚪ 观望", 0
    last = df.iloc[-1]
    prev = df.iloc[-2]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    
    cond_price = last['close'] > last['MA5']
    cond_dd = drawdown < ETF_DD_THRESHOLD
    cond_liq = last['AMT_MA5'] >= MIN_DAILY_AMOUNT
    
    if cond_price and cond_dd and cond_liq:
        score = sum([
            last['RSI'] > 40,
            last['MACD_Hist'] > prev['MACD_Hist'],
            last['turnover'] > last['TO_MA5'] * 1.1
        ])
        return ("🟢 介入" if score >= 2 else "🟡 观察"), score
    return "⚪ 观望", 0

def check_exit_conditions(df, portfolio_row):
    last = df.iloc[-1]
    prev = df.iloc[-2]
    reasons = []
    if last['close'] <= portfolio_row['stop_price']:
        reasons.append("💥 触发止损")
    if last['close'] < last['MA10']:
        reasons.append("📉 破10日线")
    if last['MACD_Hist'] < prev['MACD_Hist'] and last['RSI'] > 65:
        reasons.append("⚠️ 动能减弱")
    return " | ".join(reasons) if reasons else "✅ 正常持仓"

# --- 主执行系统 ---
def execute_system():
    if not os.path.exists(DATA_DIR):
        print("❌ 数据目录不存在！")
        return
    
    # 初始化持仓文件
    if not os.path.exists(PORTFOLIO_FILE):
        pd.DataFrame(columns=['code', 'buy_price', 'shares', 'stop_price']).to_csv(PORTFOLIO_FILE, index=False)
    portfolio = pd.read_csv(PORTFOLIO_FILE)
    
    bias, sentiment, mkt_weight = get_market_sentiment()
    
    current_holds = portfolio['code'].tolist() if not portfolio.empty else []
    available_slots = MAX_HOLD_COUNT - len(current_holds)  # 剩余可买入名额
    
    new_signals = []
    hold_monitor = []
    
    for f in glob.glob(os.path.join(DATA_DIR, "*.csv")):
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        
        df = load_data(f)
        if df.empty or len(df) < 30: continue
        df = calculate_indicators(df)
        last = df.iloc[-1]
        
        # 持仓监控
        if code in current_holds:
            p_row = portfolio[portfolio['code'] == code].iloc[0]
            status = check_exit_conditions(df, p_row)
            profit = (last['close'] - p_row['buy_price']) / p_row['buy_price'] * 100
            hold_monitor.append({
                'code': code, 'profit': profit, 'price': last['close'],
                'stop': p_row['stop_price'], 'status': status, 'shares': p_row['shares']
            })
            continue
        
        # 新信号扫描
        decision, score = analyze_etf_logic(df)
        if decision == "⚪ 观望": continue
        
        atr_stop = last['close'] - 2 * last['atr']
        ma_stop = last['MA10'] * 0.95
        stop_price = min(atr_stop, ma_stop)
        
        risk_gap = max(last['close'] - stop_price, last['close'] * 0.015)
        risk_cash = TOTAL_ASSETS * RISK_PER_TRADE * mkt_weight
        shares = int((risk_cash / risk_gap) // 100 * 100)
        
        new_signals.append({
            'code': code, 'decision': decision, 'roc20': last['ROC20']*100,
            'score': score, 'price': last['close'], 'shares': shares,
            'stop': round(stop_price, 3)
        })
    
    # 排序新信号
    new_signals.sort(key=lambda x: (x['roc20'], x['score']), reverse=True)
    new_signals = new_signals[:available_slots] if available_slots > 0 else []
    
    # 输出报告
    print("\n" + "="*100)
    print(f"🚀 天枢ETF全仓位轮动系统 | {datetime.now().strftime('%Y-%m-%d %H:%M')} | 当前日期：2025-12-20")
    print(f"大盘情绪：{sentiment} (Bias: {bias:+.2%}) | 市场权重：{mkt_weight:.1f} | 持仓/上限：{len(current_holds)}/{MAX_HOLD_COUNT}")
    print("="*100)
    
    # 持仓表
    if hold_monitor:
        print("\n【持仓监控】（建议：出现🚩信号立即卖出）")
        print(f"{'代码':<8} {'股数':<6} {'盈亏%':<8} {'现价':<8} {'止损价':<8} {'状态'}")
        print("-"*70)
        for h in hold_monitor:
            tag = "🚩 建议卖出" if "✅" not in h['status'] else ""
            print(f"{h['code']:<8} {h['shares']:<6} {h['profit']:>7.2f}% {h['price']:<8.3f} {h['stop']:<8.3f} {h['status']} {tag}")
    
    # 新信号表
    if new_signals:
        print("\n【新入场信号】（建议：次日早盘买入，记录至portfolio.csv）")
        print(f"{'排名':<4} {'代码':<8} {'ROC20':<8} {'现价':<8} {'建议股数':<10} {'止损价':<8}")
        print("-"*70)
        for i, s in enumerate(new_signals, 1):
            star = "★" if s['decision'] == "🟢 介入" else ""
            print(f"{i:<4} {star}{s['code']:<8} {s['roc20']:>7.2f}% {s['price']:<8.3f} {s['shares']:<10} {s['stop']:<8.3f}")
    else:
        print("\n【新入场信号】：暂无，保持观望或现金。")
    
    if not hold_monitor and not new_signals:
        print("\n当前空仓，耐心等待强势信号出现。")

if __name__ == "__main__":
    execute_system()
