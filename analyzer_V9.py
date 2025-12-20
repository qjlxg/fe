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
LOG_FILE = 'strategy_log.csv'  # 历史信号日志
MARKET_INDEX = '510300'
MAX_HOLD_COUNT = 5
MIN_DAILY_AMOUNT = 50000000 
RISK_PER_TRADE = 0.015
ETF_DD_THRESHOLD = -0.06

# --- 1. 数据处理 ---
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        column_map = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low'}
        df.rename(columns=column_map, inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

def calculate_indicators(df):
    if len(df) < 30: return df
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    df['ROC20'] = df['close'].pct_change(20)
    exp1 = df['close'].ewm(span=12, adjust=False).mean(); exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['MACD_Hist'] = (exp1 - exp2 - (exp1 - exp2).ewm(span=9, adjust=False).mean()) * 2
    return df

# --- 2. 报告生成逻辑 ---
def generate_reports(sentiment_data, hold_monitor, new_signals):
    """将结果写入 README.md 和历史日志"""
    # A. 更新 README.md (看板)
    with open("README.md", "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 监控看板\n\n")
        f.write(f"更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"### 📊 市场环境\n- 状态: {sentiment_data['status']}\n- 乖离率: `{sentiment_data['bias']:.2%}`\n\n")
        
        f.write(f"### 💰 持仓监控\n")
        if hold_monitor:
            f.write("| 代码 | 现价 | 盈亏 | 状态 |\n| --- | --- | --- | --- |\n")
            for h in hold_monitor:
                f.write(f"| {h['code']} | {h['price']:.3f} | {h['profit']:.2f}% | {h['status']} |\n")
        else: f.write("> 空仓中\n")

        f.write(f"\n### 🎯 入场信号\n")
        if new_signals:
            f.write("| 代码 | ROC20 | 评分 | 建议止损 |\n| --- | --- | --- | --- |\n")
            for s in new_signals[:5]:
                f.write(f"| {s['code']} | {s['roc']:.2f}% | {s['score']} | {s['stop']:.3f} |\n")
        else: f.write("> 暂无信号\n")

    # B. 追加到历史日志 CSV
    log_entries = []
    for s in new_signals:
        log_entries.append({'date': datetime.now().date(), 'code': s['code'], 'type': 'SIGNAL', 'price': s['price']})
    if log_entries:
        log_df = pd.DataFrame(log_entries)
        header = not os.path.exists(LOG_FILE)
        log_df.to_csv(LOG_FILE, mode='a', index=False, header=header, encoding='utf_8_sig')

# --- 3. 执行主流程 ---
def execute_system():
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    if not os.path.exists(PORTFOLIO_FILE):
        pd.DataFrame(columns=['code', 'buy_price', 'shares', 'stop_price']).to_csv(PORTFOLIO_FILE, index=False)
    portfolio = pd.read_csv(PORTFOLIO_FILE)
    
    # 大盘分析
    mkt_df = load_data(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv"))
    ma20 = mkt_df['close'].rolling(20).mean().iloc[-1]
    bias = (mkt_df['close'].iloc[-1] - ma20) / ma20
    sentiment = {"status": "🔥 强劲" if bias > 0.02 else "⚖️ 平衡", "bias": bias}

    new_signals, hold_monitor = [], []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        df = calculate_indicators(load_data(f))
        if len(df) < 30: continue
        last = df.iloc[-1]

        if code in portfolio['code'].astype(str).values:
            p_row = portfolio[portfolio['code'].astype(str) == code].iloc[0]
            hold_monitor.append({
                'code': code, 'price': last['close'], 'profit': (last['close']-p_row['buy_price'])/p_row['buy_price']*100,
                'status': "💥 止损" if last['close'] < p_row['stop_price'] else "✅ 正常"
            })
        elif last['amount'] > MIN_DAILY_AMOUNT:
            # 简化版买入逻辑
            if last['close'] > last['MA5'] and (last['close']-df['close'].rolling(20).max().iloc[-1])/df['close'].rolling(20).max().iloc[-1] < ETF_DD_THRESHOLD:
                new_signals.append({
                    'code': code, 'roc': last['ROC20']*100, 'price': last['close'], 
                    'score': 1 if last['MACD_Hist'] > df.iloc[-2]['MACD_Hist'] else 0,
                    'stop': min(last['close'] - 2*last['atr'], last['MA10']*0.95)
                })

    new_signals.sort(key=lambda x: x['roc'], reverse=True)
    generate_reports(sentiment, hold_monitor, new_signals)
    print("✨ 报告已生成到目录。")

if __name__ == "__main__":
    execute_system()
