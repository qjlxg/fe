import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import akshare as ak
import time
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

# 核心监控池：涵盖宽基、行业、跨境
ETF_POOL = ["510300", "510500", "588000", "159915", "513100", "512880", "512480", "515030", "159920"]

# --- 1. 自动数据抓取函数 ---
def update_live_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    print(f"🔄 正在通过 AKShare 更新 {len(ETF_POOL)} 只 ETF 的最新行情...")
    for code in ETF_POOL:
        try:
            # 获取最近 100 个交易日的日线数据
            df = ak.fund_etf_hist_sina(symbol=code).tail(100)
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']
            df['turnover'] = df['volume'] / 1000000 # 估算换手
            df.to_csv(os.path.join(DATA_DIR, f"{code}.csv"), index=False)
            print(f"✅ {code} 更新成功")
            time.sleep(0.2)
        except Exception as e:
            print(f"⚠️ {code} 更新失败: {e}")

# --- 2. 核心分析函数 ---
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

def get_market_sentiment():
    mkt_df = load_data(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv"))
    if len(mkt_df) < 20: return 0, "数据不足", 1.0
    ma20 = mkt_df['close'].rolling(20).mean().iloc[-1]
    bias = (mkt_df['close'].iloc[-1] - ma20) / ma20
    if bias > 0.02: return bias, "🔥 强劲", 1.2
    if bias < -0.02: return bias, "❄️ 冰点", 0.6
    return bias, "⚖️ 平衡", 1.0

def calculate_indicators(df):
    if len(df) < 30: return df
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    tr = pd.concat([(df['high']-df['low']), (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    df['ROC20'] = df['close'].pct_change(20)
    # MACD & RSI
    exp1 = df['close'].ewm(span=12, adjust=False).mean(); exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = exp1 - exp2; df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD_Hist'] = (df['DIF'] - df['DEA']) * 2
    delta = df['close'].diff(); gain = (delta.where(delta > 0, 0)).rolling(14).mean(); loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['RSI'] = 100 - (100 / (1 + gain/loss))
    df['AMT_MA5'] = df['amount'].rolling(5).mean()
    return df

# --- 3. 执行主流程 ---
def execute_system():
    # 步骤1：更新数据
    update_live_data()
    
    # 步骤2：初始化账本
    if not os.path.exists(PORTFOLIO_FILE):
        pd.DataFrame(columns=['code', 'buy_price', 'shares', 'stop_price']).to_csv(PORTFOLIO_FILE, index=False)
    portfolio = pd.read_csv(PORTFOLIO_FILE)
    
    # 步骤3：大盘分析
    bias, sentiment, mkt_weight = get_market_sentiment()
    
    current_holds = portfolio['code'].astype(str).tolist()
    new_signals, hold_monitor = [], []

    # 步骤4：全池扫描
    for code in ETF_POOL:
        df = load_data(os.path.join(DATA_DIR, f"{code}.csv"))
        if len(df) < 30: continue
        df = calculate_indicators(df)
        last = df.iloc[-1]

        if code in current_holds:
            # 监控逻辑
            p_row = portfolio[portfolio['code'].astype(str) == code].iloc[0]
            # 简单止损检查
            status = "✅ 正常"
            if last['close'] < p_row['stop_price']: status = "💥 触发止损"
            elif last['close'] < last['MA10']: status = "📉 破10日线"
            
            hold_monitor.append({
                'code': code, 'profit': (last['close']-p_row['buy_price'])/p_row['buy_price']*100,
                'price': last['close'], 'status': status
            })
        else:
            # 信号逻辑
            drawdown = (last['close'] - df['close'].rolling(20).max().iloc[-1]) / df['close'].rolling(20).max().iloc[-1]
            if last['close'] > last['MA5'] and drawdown < ETF_DD_THRESHOLD and last['AMT_MA5'] >= MIN_DAILY_AMOUNT:
                stop_p = min(last['close'] - 2*last['atr'], last['MA10']*0.95)
                new_signals.append({
                    'code': code, 'roc': last['ROC20']*100, 'price': last['close'], 'stop': stop_p
                })

    # 步骤5：输出可视化报告
    print("\n" + "="*80)
    print(f"🚀 天枢实战报告 | 大盘: {sentiment} | 权重: {mkt_weight}")
    print("="*80)
    
    if hold_monitor:
        print("\n【持仓监控】")
        for h in hold_monitor:
            print(f"🔹 {h['code']} | 收益: {h['profit']:.2f}% | 现价: {h['price']:.3f} | 状态: {h['status']}")
            
    if new_signals:
        print("\n【备选信号】(按强度排序)")
        new_signals.sort(key=lambda x: x['roc'], reverse=True)
        for s in new_signals[:3]:
            print(f"🌟 {s['code']} | ROC20: {s['roc']:.2%}| 现价: {s['price']:.3f} | 建议止损: {s['stop']:.3f}")
    else:
        print("\n💡 暂无新入场信号")

if __name__ == "__main__":
    execute_system()
