import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
REPORT_FILE = 'README.md'
LOG_FILE = 'trade_log.csv'
MARKET_INDEX = '510300'
MIN_DAILY_AMOUNT = 50000000 
ETF_DD_THRESHOLD = -0.06

# --- 1. 标准化读取 ---
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low', '成交量': 'volume'}
        df.rename(columns=mapping, inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

# --- 2. 策略引擎 ---
def analyze_etf(df):
    if len(df) < 30: return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 基础指标
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    roc20 = (last['close'] / df['close'].shift(20).iloc[-1]) - 1
    
    # 筛选逻辑：超跌 + 站上5日线 + 流动性
    if last['close'] > ma5 and drawdown < ETF_DD_THRESHOLD and last['amount'] > MIN_DAILY_AMOUNT:
        # 评分系统
        score = 1
        if last['close'] > ma10: score += 1 # 站上10日线更稳
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1 # 放量企稳
        
        # 建议止损位 (ATR简易版：10日线下3%)
        stop_loss = ma10 * 0.97
        
        return {
            'roc': roc20 * 100,
            'score': score,
            'price': last['close'],
            'stop': stop_loss,
            'amount': last['amount']
        }
    return None

# --- 3. 持仓对账 ---
def monitor_portfolio(portfolio, data_dir):
    hold_results = []
    for _, row in portfolio.iterrows():
        code = str(row['code'])
        f_path = os.path.join(data_dir, f"{code}.csv")
        if os.path.exists(f_path):
            df = load_data(f_path)
            last_price = df['close'].iloc[-1]
            profit = (last_price - row['buy_price']) / row['buy_price'] * 100
            ma10 = df['close'].rolling(10).mean().iloc[-1]
            
            status = "✅ 正常"
            if last_price < row['stop_price']: status = "🚨 破位止损"
            elif last_price < ma10: status = "⚠️ 警示(破10日线)"
            
            hold_results.append({
                'code': code, 'buy_price': row['buy_price'],
                'current': last_price, 'profit': profit, 'status': status
            })
    return hold_results

# --- 4. 主程序：生成看板并推送 ---
def execute():
    # A. 扫描新信号
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        res = analyze_etf(load_data(f))
        if res:
            res['code'] = code
            signals.append(res)
    
    signals.sort(key=lambda x: x['roc'], reverse=True)
    
    # B. 处理持仓
    if not os.path.exists(PORTFOLIO_FILE):
        pd.DataFrame(columns=['code', 'buy_price', 'stop_price']).to_csv(PORTFOLIO_FILE, index=False)
    portfolio = pd.read_csv(PORTFOLIO_FILE)
    holdings = monitor_portfolio(portfolio, DATA_DIR)

    # C. 写入 README.md 看板
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write("# 🚀 天枢 ETF 量化监控中心\n\n")
        f.write(f"更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        
        f.write("## 💰 当前持仓监控\n")
        if holdings:
            f.write("| 代码 | 买入价 | 现价 | 盈亏 | 状态建议 |\n| --- | --- | --- | --- | --- |\n")
            for h in holdings:
                f.write(f"| {h['code']} | {h['buy_price']:.3f} | {h['current']:.3f} | {h['profit']:+.2f}% | {h['status']} |\n")
        else:
            f.write("> 🧊 目前空仓。请在 `portfolio.csv` 中手动录入买入记录。\n")

        f.write("\n## 🎯 入场信号 (超跌共振扫描)\n")
        if signals:
            f.write("| 排名 | 代码 | ROC20% | 得分 | 现价 | 建议止损 |\n| --- | --- | --- | --- | --- | --- |\n")
            for i, s in enumerate(signals[:10], 1):
                f.write(f"| {i} | {s['code']} | {s['roc']:.2f}% | {s['score']} | {s['price']:.3f} | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 市场全线低迷，未发现符合条件的入场标的。\n")

    print(f"✨ 看板已更新。共发现 {len(signals)} 个信号。")

if __name__ == "__main__":
    execute()
