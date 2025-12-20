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
TRACKER_FILE = 'signal_performance_tracker.csv' # 历史信号跟踪（用于成功率分析）
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 3  # 看板仅显示3分及以上信号
ETF_DD_THRESHOLD = -0.06

# --- 1. 数据标准化读取 ---
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low', '成交量': 'volume'}
        df.rename(columns=mapping, inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

# --- 2. 深度评分引擎 ---
def analyze_signal(df):
    if len(df) < 30: return None
    last = df.iloc[-1]
    
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    amt_ma5 = df['amount'].rolling(5).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    roc20 = (last['close'] / df['close'].shift(20).iloc[-1]) - 1

    # 评分逻辑
    score = 0
    # 基础门槛：超跌 + 站上5日线
    if last['close'] > ma5 and dd < ETF_DD_THRESHOLD:
        score = 1
        # 2分：确认短期趋势（站上10日线）
        if last['close'] > ma10: score += 1
        # 3分：确认主力异动（今日成交额 > 5日平均）
        if last['amount'] > amt_ma5: score += 1
            
    if score >= 1:
        return {
            'roc': roc20 * 100,
            'score': score,
            'price': last['close'],
            'stop': ma10 * 0.97, # 建议止损位
            'date': datetime.now().strftime('%Y-%m-%d')
        }
    return None

# --- 3. 历史信号记录模块 ---
def update_tracker(signals):
    if not signals: return
    new_df = pd.DataFrame(signals)
    if os.path.exists(TRACKER_FILE):
        old_df = pd.read_csv(TRACKER_FILE)
        # 避免同日期重复记录
        combined = pd.concat([old_df, new_df]).drop_duplicates(subset=['date', 'code'])
        combined.to_csv(TRACKER_FILE, index=False, encoding='utf_8_sig')
    else:
        new_df.to_csv(TRACKER_FILE, index=False, encoding='utf_8_sig')

# --- 4. 执行主流程 ---
def execute():
    # A. 扫描数据
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []
    
    # 市场情绪
    mkt_df = load_data(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv"))
    mkt_bias = (mkt_df['close'].iloc[-1] / mkt_df['close'].rolling(20).mean().iloc[-1] - 1) if not mkt_df.empty else 0

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        res = analyze_signal(load_data(f))
        if res:
            res['code'] = code
            all_signals.append(res)

    # B. 更新历史跟踪表 (记录 1,2,3 分所有信号)
    update_tracker(all_signals)

    # C. 筛选精英信号 (仅展示 3 分)
    elite_signals = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]
    elite_signals.sort(key=lambda x: x['roc'], reverse=True)

    # D. 处理持仓对账 (如有)
    holdings_md = "> 🧊 空仓中。可在 `portfolio.csv` 手动录入记录。"
    if os.path.exists(PORTFOLIO_FILE):
        port = pd.read_csv(PORTFOLIO_FILE)
        if not port.empty:
            holdings_md = "| 代码 | 买入价 | 现价 | 盈亏% |\n| --- | --- | --- | --- |\n"
            for _, row in port.iterrows():
                f_path = os.path.join(DATA_DIR, f"{row['code']}.csv")
                if os.path.exists(f_path):
                    last_c = pd.read_csv(f_path).iloc[-1]['收盘']
                    profit = (last_c - row['buy_price']) / row['buy_price'] * 100
                    holdings_md += f"| {row['code']} | {row['buy_price']:.3f} | {last_c:.3f} | {profit:+.2f}% |\n"

    # E. 写入 README 看板
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英监控看板\n\n")
        f.write(f"最后同步: `{datetime.now().strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"### 📊 市场底色\n- **大盘偏离度 (Bias20)**: `{mkt_bias:.2%}`\n")
        f.write(f"- **风控建议**: {'🟢 积极探路' if mkt_bias > -0.01 else '🟡 严格止损'}\n\n")
        
        f.write(f"### 💰 实时持仓监控\n{holdings_md}\n\n")
        
        f.write(f"### 🎯 精英入场信号 (得分 ≥ {MIN_SCORE_SHOW})\n")
        if elite_signals:
            f.write("| 代码 | ROC20% | 得分 | 现价 | 建议止损 |\n| --- | --- | --- | --- | --- |\n")
            for s in elite_signals:
                f.write(f"| {s['code']} | {s['roc']:.2f}% | 🔥 {s['score']} | {s['price']:.3f} | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 今日暂无 3 分共振信号。1-2 分潜在信号已存入后台 `signal_performance_tracker.csv`。\n")
            
        f.write("\n---\n*注：历史所有信号及后续表现请下载分析 `signal_performance_tracker.csv`。*")

if __name__ == "__main__":
    execute()
