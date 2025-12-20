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
LOG_FILE = 'trade_signals_history.csv' # 历史记录
MARKET_INDEX = '510300'
MAX_HOLD_COUNT = 5
ETF_DD_THRESHOLD = -0.06

def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        # 自动识别中文列名
        mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low'}
        df.rename(columns=mapping, inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

def analyze_logic(df):
    if len(df) < 30: return None
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 指标计算
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    roc20 = df['close'].pct_change(20).iloc[-1]
    
    # 筛选条件
    cond_price = last['close'] > ma5
    cond_dd = dd < ETF_DD_THRESHOLD
    
    if cond_price and cond_dd:
        # 简单评分：站上10日线加1分，成交量放大加1分
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        
        return {
            'roc': roc20 * 100,
            'score': score,
            'price': last['close'],
            'stop': ma10 * 0.96 # 建议止损设在10日线下4%
        }
    return None

def execute_system():
    # 1. 扫描
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    signals = []
    
    # 获取大盘情绪 (Bias)
    mkt_df = load_data(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv"))
    mkt_bias = (mkt_df['close'].iloc[-1] / mkt_df['close'].rolling(20).mean().iloc[-1] - 1)

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        res = analyze_logic(load_data(f))
        if res:
            res['code'] = code
            signals.append(res)

    # 2. 排序
    signals.sort(key=lambda x: x['roc'], reverse=True)
    top_signals = signals[:10]

    # 3. 写入 README.md (推送至 GitHub 目录)
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write("# 🛰️ 天枢 ETF 监控系统\n\n")
        f.write(f"更新时间: `{datetime.now().strftime('%Y-%m-%d %H:%M')}` (北京时间)\n\n")
        f.write(f"### 📊 市场背景\n- **大盘偏离度 (Bias)**: `{mkt_bias:.2%}`\n")
        f.write(f"- **操作建议**: {'🚨 保持谨慎' if mkt_bias < -0.02 else '✅ 分批建仓'}\n\n")
        
        f.write("### 🎯 推荐关注列表 (入场参考)\n")
        f.write("| 代码 | ROC20% | 得分 | 现价 | 建议止损 |\n| --- | --- | --- | --- | --- |\n")
        for s in top_signals:
            f.write(f"| {s['code']} | {s['roc']:.2f}% | {s['score']} | {s['price']:.3f} | {s['stop']:.3f} |\n")
        
        f.write(f"\n> 💡 **说明**: 列表按强度排序。得分越高说明共振越强。")

    # 4. 写入历史记录 CSV
    history_df = pd.DataFrame(top_signals)
    history_df['date'] = datetime.now().strftime('%Y-%m-%d')
    header = not os.path.exists(LOG_FILE)
    history_df.to_csv(LOG_FILE, mode='a', index=False, header=header, encoding='utf_8_sig')

    print(f"✨ 扫描完成，已生成报告至 {REPORT_FILE} 并更新日志。")

if __name__ == "__main__":
    execute_system()
