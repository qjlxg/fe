import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心参数 ---
TOTAL_CAPITAL = 100000       
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
TRACKER_FILE = 'signal_performance_tracker.csv'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 3

# --- 1. 北京时间与行业匹配工具 ---
def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

def get_fund_tag(name):
    """基于行业关键词打标签"""
    tags = {
        "医疗/医药": ["医", "药", "生物"],
        "芯片/科技": ["芯", "半导体", "集成电路", "科技", "互联", "网"],
        "新能源/电力": ["碳", "能", "光伏", "电"],
        "大消费/酒": ["酒", "消", "食"],
        "宽基/指数": ["1000", "500", "300", "50", "创业板", "科创"]
    }
    for tag, keys in tags.items():
        if any(k in name for k in keys): return tag
    return "其他主题"

def get_name_map():
    """实时获取ETF名称"""
    try:
        import akshare as ak
        df = ak.fund_etf_category_sina("ETF基金")
        return dict(zip(df['代码'], df['名称']))
    except: return {}

# --- 2. 策略逻辑模块 ---
def analyze_signal(df):
    if len(df) < 30: return None
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    amt_ma5 = df['amount'].rolling(5).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    roc20 = (last['close'] / df['close'].shift(20).iloc[-1]) - 1

    score = 0
    if last['close'] > ma5 and dd < -0.06:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > amt_ma5: score += 1
            
    if score >= 1:
        # 建议买入股数逻辑
        risk_per_trade = TOTAL_CAPITAL * 0.02
        stop_gap = max(last['close'] - (ma10 * 0.97), 0.01)
        shares = int(risk_per_trade / stop_gap // 100 * 100)
        return {
            'roc': roc20 * 100, 'score': score, 'price': last['close'],
            'stop': ma10 * 0.97, 'shares': shares,
            'date': get_beijing_time().strftime('%Y-%m-%d')
        }
    return None

# --- 3. 执行引擎 ---
def execute():
    bj_now = get_beijing_time()
    name_map = get_name_map()
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []

    # 大盘基准分析
    mkt_df = pd.read_csv(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")) if os.path.exists(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")) else None
    mkt_bias = 0
    if mkt_df is not None:
        m_close = mkt_df.iloc[-1]['收盘']
        m_ma20 = mkt_df['收盘'].rolling(20).mean().iloc[-1]
        mkt_bias = (m_close / m_ma20 - 1)

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        try:
            df = pd.read_csv(f)
            # 兼容中文列名
            df.rename(columns={'收盘':'close','日期':'date','成交额':'amount','最高':'high','最低':'low'}, inplace=True)
            res = analyze_signal(df)
            if res:
                res['code'] = code
                res['name'] = name_map.get(code, "未知标的")
                res['tag'] = get_fund_tag(res['name'])
                all_signals.append(res)
        except: continue

    # 推送逻辑
    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]
    elite.sort(key=lambda x: x['roc'], reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板\n\n")
        f.write(f"最后同步 (北京时间): `{bj_now.strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"### 📊 市场底色\n- 大盘偏离度 (Bias20): `{mkt_bias:.2%}`\n")
        f.write(f"- 风控建议: {'🟢 积极探路' if mkt_bias > -0.01 else '🟡 严格止损'}\n\n")
        
        f.write(f"### 🎯 精英入场信号 (得分 ≥ {MIN_SCORE_SHOW})\n")
        if elite:
            f.write("| 代码 | 基金简称 | 行业/主题 | ROC20% | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in elite:
                f.write(f"| {s['code']} | {s['name']} | `{s['tag']}` | {s['roc']:.2f}% | 🔥 {s['score']} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 今日暂无 3 分信号。")
    
    # 追加到历史记录
    if all_signals:
        df_log = pd.DataFrame(all_signals)
        df_log.to_csv(TRACKER_FILE, index=False, mode='a', header=not os.path.exists(TRACKER_FILE), encoding='utf_8_sig')

if __name__ == "__main__":
    execute()
