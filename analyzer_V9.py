import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 100000       # 模拟总本金
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
TRACKER_FILE = 'signal_performance_tracker.csv'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 3

# --- 工具：北京时间转换 ---
def get_beijing_time():
    # GitHub Actions 默认是 UTC，需加 8 小时
    return datetime.utcnow() + timedelta(hours=8)

# --- 工具：行业主题匹配引擎 ---
def get_fund_tag(code, name):
    """基于名称关键词自动分类"""
    tags = {
        "医疗": ["医", "药", "生物"],
        "半导体": ["芯", "半导体"],
        "互联网": ["网", "互联"],
        "新能源": ["碳", "能", "光伏", "电"],
        "消费": ["酒", "消", "食"],
        "宽基": ["1000", "500", "300", "50", "创业板"]
    }
    for tag, keys in tags.items():
        if any(k in name for k in keys):
            return tag
    return "其他主题"

def get_fund_info_map():
    """实时获取全量ETF名称映射"""
    try:
        import akshare as ak
        fund_info = ak.fund_etf_category_sina("ETF基金")
        return dict(zip(fund_info['代码'], fund_info['名称']))
    except:
        return {}

# --- 1. 数据标准化读取 ---
def load_data(file_path):
    try:
        df = pd.read_csv(file_path)
        mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low'}
        df.rename(columns=mapping, inplace=True)
        df.columns = [c.lower() for c in df.columns]
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date').reset_index(drop=True)
    except: return pd.DataFrame()

# --- 2. 评分引擎 ---
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
        # 建议买入股数（单笔风险 2%）
        risk_per_trade = TOTAL_CAPITAL * 0.02
        stop_gap = max(last['close'] - (ma10 * 0.97), 0.01)
        shares = int(risk_per_trade / stop_gap // 100 * 100)
        return {
            'roc': roc20 * 100, 'score': score, 'price': last['close'],
            'stop': ma10 * 0.97, 'shares': shares,
            'date': get_beijing_time().strftime('%Y-%m-%d')
        }
    return None

# --- 3. 执行主流程 ---
def execute():
    bj_time = get_beijing_time()
    name_map = get_fund_info_map()
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []
    
    # 大盘偏离度
    mkt_df = load_data(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv"))
    mkt_bias = (mkt_df['close'].iloc[-1] / mkt_df['close'].rolling(20).mean().iloc[-1] - 1) if not mkt_df.empty else 0

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        res = analyze_signal(load_data(f))
        if res:
            res['code'] = code
            res['name'] = name_map.get(code, "未知名称")
            res['tag'] = get_fund_tag(code, res['name'])
            all_signals.append(res)

    # 存入历史记录
    if all_signals:
        df_new = pd.DataFrame(all_signals)
        df_new.to_csv(TRACKER_FILE, index=False, mode='a', header=not os.path.exists(TRACKER_FILE))

    # 过滤 ≥3 分信号
    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]
    elite.sort(key=lambda x: x['roc'], reverse=True)

    # 处理持仓
    holdings_md = "> 🧊 空仓中。"
    if os.path.exists(PORTFOLIO_FILE):
        port = pd.read_csv(PORTFOLIO_FILE)
        if not port.empty:
            holdings_md = "| 代码 | 基金简称 | 买入价 | 现价 | 盈亏% |\n| --- | --- | --- | --- | --- |\n"
            for _, row in port.iterrows():
                f_path = os.path.join(DATA_DIR, f"{row['code']}.csv")
                if os.path.exists(f_path):
                    last_c = pd.read_csv(f_path).iloc[-1]['收盘']
                    name = name_map.get(str(row['code']), "未知")
                    profit = (last_c - row['buy_price']) / row['buy_price'] * 100
                    holdings_md += f"| {row['code']} | {name} | {row['buy_price']:.3f} | {last_c:.3f} | {profit:+.2f}% |\n"

    # 生成看板
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板\n\n")
        f.write(f"最后同步 (北京时间): `{bj_time.strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"### 📊 市场底色\n- 大盘偏离度 (Bias20): `{mkt_bias:.2%}`\n")
        f.write(f"- 风控建议: {'🟢 积极探路' if mkt_bias > -0.01 else '🟡 严格止损'}\n\n")
        f.write(f"### 💰 实时持仓监控\n{holdings_md}\n\n")
        f.write(f"### 🎯 精英入场信号 (得分 ≥ {MIN_SCORE_SHOW})\n")
        if elite:
            f.write("| 代码 | 基金简称 | 主题行业 | ROC20% | 得分 | 现价 | 建议买入 | 建议止损 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in elite:
                f.write(f"| {s['code']} | {s['name']} | `{s['tag']}` | {s['roc']:.2f}% | 🔥 {s['score']} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 今日暂无 3 分信号。")

if __name__ == "__main__":
    execute()
