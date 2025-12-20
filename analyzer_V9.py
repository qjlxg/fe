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

# --- 1. 增强版行业与名称识别引擎 ---
def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

def get_fund_info_map():
    """多级抓取：先尝试API，失败则用本地兜底"""
    name_map = {
        # 本地核心库兜底（覆盖市面 80% 成交额标的）
        "159102": "中证1000ETF", "513060": "恒生医疗ETF", "512170": "医疗ETF",
        "513050": "中概互联网ETF", "510300": "沪深300ETF", "159915": "创业板ETF",
        "513100": "纳指ETF", "510500": "中证500ETF", "588000": "科创50ETF",
        "159659": "恒生科技ETF", "513330": "恒生科技ETF", "513130": "恒生科技ETF"
    }
    try:
        import akshare as ak
        # 尝试从新浪接口获取实时列表
        df = ak.fund_etf_category_sina("ETF基金")
        api_map = dict(zip(df['代码'].str[-6:], df['名称'])) # 确保只匹配后6位数字
        name_map.update(api_map)
    except Exception as e:
        print(f"⚠️ API抓取失败，使用本地兜底库: {e}")
    return name_map

def get_fund_tag(name):
    """关键词语义打标签"""
    tags = {
        "医疗/医药": ["医", "药", "生物"],
        "互联网/科技": ["网", "互联", "科技", "芯片", "半导体"],
        "新能源/电力": ["碳", "能", "光伏", "电"],
        "宽基/指数": ["1000", "500", "300", "50", "创业板", "科创", "恒生"],
        "消费/白酒": ["酒", "消", "食"]
    }
    for tag, keys in tags.items():
        if any(k in name for k in keys): return tag
    return "行业主题"

# --- 2. 策略逻辑模块 ---
def analyze_signal(df):
    if len(df) < 30: return None
    # 自动识别列名
    mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
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
        # 风险管理：单笔亏损控制在总本金 2%
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
    name_map = get_fund_info_map()
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []

    # 大盘基准分析
    mkt_bias = 0
    mkt_path = os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")
    if os.path.exists(mkt_path):
        m_df = pd.read_csv(mkt_path)
        m_df.rename(columns={'收盘':'close'}, inplace=True)
        m_ma20 = m_df['close'].rolling(20).mean().iloc[-1]
        mkt_bias = (m_df['close'].iloc[-1] / m_ma20 - 1)

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        try:
            res = analyze_signal(pd.read_csv(f))
            if res:
                res['code'] = code
                res['name'] = name_map.get(code, "未知标的")
                # 如果依然未知，尝试处理带前缀的代码
                if res['name'] == "未知标的":
                    res['name'] = name_map.get(f"sh{code}", name_map.get(f"sz{code}", "未知标的"))
                res['tag'] = get_fund_tag(res['name'])
                all_signals.append(res)
        except: continue

    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]
    elite.sort(key=lambda x: x['roc'], reverse=True)

    # 渲染 README
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板\n\n")
        f.write(f"最后同步 (北京时间): `{bj_now.strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"### 📊 市场底色\n- 大盘偏离度 (Bias20): `{mkt_bias:.2%}`\n")
        f.write(f"- 风控建议: {'🟢 积极探路' if mkt_bias > -0.01 else '🟡 严格止损'}\n\n")
        
        f.write(f"### 🎯 精英入场信号 (得分 ≥ {MIN_SCORE_SHOW})\n")
        if elite:
            f.write("| 代码 | 基金简称 | 行业/主题 | ROC20% | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in elite:
                score_str = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['tag']}` | {s['roc']:.2f}% | {score_str} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 今日暂无精英共振信号。")

    if all_signals:
        pd.DataFrame(all_signals).to_csv(TRACKER_FILE, index=False, mode='a', header=not os.path.exists(TRACKER_FILE), encoding='utf_8_sig')

if __name__ == "__main__":
    execute()
