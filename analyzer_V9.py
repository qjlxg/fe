import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 100000       
DATA_DIR = 'fund_data'
PORTFOLIO_FILE = 'portfolio.csv'
TRACKER_FILE = 'signal_performance_tracker.csv'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 3

# --- 1. 本地数据库匹配引擎 ---
def load_fund_database():
    """读取本地上传的沪深ETF列表，合并为本地字典"""
    db = {}
    try:
        # 读取沪市列表 (ETF列表沪.xls - 基金列表.csv)
        df_sh = pd.read_csv('ETF列表沪.xls - 基金列表.csv')
        # 假设列名包含：基金代码, 基金简称, 追踪指数名称, 管理费率(%)
        for _, row in df_sh.iterrows():
            code = str(row.get('基金代码', '')).split('.')[0].zfill(6)
            db[code] = {
                'name': row.get('基金简称', '未知'),
                'index': row.get('追踪指数名称', '宽基指数'),
                'fee': float(row.get('管理费率(%)', 0.5)) if not pd.isna(row.get('管理费率(%)')) else 0.5
            }
        
        # 读取深市列表 (ETF列表深.xlsx - ETF列表.csv)
        df_sz = pd.read_csv('ETF列表深.xlsx - ETF列表.csv')
        for _, row in df_sz.iterrows():
            code = str(row.get('证券代码', '')).zfill(6)
            db[code] = {
                'name': row.get('证券简称', '未知'),
                'index': row.get('拟合指数简称', '宽基指数'), # 深市表常用列名
                'fee': 0.5 # 如果深市表没费率，默认为行业标准
            }
    except Exception as e:
        print(f"读取本地库警告: {e}")
    return db

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 2. 策略引擎 (加入回撤控制) ---
def analyze_signal(df):
    if len(df) < 30: return None
    mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '最高': 'high', '最低': 'low'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    amt_ma5 = df['amount'].rolling(5).mean().iloc[-1]
    
    # 动态超跌计算
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    drawdown = (last['close'] - peak_20) / peak_20
    roc20 = (last['close'] / df['close'].shift(20).iloc[-1]) - 1

    score = 0
    # 核心入场：超跌 + 站上5日线
    if last['close'] > ma5 and drawdown < -0.06:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > amt_ma5: score += 1
            
    if score >= 1:
        risk_amt = TOTAL_CAPITAL * 0.02
        stop_price = ma10 * 0.97
        shares = int(risk_amt / max(last['close'] - stop_price, 0.01) // 100 * 100)
        return {
            'roc': roc20 * 100, 'score': score, 'price': last['close'],
            'stop': stop_price, 'shares': shares, 'dd': drawdown * 100
        }
    return None

# --- 3. 执行引擎 ---
def execute():
    bj_now = get_beijing_time()
    fund_db = load_fund_database()
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []

    # 大盘基准
    mkt_bias = 0
    if os.path.exists(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv")):
        m_df = pd.read_csv(os.path.join(DATA_DIR, f"{MARKET_INDEX}.csv"))
        m_df.rename(columns={'收盘':'close'}, inplace=True)
        m_ma20 = m_df['close'].rolling(20).mean().iloc[-1]
        mkt_bias = (m_df['close'].iloc[-1] / m_ma20 - 1)

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        try:
            res = analyze_signal(pd.read_csv(f))
            if res:
                info = fund_db.get(code, {'name': '未知标的', 'index': '未知指数', 'fee': 0.5})
                res.update({'code': code, 'name': info['name'], 'index': info['index'], 'fee': info['fee']})
                all_signals.append(res)
        except: continue

    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]
    elite.sort(key=lambda x: x['roc'], reverse=True)

    # 4. 渲染看板
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 终极精英看板\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 数据库状态: `本地沪深表已挂载`\n\n")
        f.write(f"### 📊 市场底色\n- 大盘偏离度: `{mkt_bias:.2%}` | 风控建议: {'🟢 积极探路' if mkt_bias > -0.01 else '🟡 严格止损'}\n\n")
        
        f.write(f"### 🎯 顶级信号 (得分 ≥ {MIN_SCORE_SHOW})\n")
        if elite:
            f.write("| 代码 | 基金简称 | 追踪指数 | 费率 | ROC20 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in elite:
                # 费用优势标注
                fee_tag = "💎 低费" if s['fee'] <= 0.2 else f"{s['fee']}%"
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {fee_tag} | {s['roc']:.2f}% | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 今日暂无精英信号。")

    if all_signals:
        pd.DataFrame(all_signals).to_csv(TRACKER_FILE, index=False, mode='a', header=not os.path.exists(TRACKER_FILE), encoding='utf_8_sig')

if __name__ == "__main__":
    execute()
