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

# --- 1. 本地数据库精准匹配 (沪深两表) ---
def load_local_db():
    db = {}
    try:
        # 沪市表匹配
        sh_file = 'ETF列表沪.xls - 基金列表.csv'
        if os.path.exists(sh_file):
            df_sh = pd.read_csv(sh_file)
            for _, row in df_sh.iterrows():
                code = str(row['基金代码']).zfill(6)
                db[code] = {'name': row['基金简称'], 'index': row['标的指数'], 'size': row['最新规模(亿元)']}
        
        # 深市表匹配
        sz_file = 'ETF列表深.xlsx - ETF列表.csv'
        if os.path.exists(sz_file):
            df_sz = pd.read_csv(sz_file)
            for _, row in df_sz.iterrows():
                code = str(row['证券代码']).zfill(6)
                db[code] = {'name': row['证券简称'], 'index': row['拟合指数'], 'size': '未知'}
    except Exception as e:
        print(f"本地数据库载入提示: {e}")
    return db

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 2. 增强型策略引擎 (利用换手率与振幅) ---
def analyze_enhanced(df):
    if len(df) < 40: return None
    
    # 统一列名处理
    mapping = {'日期': 'date', '收盘': 'close', '成交额': 'amount', '换手率': 'turnover', '振幅': 'volatility'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 计算指标
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma20 = df['close'].rolling(20).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    # 换手率分析 (地量判断)
    avg_turnover_20 = df['turnover'].rolling(20).mean().iloc[-1]
    is_low_volume = last['turnover'] < (avg_turnover_20 * 0.8) # 缩量 20% 以上
    
    # 评分系统
    score = 0
    # 基础门槛：超跌 + 价格站上5日线
    if last['close'] > ma5 and dd < -0.06:
        score = 1
        # 2分：趋势确认 (站上20日线或10日线)
        if last['close'] > ma20: score += 1
        # 3分：量价确认 (缩量回踩后的放量企稳)
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        # 4分额外加分：低换手率止跌 (真正的底部特征)
        if is_low_volume: score += 1
            
    if score >= 1:
        risk_per_trade = TOTAL_CAPITAL * 0.02
        stop_price = last['close'] * 0.96 # 硬性4%止损
        shares = int(risk_per_trade / (last['close'] - stop_price) // 100 * 100)
        return {
            'score': score, 'price': last['close'], 'stop': stop_price,
            'shares': shares, 'dd': dd * 100, 'turnover': last['turnover']
        }
    return None

# --- 3. 执行主程序 ---
def execute():
    bj_now = get_beijing_time()
    db = load_local_db()
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    all_signals = []

    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        try:
            res = analyze_enhanced(pd.read_csv(f))
            if res:
                info = db.get(code, {'name': '未知标的', 'index': '-', 'size': '-'})
                res.update({'code': code, 'name': info['name'], 'index': info['index'], 'size': info['size']})
                all_signals.append(res)
        except: continue

    # 排序：得分优先，同分看回撤深度
    all_signals.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)
    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]

    # 生成 README
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V11.5\n\n")
        f.write(f"北京时间: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 数据源: `本地沪深双表匹配`\n\n")
        
        f.write("### 🎯 高胜率共振信号 (得分 ≥ 3)\n")
        f.write("> 逻辑：超跌(>-6%) + 站上均线 + 换手率/量能确认\n\n")
        if elite:
            f.write("| 代码 | 简称 | 追踪指数 | 换手% | 回撤 | 得分 | 建议买入 | 止损位 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in elite:
                score_icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['turnover']}% | {s['dd']:.1f}% | {score_icon} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("😴 暂无高分共振信号，建议继续观望。")

    if all_signals:
        pd.DataFrame(all_signals).to_csv(TRACKER_FILE, index=False, mode='a', header=not os.path.exists(TRACKER_FILE))

if __name__ == "__main__":
    execute()
