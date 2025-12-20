import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 配置 ---
TOTAL_CAPITAL = 100000       
DATA_DIR = 'fund_data'
TRACKER_FILE = 'signal_performance_tracker.csv'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 3

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 1. 强化版本地数据库匹配 ---
def load_local_db():
    db = {}
    
    def clean_code(x):
        try: return str(int(float(x))).zfill(6)
        except: return str(x).strip().zfill(6)

    # 沪市表处理
    sh_file = 'ETF列表沪.xls - 基金列表.csv'
    if os.path.exists(sh_file):
        try:
            df_sh = pd.read_csv(sh_file, encoding='utf-8-sig')
            # 自动寻找包含“代码”、“简称”、“指数”的列
            code_col = [c for c in df_sh.columns if '代码' in c][0]
            name_col = [c for c in df_sh.columns if '简称' in c][0]
            idx_col = [c for c in df_sh.columns if '指数' in c or '标的' in c][0]
            
            for _, row in df_sh.iterrows():
                c = clean_code(row[code_col])
                db[c] = {
                    'name': str(row[name_col]).strip(),
                    'index': str(row[idx_col]).strip() if not pd.isna(row[idx_col]) else "宽基/策略"
                }
        except Exception as e: print(f"沪市表读取失败: {e}")

    # 深市表处理
    sz_file = 'ETF列表深.xlsx - ETF列表.csv'
    if os.path.exists(sz_file):
        try:
            df_sz = pd.read_csv(sz_file, encoding='utf-8-sig')
            code_col = [c for c in df_sz.columns if '代码' in c][0]
            name_col = [c for c in df_sz.columns if '简称' in c][0]
            idx_col = [c for c in df_sz.columns if '指数' in c or '拟合' in c][0]
            
            for _, row in df_sz.iterrows():
                c = clean_code(row[code_col])
                db[c] = {
                    'name': str(row[name_col]).strip(),
                    'index': str(row[idx_col]).strip() if not pd.isna(row[idx_col]) else "宽基/策略"
                }
        except Exception as e: print(f"深市表读取失败: {e}")
    
    return db

# --- 2. 增强型策略引擎 ---
def analyze_logic(df):
    if len(df) < 30: return None
    # 修正列名映射
    mapping = {'日期':'date','收盘':'close','成交额':'amount','换手率':'turnover','最高':'high','最低':'low'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    amt_ma5 = df['amount'].rolling(5).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    score = 0
    if last['close'] > ma5 and dd < -0.06:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > amt_ma5: score += 1
        # 利用 CSV 里的“振幅”数据：若近期振幅收敛，代表变盘在即
        if '振幅' in df.columns:
            if df['振幅'].tail(3).mean() < df['振幅'].tail(20).mean(): score += 1
            
    if score >= 1:
        risk = TOTAL_CAPITAL * 0.02
        stop = ma10 * 0.97
        shares = int(risk / max(last['close'] - stop, 0.01) // 100 * 100)
        return {'score':score, 'price':last['close'], 'stop':stop, 'shares':shares, 'dd':dd*100, 'turnover':last.get('换手率',0)}
    return None

# --- 3. 执行 ---
def execute():
    bj_now = get_beijing_time()
    db = load_local_db()
    all_signals = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0]
        if code == MARKET_INDEX: continue
        try:
            res = analyze_logic(pd.read_csv(f))
            if res:
                info = db.get(code, {'name': '未知标的', 'index': '-'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                all_signals.append(res)
        except: continue

    all_signals.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)
    elite = [s for s in all_signals if s['score'] >= MIN_SCORE_SHOW]

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V12.0\n\n")
        f.write(f"最后同步 (北京): `{bj_now.strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write("### 🎯 高胜率共振信号 (得分 ≥ 3)\n")
        if elite:
            f.write("| 代码 | 基金简称 | 追踪指数 | ROC/回撤 | 得分 | 现价 | 建议买入 | 止损位 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in elite:
                score_icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {score_icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 今日暂无精英共振信号。")

if __name__ == "__main__":
    execute()
