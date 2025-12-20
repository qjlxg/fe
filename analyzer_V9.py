import pandas as pd
import glob
import os
import io
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 100000       
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
MARKET_INDEX = '510300'
MIN_SCORE_SHOW = 2  # 调低门槛，让 2 分（潜力）和 3/4 分（精英）都能看到，防止结果忽多忽少

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 1. 工业级数据清洗引擎 ---
def load_fund_db():
    fund_db = {}
    files = ['ETF列表沪.xls - 基金列表.csv', 'ETF列表深.xlsx - ETF列表.csv']
    
    for f_name in files:
        if not os.path.exists(f_name): continue
        try:
            # 彻底解决 BOM 和 特殊换行符
            with open(f_name, 'r', encoding='utf-8-sig') as f:
                content = f.read().replace('\r\n', '\n').replace('\r', '\n')
            
            df = pd.read_csv(io.StringIO(content), dtype=str)
            df.columns = [str(c).strip().replace('\ufeff', '') for c in df.columns]
            
            # 定位关键列
            c_code = next((c for c in df.columns if '代码' in c), None)
            c_name = next((c for c in df.columns if '简称' in c), None)
            c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '拟合', '标的'])), None)

            if c_code and c_name:
                for _, row in df.iterrows():
                    code = str(row[c_code]).strip().split('.')[0].zfill(6)
                    name = str(row[c_name]).strip()
                    idx = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "-"
                    if idx == "-" or idx == "nan": idx = "宽基/策略指数"
                    
                    fund_db[code] = {'name': name, 'index': idx}
        except Exception as e:
            print(f"解析 {f_name} 失败: {e}")
    return fund_db

# --- 2. 增强策略 (区分潜力与精英) ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','振幅':'vol','换手率':'turnover'}
    df.rename(columns=mapping, inplace=True)
    df.columns = [c.lower() for c in df.columns]
    
    # 转换数值并填充空值
    for col in ['close','amount','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / peak_20
    
    score = 0
    # 基础分：超跌且站上5日线 (1分)
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        # 2分：确认站上10日线 (趋势初步扭转)
        if last['close'] > ma10: score += 1
        # 3分：成交量有效放大 (主力资金入场)
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        # 4分：波动率收敛 (底部的极致信号)
        if 'vol' in df.columns and last['vol'] > 0:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    if score >= MIN_SCORE_SHOW:
        risk = TOTAL_CAPITAL * 0.02
        stop_p = last['close'] * 0.96 # 固定4%止损，实战更稳
        shares = int(risk / max(last['close'] - stop_p, 0.01) // 100 * 100)
        return {'score': score, 'price': last['close'], 'stop': stop_p, 'shares': shares, 'dd': dd * 100}
    return None

# --- 3. 执行流程 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        code = os.path.splitext(os.path.basename(f))[0].zfill(6)
        try:
            res = analyze_signal(pd.read_csv(f))
            if res:
                info = db.get(code, {'name': '未知标的', 'index': '-'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                results.append(res)
        except: continue

    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V14.0\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 适配状态: `本地沪深表全量对齐`\n\n")
        f.write("### 🎯 实时信号追踪 (2分潜力 / 3分及以上精英)\n")
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                icon = "🔥" * s['score'] if s['score'] >= 3 else "⭐"
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 市场处于横盘震荡，无符合逻辑的超跌信号。")

if __name__ == "__main__":
    execute()
