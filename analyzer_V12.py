import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 10000        # 总可用资金
SINGLE_MAX_WEIGHT = 0.25     # 单只标的资金占用上限降至 25%
MIN_AMOUNT = 50000000        # 流动性门槛：日成交额低于 5000 万的不要
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
MIN_SCORE_SHOW = 4           # 准入门槛提高到 4 分
EXCEL_DB = 'ETF列表.xlsx' 

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB): return fund_db
    try:
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c), None)
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '行业', '板块'])), "未分类")
        for _, row in df.iterrows():
            code = "".join(filter(str.isdigit, str(row[c_code]))).zfill(6)
            fund_db[code] = {
                'name': str(row[c_name]).strip(),
                'index': str(row[c_idx]).strip() if not pd.isna(row.get(c_idx)) else "未分类"
            }
    except: pass
    return fund_db

def calculate_advanced_metrics(df):
    """计算核心过滤指标：RSI, MACD, ATR, Bollinger"""
    # 1. ATR (14日平均真实波幅) - 用于动态止损
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()

    # 2. RSI (14日) - 超卖阈值
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss)))

    # 3. MACD
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_hist'] = (exp1 - exp2) - (exp1 - exp2).ewm(span=9, adjust=False).mean()

    # 4. Bollinger & 40-day Drawdown
    df['ma20'] = df['close'].rolling(20).mean()
    df['lower_band'] = df['ma20'] - (2 * df['close'].rolling(20).std())
    df['peak_40'] = df['close'].rolling(40).max() # 窗口期延长至 40 日
    
    return df

def analyze_signal(df):
    if len(df) < 40: return None
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','最高':'high','最低':'low'}
    df.rename(columns=mapping, inplace=True)
    
    # 强制数值化
    for c in ['close','amount','high','low']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    # 流动性过滤：5日平均成交额太低的不看
    if df['amount'].tail(5).mean() < MIN_AMOUNT: return None

    df = calculate_advanced_metrics(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 基础回撤
    dd = (last['close'] - last['peak_40']) / last['peak_40']
    
    score = 0
    # 门槛：价格>5日线 且 回撤>4%
    if last['close'] > df['close'].rolling(5).mean().iloc[-1] and dd < -0.04:
        score += 1
        if last['macd_hist'] > prev['macd_hist']: score += 1      # 动能改善
        if last['rsi'] < 40: score += 1                            # 深度超卖
        if last['close'] < last['lower_band'] * 1.03: score += 1   # 布林支撑
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1] * 1.1: score += 1 # 明确放量

    if score >= MIN_SCORE_SHOW:
        # --- 动态 ATR 止损逻辑 ---
        # 止损位 = 现价 - 2倍ATR
        atr_value = last['atr'] if not np.isnan(last['atr']) else last['close'] * 0.05
        stop_price = last['close'] - (2 * atr_value)
        
        # 风险资金控制 (固定亏损总额 2%)
        risk_money = TOTAL_CAPITAL * 0.02
        theory_invest = risk_money / max((last['close'] - stop_price), 0.01)
        actual_invest = min(theory_invest, TOTAL_CAPITAL * SINGLE_MAX_WEIGHT)
        
        lots = int(actual_invest / last['close'] // 100)
        if lots < 1: return None

        return {
            'score': score, 'price': last['close'], 'stop': stop_price,
            'lots': lots, 'dd': dd * 100, 'rsi': last['rsi'], 'amount': last['amount']
        }
    return None

def execute():
    db = load_fund_db()
    raw_results = []
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    for f in files:
        code = "".join(filter(str.isdigit, os.path.basename(f))).zfill(6)
        try:
            df = pd.read_csv(f)
            res = analyze_signal(df)
            if res:
                info = db.get(code, {'name': f'未匹配({code})', 'index': '未分类'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                raw_results.append(res)
        except: continue

    # --- 板块去重逻辑 (Group By 'index' and pick best) ---
    final_results = []
    if raw_results:
        df_res = pd.DataFrame(raw_results)
        # 每个板块先按分数排，再按成交额排
        df_res = df_res.sort_values(by=['index', 'score', 'amount'], ascending=[True, False, False])
        # 每个行业只取 Top 1
        final_results = df_res.groupby('index').head(1).to_dict('records')
        # 全局再排一次序
        final_results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 精英 ETF 选基看板 V17.0\n\n")
        f.write(f"最后更新: `{get_beijing_time().strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write("> **策略升级**：① 板块自动去重 ② ATR动态止损 ③ 5000万成交门槛 ④ 40日长波回撤\n\n")
        
        if final_results:
            f.write("| 代码 | 简称 | 追踪板块 | 得分 | 建议买入 | 止损位 | 现价 | RSI | 40D回撤 |\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in final_results:
                icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {icon} | **{s['lots']} 手** | {s['stop']:.3f} | {s['price']:.3f} | {s['rsi']:.1f} | {s['dd']:.1f}% |\n")
        else:
            f.write("> 😴 当前市场暂无高质量共振标的。")
    print(f"✨ 扫描完成。原始信号 {len(raw_results)} 个，板块去重后剩余 {len(final_results)} 个。")

if __name__ == "__main__":
    execute()
