import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 10000        # 总可用资金
SINGLE_MAX_WEIGHT = 0.3      # 单只基金最大占用资金上限 (30%)
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
MIN_SCORE_SHOW = 3           # 只有总分 >= 3 才会被列入看板
EXCEL_DB = 'ETF列表.xlsx' 

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB):
        return fund_db
    try:
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c or '名称' in c), None)
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '标的', '追踪', '行业'])), "行业/主题")

        for _, row in df.iterrows():
            code = "".join(filter(str.isdigit, str(row[c_code]))).zfill(6)
            fund_db[code] = {
                'name': str(row[c_name]).strip(),
                'index': str(row[c_idx]).strip() if not pd.isna(row.get(c_idx)) else "行业/主题"
            }
    except: pass
    return fund_db

def calculate_indicators(df):
    """计算核心技术指标"""
    # 基础均线
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma10'] = df['close'].rolling(10).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    
    # 1. RSI (14日) - 判断超卖
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss)))
    
    # 2. MACD - 判断动能翻转
    exp1 = df['close'].ewm(span=12, adjust=False).mean()
    exp2 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = exp1 - exp2
    df['signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['hist'] = df['macd'] - df['signal']
    
    # 3. 布林带 (20, 2) - 判断支撑位
    df['std'] = df['close'].rolling(20).std()
    df['lower_band'] = df['ma20'] - (2 * df['std'])
    
    # 4. 20日最高价（算回撤）
    df['peak_20'] = df['close'].rolling(20).max()
    
    return df

def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','收盘价':'close'}
    df.rename(columns=mapping, inplace=True)
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    
    df = calculate_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 计算当前回撤
    dd = (last['close'] - last['peak_20']) / last['peak_20']
    
    score = 0
    # --- 维度 1: 基础超跌反弹 ---
    if last['close'] > last['ma5'] and dd < -0.04:
        score += 1
        
        # --- 维度 2: 动能确认 (MACD) ---
        # 逻辑：红柱增长 或 绿柱缩短
        if last['hist'] > prev['hist']:
            score += 1
            
        # --- 维度 3: 超卖保护 (RSI) ---
        # 逻辑：RSI在低位（<50）才有价值，若RSI太高说明没跌透
        if last['rsi'] < 45:
            score += 1
            
        # --- 维度 4: 支撑确认 (布林带) ---
        # 逻辑：价格在下轨附近收回
        if last['close'] < last['lower_band'] * 1.05:
            score += 1

        # --- 维度 5: 成交量确认 ---
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]:
            score += 1

    if score >= MIN_SCORE_SHOW:
        # 仓位计算：单笔亏损控制在本金 2%
        risk_per_trade = TOTAL_CAPITAL * 0.02
        stop_loss_rate = 0.05 # 设 5% 为止损宽度
        
        # 考虑资金限额 (30% 限制)
        max_invest = TOTAL_CAPITAL * SINGLE_MAX_WEIGHT
        theory_invest = risk_per_trade / stop_loss_rate
        
        actual_invest = min(theory_invest, max_invest)
        lots = int((actual_invest / last['close']) // 100)
        
        if lots < 1: return None

        return {
            'score': score,
            'price': last['close'],
            'stop': last['close'] * (1 - stop_loss_rate),
            'lots': lots,
            'dd': dd * 100,
            'rsi': last['rsi']
        }
    return None

def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        code = "".join(filter(str.isdigit, os.path.basename(f))).zfill(6)
        try:
            df = pd.read_csv(f)
            res = analyze_signal(df)
            if res:
                info = db.get(code, {'name': f'未匹配({code})', 'index': '需手动检查'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                results.append(res)
        except: continue

    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ ETF 综合策略看板 (增强版)\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 策略：**多维度指标共振 (Score >= 3)**\n\n")
        f.write("> **评分标准**：RSI低位(1) + MACD转强(1) + 布林带下轨支撑(1) + 站上5日线(1) + 放量(1)\n\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 得分 | 建议买入 | 止损位 | 现价 | RSI | 回撤 |\n")
            f.write("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |\n")
            for s in results:
                icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {icon} | **{s['lots']} 手** | {s['stop']:.3f} | {s['price']:.3f} | {s['rsi']:.1f} | {s['dd']:.1f}% |\n")
        else:
            f.write("> 😴 当前市场信号疲软，暂未发现高质量共振标的。")
    
    print(f"✨ 执行完毕，捕捉到 {len(results)} 个高质量信号。")

if __name__ == "__main__":
    execute()
