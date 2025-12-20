import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 10000        
SINGLE_MAX_WEIGHT = 0.25     
MIN_AMOUNT = 50000000        
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
HISTORY_FILE = 'signal_history.csv'
MIN_SCORE_SHOW = 4           
EXCEL_DB = 'ETF列表.xlsx' 

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

def load_fund_db():
    """修正版：直接匹配'证券代码'和'证券简称'"""
    fund_db = {}
    if not os.path.exists(EXCEL_DB): return fund_db
    try:
        # 支持 xlsx 或由 xlsx 转换的 csv
        if EXCEL_DB.endswith('.csv'):
            df = pd.read_csv(EXCEL_DB, dtype=str)
        else:
            df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        
        # 清洗列名空格
        df.columns = [str(c).strip() for c in df.columns]
        
        # 严格匹配你的列名：'证券代码', '证券简称'
        c_code = '证券代码'
        c_name = '证券简称'
        # 行业/板块逻辑：如果列里没有'行业'，则默认为'行业/主题'
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '行业', '板块'])), None)

        for _, row in df.iterrows():
            code = str(row[c_code]).strip().zfill(6)
            name = str(row[c_name]).strip()
            sector = str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "行业/主题"
            fund_db[code] = {'name': name, 'index': sector}
    except Exception as e:
        print(f"Excel读取失败: {e}")
    return fund_db

def calculate_all_metrics(df):
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    tr = pd.concat([df['high']-df['low'], (df['high']-df['close'].shift()).abs(), (df['low']-df['close'].shift()).abs()], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss)))
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_hist'] = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
    df['peak_40'] = df['close'].rolling(40).max()
    df['avg_amount'] = df['amount'].rolling(5).mean()
    df['lower_band'] = df['ma20'] - (2 * df['close'].rolling(20).std())
    return df

def analyze_signal(df):
    if len(df) < 40: return None
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','最高':'high','最低':'low','收盘价':'close'}
    df.rename(columns=mapping, inplace=True)
    for c in ['close','amount','high','low']: df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = calculate_all_metrics(df)
    last = df.iloc[-1]; prev = df.iloc[-2]
    if last['avg_amount'] < MIN_AMOUNT: return None
    dd = (last['close'] - last['peak_40']) / last['peak_40']
    
    score = 0
    if last['close'] > last['ma5'] and dd < -0.04:
        score += 1
        if last['macd_hist'] > prev['macd_hist']: score += 1
        if last['rsi'] < 40: score += 1
        if last['close'] < last['lower_band'] * 1.05: score += 1
        if last['amount'] > last['avg_amount'] * 1.1: score += 1

    if score >= MIN_SCORE_SHOW:
        atr_val = last['atr'] if not np.isnan(last['atr']) else last['close'] * 0.05
        stop_price = min(last['close'] - 3.0 * atr_val, last['close'] * 0.93)
        risk_money = TOTAL_CAPITAL * 0.02
        theory_invest = risk_money / max((last['close'] - stop_price), 0.001)
        actual_invest = min(theory_invest, TOTAL_CAPITAL * SINGLE_MAX_WEIGHT)
        return {
            'score': score, 'price': last['close'], 'stop': stop_price, 
            'theory_invest': actual_invest, 'dd': dd * 100, 
            'rsi': last['rsi'], 'avg_amount': last['avg_amount']
        }
    return None

def save_history(results):
    if not results: return
    bj_date = get_beijing_time().strftime('%Y-%m-%d')
    new_entries = []
    for s in results:
        new_entries.append({
            'date': bj_date, 'code': s['code'], 'name': s['name'], 'index': s['index'],
            'price': round(s['price'], 3), 'stop': round(s['stop'], 3), 
            'rsi': round(s['rsi'], 1), 'dd': round(s['dd'], 1), 
            'score': s['score'], 'lots': s['final_lots'], 'pos_pct': round(s['pos_percent'], 2)
        })
    df_new = pd.DataFrame(new_entries)
    if os.path.exists(HISTORY_FILE):
        try:
            df_old = pd.read_csv(HISTORY_FILE, dtype={'code': str})
            pd.concat([df_old, df_new]).drop_duplicates(subset=['date', 'code']).to_csv(HISTORY_FILE, index=False, encoding='utf_8_sig')
        except: df_new.to_csv(HISTORY_FILE, index=False, encoding='utf_8_sig')
    else:
        df_new.to_csv(HISTORY_FILE, index=False, encoding='utf_8_sig')

def check_streak(code):
    if not os.path.exists(HISTORY_FILE): return False
    try:
        df_h = pd.read_csv(HISTORY_FILE, dtype={'code': str})
        # 检查除今天外，最近3天是否有记录
        today = get_beijing_time().strftime('%Y-%m-%d')
        three_days_ago = (get_beijing_time() - timedelta(days=3)).strftime('%Y-%m-%d')
        recent = df_h[(df_h['date'] >= three_days_ago) & (df_h['date'] < today)]
        return code in recent['code'].values
    except: return False

def execute():
    db = load_fund_db()
    raw_candidates = []
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for f in files:
        code = "".join(filter(str.isdigit, os.path.basename(f))).zfill(6)
        try:
            df = pd.read_csv(f)
            res = analyze_signal(df)
            if res:
                info = db.get(code, {'name': f'未匹配({code})', 'index': '未分类'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                raw_candidates.append(res)
        except: continue

    if not raw_candidates:
        print("😴 今日无高分信号")
        return

    df_c = pd.DataFrame(raw_candidates)
    # 板块选优：得分降序，回撤深度降序（dd越小负值越大越优），成交额降序
    df_c = df_c.sort_values(by=['index', 'score', 'dd', 'avg_amount'], ascending=[True, False, False, False])
    unique_candidates = df_c.groupby('index').head(1).to_dict('records')

    # 资金分配优化
    for _ in range(2):
        total_needed = sum(item['theory_invest'] for item in unique_candidates)
        scale_factor = min(1.0, TOTAL_CAPITAL / total_needed) if total_needed > 0 else 1.0
        for item in unique_candidates:
            item['final_lots'] = int((item['theory_invest'] * scale_factor) / item['price'] // 100)
            if item['final_lots'] < 1: item['theory_invest'] = 0 

    final_show = [s for s in unique_candidates if s['final_lots'] >= 1]
    for s in final_show:
        s['pos_percent'] = (s['final_lots'] * 100 * s['price'] / TOTAL_CAPITAL) * 100
        s['is_streak'] = check_streak(s['code'])
    
    save_history(final_show)
    total_used = sum(s['final_lots'] * 100 * s['price'] for s in final_show)
    final_show.sort(key=lambda x: (x['score'], -x['dd'], x['avg_amount']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 闭环复盘看板 V21.1\n\n")
        f.write(f"最后更新: `{get_beijing_time().strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"> **当前总仓位**: `{total_used / TOTAL_CAPITAL * 100:.1f}%` | **入选标的**: `{len(final_show)} 只`\n\n")
        f.write("> **策略逻辑**: 3.0xATR止损 | 同行业优选 | 连板检测 | 证券代码/简称精准匹配\n\n")
        
        f.write("| 标签 | 代码 | 简称 | 板块 | 得分 | 建议买入 | 预计占用 | 止损位 | 现价 | RSI | 40D回撤 | 均额(万) |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for s in final_show:
            tag = "🔄" if s['is_streak'] else "⭐"
            icon = "🔥" * s['score']
            f.write(f"| {tag} | {s['code']} | **{s['name']}** | `{s['index']}` | {icon} | **{s['final_lots']} 手** | {s['pos_percent']:.1f}% | {s['stop']:.3f} | {s['price']:.3f} | {s['rsi']:.1f} | {s['dd']:.1f}% | {int(s['avg_amount']/10000)} |\n")

    print(f"✨ 修复版执行完毕。代码: {len(final_show)} 只, 匹配库: {len(db)} 条")

if __name__ == "__main__":
    execute()
