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

def check_market_safe():
    """
    大盘刹车片逻辑：
    检测沪深300(510300)是否站上20日均线。
    这是量化中过滤系统性风险(Beta风险)最有效的方法。
    """
    path = os.path.join(DATA_DIR, "510300.csv")
    if not os.path.exists(path):
        print("⚠️ 未发现510300.csv，大盘风控跳过。")
        return True
    try:
        df_bm = pd.read_csv(path)
        df_bm.columns = [c.strip().lower() for c in df_bm.columns]
        # 兼容列名映射
        mapping = {'日期':'date','收盘':'close'}
        df_bm.rename(columns=mapping, inplace=True)
        df_bm['close'] = pd.to_numeric(df_bm['close'], errors='coerce')
        
        # 计算20日均线
        ma20 = df_bm['close'].rolling(20).mean().iloc[-1]
        last_close = df_bm['close'].iloc[-1]
        
        if last_close < ma20:
            return False # 大盘破位，不安全
        return True
    except Exception as e:
        print(f"❌ 大盘风控计算出错: {e}")
        return True

def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB): return fund_db
    try:
        # 尝试读取Excel或CSV
        if EXCEL_DB.endswith('.csv'):
            df = pd.read_csv(EXCEL_DB, dtype=str)
        else:
            df = pd.read_excel(EXCEL_DB, dtype=str)
        df.columns = [str(c).strip() for c in df.columns]
        c_code, c_name = '证券代码', '证券简称'
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '行业', '板块'])), None)
        for _, row in df.iterrows():
            code = str(row[c_code]).strip().zfill(6)
            fund_db[code] = {
                'name': str(row[c_name]).strip(),
                'index': str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "行业/主题"
            }
    except: pass
    return fund_db

def calculate_all_metrics(df):
    """全指标计算核心：MA5, MACD, RSI, Bollinger, ATR"""
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    
    # ATR 动态止损计算
    tr = pd.concat([
        df['high']-df['low'], 
        (df['high']-df['close'].shift()).abs(), 
        (df['low']-df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()
    
    # RSI 14
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / (loss + 0.00001))))
    
    # MACD 柱
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_hist'] = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
    
    # 支撑与放量指标
    df['peak_40'] = df['close'].rolling(40).max()
    df['avg_amount'] = df['amount'].rolling(5).mean()
    df['lower_band'] = df['ma20'] - (2 * df['close'].rolling(20).std())
    
    # 换手率识别
    to_col = next((c for c in df.columns if '换手率' in c), None)
    if to_col:
        df['turnover_val'] = pd.to_numeric(df[to_col], errors='coerce')
        df['avg_turnover'] = df['turnover_val'].rolling(5).mean()
    else:
        df['turnover_val'] = 0
        df['avg_turnover'] = 0
        
    return df

def analyze_signal(df):
    if len(df) < 40: return None
    # 列名清洗
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','最高':'high','最低':'low'}
    df.rename(columns=mapping, inplace=True)
    
    for c in ['close','amount','high','low']: 
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = calculate_all_metrics(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 流动性过滤
    if last['avg_amount'] < MIN_AMOUNT: return None
    
    # 40日回撤计算
    dd = (last['close'] - last['peak_40']) / (last['peak_40'] + 0.0001)
    
    score = 0
    # 基础门槛：MA5站上 + 40日回撤 > 4%
    if last['close'] > last['ma5'] and dd < -0.04:
        score += 1
        if last['macd_hist'] > prev['macd_hist']: score += 1      # 动能改善
        if last['rsi'] < 40: score += 1                          # RSI低位
        if last['close'] < last['lower_band'] * 1.05: score += 1 # 布林支撑
        # 换手率或成交额异动
        if last['amount'] > last['avg_amount'] * 1.1 or last['turnover_val'] > last['avg_turnover'] * 1.3:
            score += 1

    if score >= MIN_SCORE_SHOW:
        atr_val = last['atr'] if not np.isnan(last['atr']) else last['close'] * 0.05
        # 3.0倍ATR止损，且不超过7%硬止损
        stop_price = min(last['close'] - 3.0 * atr_val, last['close'] * 0.93)
        risk_money = TOTAL_CAPITAL * 0.02
        theory_invest = risk_money / max((last['close'] - stop_price), 0.001)
        actual_invest = min(theory_invest, TOTAL_CAPITAL * SINGLE_MAX_WEIGHT)
        
        return {
            'score': score, 'price': last['close'], 'stop': stop_price, 
            'theory_invest': actual_invest, 'dd': dd * 100, 'rsi': last['rsi'], 
            'avg_amount': last['avg_amount'], 'turnover': last['turnover_val']
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
            'rsi': round(s['rsi'], 1), 'turnover': round(s['turnover'], 2),
            'dd': round(s['dd'], 1), 'score': s['score'], 'lots': s['final_lots']
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
        today = get_beijing_time().strftime('%Y-%m-%d')
        # 近3日内是否连续上榜
        three_days_ago = (get_beijing_time() - timedelta(days=3)).strftime('%Y-%m-%d')
        recent = df_h[(df_h['date'] >= three_days_ago) & (df_h['date'] < today)]
        return code in recent['code'].values
    except: return False

def execute():
    # --- 新增：大盘刹车片检查 ---
    if not check_market_safe():
        with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
            f.write(f"# 🛰️ 全维度复盘看板 - 🛑 强行空仓模式\n\n")
            f.write(f"更新: `{get_beijing_time().strftime('%Y-%m-%d %H:%M')}`\n\n")
            f.write(f"### 🚨 系统预警：大盘风控已开启\n")
            f.write(f"目前大盘（沪深300）处于20日线下方的弱势下降通道。根据量化风控原则，此时全市场信号失效概率极大，系统已**自动拦截**所有买入建议以保护初始资金。")
        print("🚨 大盘环境不佳，已执行防御性空仓。")
        return

    # --- 正常的选股流程 ---
    db = load_fund_db(); raw_candidates = []
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    for f in files:
        # 获取6位代码
        code = "".join(filter(str.isdigit, os.path.basename(f))).zfill(6)
        if code == "510300": continue # 基准不入选
        try:
            df = pd.read_csv(f)
            res = analyze_signal(df)
            if res:
                info = db.get(code, {'name': f'未匹配({code})', 'index': '行业/主题'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                raw_candidates.append(res)
        except: continue

    if not raw_candidates:
        with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
            f.write(f"# 🛰️ 全维度复盘看板\n\n更新: `{get_beijing_time().strftime('%Y-%m-%d %H:%M')}`\n\n😴 今日无高分信号")
        print("😴 今日无符合条件的信号")
        return

    # 行业去重：同板块只选得分最高/回撤最深的那个
    df_c = pd.DataFrame(raw_candidates)
    df_c = df_c.sort_values(by=['index', 'score', 'dd', 'avg_amount'], ascending=[True, False, False, False])
    unique_candidates = df_c.groupby('index').head(1).to_dict('records')

    # 计算最终买入手数
    for _ in range(2):
        total_needed = sum(item['theory_invest'] for item in unique_candidates)
        scale_factor = min(1.0, TOTAL_CAPITAL / (total_needed + 0.001))
        for item in unique_candidates:
            item['final_lots'] = int((item['theory_invest'] * scale_factor) / item['price'] // 100)
            if item['final_lots'] < 1: item['theory_invest'] = 0 

    final_show = [s for s in unique_candidates if s['final_lots'] >= 1]
    
    for s in final_show:
        s['pos_percent'] = (s['final_lots'] * 100 * s['price'] / TOTAL_CAPITAL) * 100
        s['is_streak'] = check_streak(s['code'])
    
    # 保存历史与输出看板
    save_history(final_show)
    total_used = sum(s['final_lots'] * 100 * s['price'] for s in final_show)
    final_show.sort(key=lambda x: (x['score'], -x['dd'], x['avg_amount']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 全维度复盘看板 V22.1 (带大盘风控)\n\n")
        f.write(f"更新: `{get_beijing_time().strftime('%Y-%m-%d %H:%M')}`\n\n")
        f.write(f"> **当前总仓位**: `{total_used / TOTAL_CAPITAL * 100:.1f}%` | **入选标的**: `{len(final_show)} 只`\n\n")
        f.write("> **策略增强**: 20日线大盘风控 | 3.0xATR止损 | 行业去重 | 资金保护模式\n\n")
        f.write("| 标签 | 代码 | 简称 | 板块 | 得分 | 建议买入 | 预计占用 | 止损位 | 现价 | RSI | 换手率 | 40D回撤 | 均额(万) |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        for s in final_show:
            tag = "🔄" if s['is_streak'] else "⭐"
            icon = "🔥" * s['score']
            f.write(f"| {tag} | {s['code']} | **{s['name']}** | `{s['index']}` | {icon} | **{s['final_lots']} 手** | {s['pos_percent']:.1f}% | {s['stop']:.3f} | {s['price']:.3f} | {s['rsi']:.1f} | {s['turnover']:.2f}% | {s['dd']:.1f}% | {int(s['avg_amount']/10000)} |\n")

if __name__ == "__main__":
    execute()
