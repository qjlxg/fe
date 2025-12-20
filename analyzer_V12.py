import pandas as pd
import glob
import os
import numpy as np
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# --- 核心配置 ---
TOTAL_CAPITAL = 10000        # 总预算（实盘可用总额）
SINGLE_MAX_WEIGHT = 0.25     # 单只标的初始分配上限 (25%)
MIN_AMOUNT = 50000000        # 流动性硬门槛：5000万
DATA_DIR = 'fund_data'
REPORT_FILE = 'README.md'
MIN_SCORE_SHOW = 4           # 准入门槛：4分及以上
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
                'index': str(row[c_idx]).strip() if not pd.isna(row.get(c_idx)) else "行业/主题"
            }
    except: pass
    return fund_db

def calculate_all_metrics(df):
    """指标统一计算中心"""
    # 价格序列
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    
    # ATR (14日) - 用于动态止损
    tr = pd.concat([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()

    # RSI (14日)
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
    df['rsi'] = 100 - (100 / (1 + (gain / loss)))

    # MACD
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    df['macd_hist'] = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()

    # 布林 & 回撤
    df['lower_band'] = df['ma20'] - (2 * df['close'].rolling(20).std())
    df['peak_40'] = df['close'].rolling(40).max()
    
    # 5日均额
    df['avg_amount'] = df['amount'].rolling(5).mean()
    
    return df

def analyze_signal(df):
    if len(df) < 40: return None
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','最高':'high','最低':'low','收盘价':'close'}
    df.rename(columns=mapping, inplace=True)
    
    for c in ['close','amount','high','low']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    
    df = calculate_all_metrics(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 流动性过滤
    if last['avg_amount'] < MIN_AMOUNT: return None

    dd = (last['close'] - last['peak_40']) / last['peak_40']
    
    score = 0
    # 核心判断：站上MA5 且 深度回撤
    if last['close'] > last['ma5'] and dd < -0.04:
        score += 1
        if last['macd_hist'] > prev['macd_hist']: score += 1
        if last['rsi'] < 40: score += 1
        if last['close'] < last['lower_band'] * 1.05: score += 1
        if last['amount'] > last['avg_amount'] * 1.1: score += 1

    if score >= MIN_SCORE_SHOW:
        # --- 动态止损：3.0 * ATR，且最大不超过 10% ---
        atr_width = 3.0 * (last['atr'] if not np.isnan(last['atr']) else last['close'] * 0.03)
        stop_price = min(last['close'] - atr_width, last['close'] * 0.93)
        
        # 初始头寸计算
        risk_money = TOTAL_CAPITAL * 0.02
        theory_invest = risk_money / max((last['close'] - stop_price), 0.001)
        actual_invest = min(theory_invest, TOTAL_CAPITAL * SINGLE_MAX_WEIGHT)
        
        return {
            'score': score, 'price': last['close'], 'stop': stop_price,
            'theory_invest': actual_invest, 'dd': dd * 100, 
            'rsi': last['rsi'], 'avg_amount': last['avg_amount']
        }
    return None

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
                info = db.get(code, {'name': f'未匹配({code})', 'index': '行业/主题'})
                res.update({'code': code, 'name': info['name'], 'index': info['index']})
                raw_candidates.append(res)
        except: continue

    if not raw_candidates:
        print("😴 今日无信号"); return

    # 1. 板块去重逻辑
    df_c = pd.DataFrame(raw_candidates)
    df_c = df_c.sort_values(by=['index', 'score', 'dd', 'avg_amount'], ascending=[True, False, True, False])
    unique_candidates = df_c.groupby('index').head(1).to_dict('records')

    # 2. 全局仓位缩放逻辑
    total_needed = sum(item['theory_invest'] for item in unique_candidates)
    scale_factor = min(1.0, TOTAL_CAPITAL / total_needed) if total_needed > 0 else 1.0
    
    for item in unique_candidates:
        # 按比例缩放后的手数
        item['final_lots'] = int((item['theory_invest'] * scale_factor) / item['price'] // 100)

    # 3. 排序逻辑：分高、回撤大、额大 优先
    unique_candidates.sort(key=lambda x: (x['score'], -x['dd'], x['avg_amount']), reverse=True)

    # 4. 生成报告
    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 实盘级 ETF 选基看板 V18.0\n\n")
        f.write(f"最后更新: `{get_beijing_time().strftime('%Y-%m-%d %H:%M')}` | 资金规模: `{TOTAL_CAPITAL}`\n\n")
        f.write("> **风控提示**：全局总仓位限制 100% | 单一行业限 1 只 | 3xATR 动态止损 | 活跃度门槛 5000 万\n\n")
        
        f.write("| 代码 | 简称 | 板块 | 得分 | 建议买入 | 止损参考 | 现价 | 5日均额(万) | RSI | 回撤 |\n")
        f.write("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
        
        for s in unique_candidates:
            if s['final_lots'] < 1: continue # 过滤掉资金不足买入手的标的
            icon = "🔥" * s['score']
            f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {icon} | **{s['final_lots']} 手** | {s['stop']:.3f} | {s['price']:.3f} | {int(s['avg_amount']/10000)} | {s['rsi']:.1f} | {s['dd']:.1f}% |\n")

    print(f"✨ 扫描完毕。板块去重前 {len(df_c)} 个，去重缩放后剩余 {len(unique_candidates)} 个。")

if __name__ == "__main__":
    execute()
