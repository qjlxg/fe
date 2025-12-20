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
REPORT_FILE = 'README.md'
MIN_SCORE_SHOW = 3  # 严格执行：只显示 3 分及以上精英信号
EXCEL_DB = 'ETF列表.xlsx' 

def get_beijing_time():
    return datetime.utcnow() + timedelta(hours=8)

# --- 1. 深度匹配引擎（适配你的Excel格式） ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB):
        print(f"❌ 找不到数据库: {EXCEL_DB}")
        return fund_db

    try:
        # 强制以字符串读取，避免代码变成浮点数
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 匹配“证券代码”和“证券简称”列（支持常见变体）
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c or '名称' in c), None)
        # 可选：追踪指数列（如果以后加了这一列会自动识别）
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '标的', '跟踪', '追踪', '行业'])), None)

        if not c_code or not c_name:
            print(f"❌ Excel 列名无法识别，当前列: {list(df.columns)}")
            return fund_db

        for _, row in df.iterrows():
            # 处理代码：提取数字，补足6位
            raw_code = str(row[c_code]).strip()
            clean_code = "".join(filter(str.isdigit, raw_code)).zfill(6)
            
            if clean_code and len(clean_code) == 6:
                fund_db[clean_code] = {
                    'name': str(row[c_name]).strip() if not pd.isna(row[c_name]) else "未知基金",
                    'index': str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "需手动补充指数"
                }

        print(f"✅ 匹配库加载完成，共 {len(fund_db)} 条记录")
    except Exception as e:
        print(f"❌ 解析 Excel 失败: {e}")
    return fund_db

# --- 2. 策略逻辑（不变） ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','振幅':'vol','换手率':'turnover'}
    df.rename(columns=mapping, inplace=True)
    
    for col in ['close','amount','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / (peak_20 if peak_20 != 0 else 1)
    
    score = 0
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        if 'vol' in df.columns and last['vol'] > 0:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    if score >= MIN_SCORE_SHOW:
        risk = TOTAL_CAPITAL * 0.02
        stop_p = last['close'] * 0.96
        shares = int(risk / max(last['close'] - stop_p, 0.01) // 100 * 100)
        return {'score': score, 'price': last['close'], 'stop': stop_p, 'shares': shares, 'dd': dd * 100}
    return None

# --- 3. 执行引擎 ---
def execute():
    bj_now = get_beijing_time()
    db = load_fund_db()
    results = []
    
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    for f in files:
        fname = os.path.splitext(os.path.basename(f))[0]
        code = "".join(filter(str.isdigit, fname)).zfill(6)
        
        try:
            df = pd.read_csv(f)
            res = analyze_signal(df)
            if res:
                info = db.get(code)
                if info:
                    name_display = info['name']
                    index_display = info['index']
                else:
                    name_display = f"未匹配({code})"
                    index_display = "需检查Excel"

                res.update({
                    'code': code,
                    'name': name_display,
                    'index': index_display
                })
                results.append(res)
        except Exception as e:
            print(f"⚠️ 处理 {code} 失败: {e}")
            continue

    # 排序：得分高 → 回撤深 优先
    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 看板 V15.4\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 过滤条件: `得分 ≥ 3`\n\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损参考 |\n")
            f.write("| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 当前市场暂无满足 3 分条件的精英标的。")
    
    print(f"✨ 执行完毕！共检测到 {len(results)} 个 3 分以上标的。")

if __name__ == "__main__":
    execute()
