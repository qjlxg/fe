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

# --- 1. 深度匹配引擎 (针对纯数字文件名优化) ---
def load_fund_db():
    fund_db = {}
    if not os.path.exists(EXCEL_DB):
        print(f"❌ 找不到数据库: {EXCEL_DB}")
        return fund_db

    try:
        # 显式使用字符串读取，防止 Excel 自动将代码转为 float
        df = pd.read_excel(EXCEL_DB, dtype=str, engine='openpyxl')
        df.columns = [str(c).strip() for c in df.columns]
        
        # 定位列名
        c_code = next((c for c in df.columns if '代码' in c), None)
        c_name = next((c for c in df.columns if '简称' in c), None)
        c_idx = next((c for c in df.columns if any(k in c for k in ['指数', '拟合', '标的'])), None)

        if c_code and c_name:
            for _, row in df.iterrows():
                # 处理 Excel 代码：先转字符串，去掉可能存在的 '.0'，再补零
                raw_code = str(row[c_code]).strip().split('.')[0]
                clean_code = "".join(filter(str.isdigit, raw_code)).zfill(6)
                
                if clean_code:
                    fund_db[clean_code] = {
                        'name': str(row[c_name]).strip(),
                        'index': str(row[c_idx]).strip() if c_idx and not pd.isna(row[c_idx]) else "行业/宽基指数"
                    }
            print(f"✅ 匹配库加载完成，共 {len(fund_db)} 条记录")
        else:
            print(f"❌ Excel 列名不匹配，当前列名: {list(df.columns)}")
    except Exception as e:
        print(f"❌ 解析 Excel 失败: {e}")
    return fund_db

# --- 2. 策略逻辑 (保持 3 分以上过滤) ---
def analyze_signal(df):
    if len(df) < 30: return None
    
    df.columns = [str(c).strip().lower() for c in df.columns]
    mapping = {'日期':'date','收盘':'close','成交额':'amount','振幅':'vol','换手率':'turnover'}
    df.rename(columns=mapping, inplace=True)
    
    # 数据转换
    for col in ['close','amount','vol']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    last = df.iloc[-1]
    ma5 = df['close'].rolling(5).mean().iloc[-1]
    ma10 = df['close'].rolling(10).mean().iloc[-1]
    peak_20 = df['close'].rolling(20).max().iloc[-1]
    dd = (last['close'] - peak_20) / (peak_20 if peak_20 != 0 else 1)
    
    score = 0
    # 评分逻辑
    if last['close'] > ma5 and dd < -0.05:
        score = 1
        if last['close'] > ma10: score += 1
        if last['amount'] > df['amount'].rolling(5).mean().iloc[-1]: score += 1
        if 'vol' in df.columns and last['vol'] > 0:
            if last['vol'] < df['vol'].rolling(10).mean().iloc[-1]: score += 1

    # 严格门槛过滤
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
    
    # 获取 DATA_DIR 下的所有 CSV 文件
    files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    
    for f in files:
        # 文件名直接取数字（如 159001.csv -> 159001）
        fname = os.path.splitext(os.path.basename(f))[0]
        code = "".join(filter(str.isdigit, fname)).zfill(6)
        
        try:
            res = analyze_signal(pd.read_csv(f))
            if res:
                # 即使没有在 Excel 匹配到，也赋予默认名称防止结果消失
                info = db.get(code)
                if info:
                    res.update({'code': code, 'name': info['name'], 'index': info['index']})
                else:
                    res.update({'code': code, 'name': f"未匹配({code})", 'index': "需检查Excel"})
                results.append(res)
        except Exception as e:
            continue

    # 排序：得分从高到低
    results.sort(key=lambda x: (x['score'], -x['dd']), reverse=True)

    with open(REPORT_FILE, "w", encoding="utf_8_sig") as f:
        f.write(f"# 🛰️ 天枢 ETF 精英看板 V15.3\n\n")
        f.write(f"最后更新: `{bj_now.strftime('%Y-%m-%d %H:%M')}` | 过滤条件: `得分 ≥ 3`\n\n")
        
        if results:
            f.write("| 代码 | 简称 | 追踪指数/行业 | 回撤 | 得分 | 现价 | 建议买入 | 止损参考 |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
            for s in results:
                icon = "🔥" * s['score']
                f.write(f"| {s['code']} | **{s['name']}** | `{s['index']}` | {s['dd']:.1f}% | {icon} | {s['price']:.3f} | {s['shares']}股 | {s['stop']:.3f} |\n")
        else:
            f.write("> 😴 当前市场暂无满足 3 分条件的精英标的。")
    
    print(f"✨ 执行完毕！共检测到 {len(results)} 个 3 分以上标的。")

if __name__ == "__main__":
    execute()
